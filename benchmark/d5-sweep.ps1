<#
.SYNOPSIS
    D5 阶跃加压：三条曲线一次跑完，出「单 key 拐点」与「业务链路拐点」。

.DESCRIPTION
    要回答的问题不是"能跑多少 QPS"，而是"吞吐在哪个并发档不再上升，以及那一档是谁到顶了"。
    "吞吐不再上升"这一种形状至少有四个来源，光看一条曲线分不出来：

        /api/dev/ping        Tomcat + Spring MVC               ← 客户端与容器的地板
        /api/dev/lua/claim   上一行 + Lettuce 往返 + claim.lua  ← 单 key Redis 的真实上限
        /api/claim/mq        上一行 + 元信息缓存 + 同步 MQ 发送  ← 业务链路

    三条都在同一个 JVM、同一次调用里跑，唯一的差别是 path —— 压测器参数、线程模型、
    RT 起点全部一致。跨 JVM 比三条曲线是没有意义的（JIT 状态不同，实测能差 40%）。

    两轮：第一轮每档都吃了一遍 JIT 预热，第二轮才是稳态。同档两轮的差值本身就是
    "这台机器的不确定度"，报告里要一起给出来，不能只挑好看的那轮。

    必须由 experiment.ps1 调用（本机执行环境每次调用结束会回收整个进程树）：
        powershell -NoProfile -ExecutionPolicy Bypass -File deploy/experiment.ps1 `
            -Work "& benchmark/d5-sweep.ps1"

.PARAMETER Steps
    并发档位。默认 100,200,400,800 —— 低档（50）在这个链路上离饱和太远，
    多测一档只是多花 5 秒，对找拐点没有帮助；高档（1200+）会让 Windows 的线程调度
    自己变成瓶颈，测出来的是桌面的上限。

.PARAMETER Seconds
    每档压多久。5 秒在 500 req/s 时约 2500 个样本，p99 有 25 个样本支撑，够用。
    调到 60 秒（Gatling 的默认）在这台机器上一次扫描要 40 分钟，换不来更多信息。
#>
[CmdletBinding()]
param(
    [string]$Base = 'http://127.0.0.1:8080',
    [string]$Steps = '100,200,400,800',
    [int]$Seconds = 5,
    [int]$ClaimSeconds = 2,
    [int]$Rounds = 2,
    [int]$WarmupSeconds = 3,
    [int]$FullQuota = 500000,
    [long]$ProbeId = 9001,
    [long]$ProbeQuota = 100000000
)

$ErrorActionPreference = 'Stop'

# 消费端 poison/drift 计数的基线：每次 Reset-FullPath 成功后刷新。
# 这两个 Counter 是进程级单调的，reset 清不掉，所以 drain 判据必须算增量。
$script:BaseResidual = 0

$java = (Get-Command java.exe).Source
$logDir = Join-Path (Get-Location) 'logs'
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }
$log = Join-Path $logDir 'd5-sweep.log'
Set-Content -Path $log -Value '' -Encoding UTF8

# 统一走 curl.exe + --noproxy：PowerShell 5.1 的 Invoke-* 会读系统代理设置，
# 而本机的 HTTPS_PROXY 指向一个不存在的代理，本地回环请求会被它拦成超时。
function CGet([string]$Url) {
    return ((& curl.exe --noproxy '*' -s --max-time 600 $Url) -join '')
}

function CPost([string]$Url) {
    return ((& curl.exe --noproxy '*' -s --max-time 600 -X POST $Url) -join '')
}

function Run-Hammer([string[]]$HammerArgs) {
    & $java 'benchmark/hammer/BaselineHammer.java' @HammerArgs 2>&1 |
        Tee-Object -FilePath $log -Append
}

function Reset-Probe {
    Write-Host ('--- reset probe: resourceId=' + $ProbeId)
    CGet ($Base + '/api/dev/lua/reset?resourceId=' + $ProbeId + '&quota=' + $ProbeQuota)
    Write-Host ''
}

function Get-PersistResidual {
    <#
        自进程启动以来，被判定为「ACK 但不落库」的消息条数（poison + drift 累计）。

        为什么必须查 8082：计数器在消费者那个 JVM 里，app 侧的 /progress 拿不到。
        两个服务都带了 micrometer-registry-prometheus，直接读消费端的 /actuator/prometheus
        比让两个进程互相通信便宜得多（合并进程或 app 反查 8082 都更贵）。

        返回 $null 表示「不可达 / 没这个指标」—— 调用方必须把 $null 和 0 区分开：
        把不可达当成 0，等于在消费端已经死掉时宣布账平。
    #>
    try {
        $text = ((& curl.exe --noproxy '*' -s --max-time 3 `
                    'http://127.0.0.1:8082/actuator/prometheus') -join "`n")
    }
    catch {
        return $null
    }
    if (-not $text -or $text -notmatch 'campusslot_claim_persist_total') {
        return $null
    }
    $sum = 0.0
    # 标签集合在 micrometer 里是 (application, result)，按字母序输出，所以 result 在后面：
    #   campusslot_claim_persist_total{application="campusslot-consumer",result="poison"} 3.0
    # 老版本没有 application 标签、或尾逗号有无，都不影响这个写法 —— 但写成 {result=
    # 直接贴 { 就只能匹配到那种没有其他标签的输出，实测匹配不到。
    foreach ($m in [regex]::Matches($text,
            'campusslot_claim_persist_total\{[^}]*result="(poison|drift)"[^}]*\}\s+([0-9]+(?:\.[0-9]+)?)')) {
        $sum += [double]$m.Groups[2].Value
    }
    return [long]$sum
}

function Wait-Drain {
    <#
        等「账平」，返回 'ok' / 'residual' / 'negative' / 'timeout'。

        pending 的语义是 redisConsumed - dbConsumed（= dbRemain - redisRemain），四种结果：
          正数且残差全能被解释 → 'residual'：账已收敛，差的那几条是 poison/drift，永远落不了库
          0                     → 'ok'：真正追平
          负数                  → 'negative'：DB 比 Redis 多扣了，等下去永远不会收敛
          超时                  → 'timeout'：真有消息卡在队列里

        为什么不能只看 pending == 0：一条毒消息（字段超长、CHECK 违反）在入口已经扣了 Redis，
        消费端 ACK 但不写 quota_record，于是 pending 永久 +1。旧实现靠 -AllowQuiet
        「records 静止 3 秒就返回」来兜底，但那分不清「一条脏消息」和「消费者真的卡住了」，
        前者该继续跑、后者该立刻停，而它两种都返回 negative —— 前者白丢数据，后者被静默放过。

        现在按语义判定：pending - (poison + drift) == 0 才算账平。poison 与 drift
        （REMAIN_EXHAUSTED / COLLEGE_EXHAUSTED，D8 之后会成为常态分支）都不落库，
        所以它们解释的差额不是「还没追平」，而是「本来就不会平」。

        用 /progress 而不是 /stat：前者一次 Redis GET + 一个 COUNT，后者要 SCAN 50 万 key，
        200ms 一次的轮询用它会让 drain 的耗时里有一半是探针自己造成的。
    #>
    param([int]$TimeoutSec = 300)

    Write-Host '--- drain'
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    $pending = $null
    $unexplained = $null
    while ((Get-Date) -lt $deadline) {
        $raw = CGet ($Base + '/api/claim/dev/progress?resourceId=1')
        try {
            $obj = $raw | ConvertFrom-Json
        }
        catch {
            Start-Sleep -Milliseconds 200
            continue
        }
        $rec = [int]$obj.data.records
        $pending = $obj.data.pending
        if ($null -ne $pending) {
            if ([int]$pending -eq 0) {
                Write-Host ('    pending=0 records=' + $rec + ' 追平')
                return 'ok'
            }
            if ([int]$pending -lt 0) {
                Write-Host ('    pending=' + $pending + ' < 0 —— DB 比 Redis 多扣了，等下去不会收敛')
                return 'negative'
            }
            $residual = Get-PersistResidual
            if ($null -eq $residual) {
                # 消费端不可达：差额可能是「没消费完」，也可能是「消费端死了」。
                # 这两种都不该在这里下结论，继续等，超时交给调用方 throw。
                $unexplained = $null
            }
            else {
                $since = $residual - $script:BaseResidual
                $unexplained = [long]$pending - $since
                if ($unexplained -eq 0) {
                    Write-Host ('    pending=' + $pending + ' 全部由 poison/drift 解释（' + $since +
                        ' 条，本进程累计），账已收敛；这些消息不会落库，也不会自己消失')
                    return 'residual'
                }
            }
        }
        Start-Sleep -Milliseconds 200
    }
    $tail = if ($null -eq $unexplained) { '（残差未能核定：消费端不可达）' } else { '（其中 ' + ($pending - $unexplained) + ' 条已由 poison/drift 解释）' }
    Write-Host ('    drain 超时 pending=' + $pending + ' 未解释=' + $unexplained + $tail)
    return 'timeout'
}

function Reset-FullPath([int]$Quota) {
    # 复位前必须排空：reset 会把 Redis 抹平，但队列里的消息稍后仍会 UPDATE remain-1，
    # 于是 DB 比 Redis 多扣一大截。这一步的顺序错了，后面所有判据都从"不平"开始。
    $state = Wait-Drain
    if ($state -eq 'timeout') {
        throw 'drain 超时，队列仍未消费完，本轮数据作废（宁可停，也不要带着脏账跑 10 分钟）'
    }
    # ok 以外都要 force：residual 与 negative 的 pendingBeforeReset 都不是 0。
    # 但这两者的性质完全不同，先说清楚再复位：
    #   residual → 差额全由 poison/drift 解释，复位即重新对齐，可以继续
    #   negative → 上一轮残留的脏账，复位是在救场，本轮结论要打折扣
    $force = if ($state -eq 'ok') { '' } else { '&force=true' }
    if ($state -eq 'negative') {
        Write-Host '!!! 复位前 pending<0：上一轮的账本被污染过，本轮从干净起点重来，但历史数据仍有污点'
    }
    Write-Host ('--- reset full path: resourceId=1 quota=' + $Quota + $force)
    $resp = CPost ($Base + '/api/claim/dev/reset?resourceId=1&quota=' + $Quota + $force)
    Write-Host $resp
    if ($resp -notmatch '"code":0') {
        throw ('reset 失败：' + $resp)
    }
    # 只有 state=ok 时才要求 pendingBeforeReset 必须是 0：那是「本不该有差额」的路径。
    # residual 下它非 0 是已知且已被解释的，拿它当失败会误杀一整轮。
    if ($state -eq 'ok' -and $resp -match '"pendingBeforeReset":\s*-?[1-9]') {
        throw ('reset 前仍有未落库消息，本轮账本会脏，中止：' + $resp)
    }
    # 复位后重新取残差基线：poison/drift 是进程级单调计数器，reset 清不掉，
    # 必须记住「本轮开始时已经有多少」，否则上一轮的一条毒消息会让这一轮永远判 residual。
    $script:BaseResidual = Get-PersistResidual
    if ($null -eq $script:BaseResidual) {
        Write-Host '!!! 无法读取 8082 的 poison/drift 计数（消费端未启动或指标缺失），本轮 drain 只能按 pending==0 判定'
        $script:BaseResidual = 0
    }
    Write-Host ('    residual baseline = ' + $script:BaseResidual)
    Write-Host ''
}

$pingPath = '/api/dev/ping'
$luaPath = '/api/dev/lua/claim'
$claimPath = '/api/claim/mq'

# 每条都要单独加括号：PowerShell 里逗号优先级高于 +，
# @('--sweep=' + $Steps, '--seconds=' + $Seconds) 会被解析成
# '--sweep=' + ($Steps, '--seconds=') + $Seconds —— 一个拼坏的长字符串，
# java 收到 "--sweep=100,200 --seconds=2" 就 NumberFormatException。
$sweepArgs = @(('--sweep=' + $Steps), ('--seconds=' + $Seconds))

# 曲线 C 的窗口必须比 A/B 短，原因不是"想省时间"，而是被测系统自身的特性：
# 入口 4.4k req/s 而消费者只有 323 条/秒（见 LOGBOOK D5「消费侧天花板」），
# 5 秒 × 4 档会打出 7.7 万条消息，光追平就要 240 秒 —— 两轮就是 8 分钟，
# 实验会在 drain 里超时，而不是在测量里超时。
# 2 秒窗口在 800 并发下仍有 9k 个样本（p99 = 90 个），判饱和足够。
$claimSweepArgs = @(('--sweep=' + $Steps), ('--seconds=' + $ClaimSeconds))

# ---- 预清理 ----
# 顺序必须是「先 drain、后 reset」，反过来的话上一轮崩溃遗留在队列里的几万条消息
# 会在 reset 之后才被消费，把刚复位好的与 DB 又拉开一个负的 pending
# （实测 -27605），后面每一个判据都从"不平"开始。
Write-Host ''
Write-Host '=== 预清理：先消费干净上一轮遗留的消息，再复位 ==='
# 不在这里单独 drain：Reset-FullPath 内部就是「先 drain 后 reset」。
# 上一版在这里多写了一次，返回值还被丢掉，紧接着 Reset-FullPath 又用不带逃生开关的
# 参数再排一遍 —— 那次调用纯粹白等，而且正是它把「一条毒消息」放大成 300 秒超时。
Reset-FullPath $FullQuota

# 预热：三条链路是三个不同的代码路径，各自都要被 JIT 编一遍。
# 不预热的话每次曲线的第一档都是"冷启动"，实测同一个 jar 同一档能差 4 倍
# （/api/claim/mq 冷启动 491 req/s，第二轮同档 2132 req/s）。
# 这段数据全部丢弃，只为了让正式两轮的每一档都落在稳态上。
if ($WarmupSeconds -gt 0) {
    Write-Host ''
    Write-Host ('=== warmup ' + $WarmupSeconds + 's per path (discarded) ===')
    $warmArgs = @('--sweep=100', ('--seconds=' + $WarmupSeconds))
    Reset-Probe | Out-Null
    Run-Hammer (@($Base, '0', '0', '9001', $pingPath, 'none') + $warmArgs +
                @('--count=/api/dev/ping/count')) | Out-Null
    Reset-Probe | Out-Null
    Run-Hammer (@($Base, '0', '0', [string]$ProbeId, $luaPath, 'resourceId') + $warmArgs +
                @('--count=/api/dev/lua/count')) | Out-Null
    Reset-FullPath $FullQuota | Out-Null
    Run-Hammer (@($Base, '0', '0', '1', $claimPath, 'resourceId') +
                @('--sweep=100', ('--seconds=' + $ClaimSeconds))) | Out-Null
    Wait-Drain | Out-Null
}

Write-Host ('=== D5 sweep  steps=' + $Steps + ' seconds=' + $Seconds + ' rounds=' + $Rounds + ' ===')

for ($round = 1; $round -le $Rounds; $round++) {
    Write-Host ''
    Write-Host ('##### round ' + $round + ' / ' + $Rounds + ' #####')

    # ---- 曲线 A：客户端与容器的地板 ----
    Reset-Probe | Out-Null
    Run-Hammer (@($Base, '0', '0', '9001', $pingPath, 'none') + $sweepArgs +
                @('--count=/api/dev/ping/count'))

    # ---- 曲线 B：单 key Redis ----
    Reset-Probe | Out-Null
    Run-Hammer (@($Base, '0', '0', [string]$ProbeId, $luaPath, 'resourceId') + $sweepArgs +
                @('--count=/api/dev/lua/count'))

    # ---- 曲线 C：业务全链路 ----
    # 曲线 C 的吞吐看 hammer 的输出，账目看 drain 之后的 stat：
    # 曲线本身不写库，写库的是消费者，所以「压完立刻读 stat」必然 pending > 0，
    # 那是正常的"还没消费完"，不是丢消息。必须等 drain 完再读。
    # Reset-FullPath 内部自带「先排空再复位」，这里不用再 drain 一次。
    Reset-FullPath $FullQuota | Out-Null
    Run-Hammer (@($Base, '0', '0', '1', $claimPath, 'resourceId') + $claimSweepArgs)
    $state = Wait-Drain
    Write-Host ('--- round ' + $round + ' stat')
    CGet ($Base + '/api/claim/dev/stat?resourceId=1')
    Write-Host ''
    if ($state -eq 'residual') {
        # 账收敛了，但有消息被判为不可落库。这不影响吞吐/RT 曲线的读法，
        # 只影响「客户端成功数 == records == idemKeys」这条三方对齐 —— 它必然差这么多。
        Write-Host ('!!! round ' + $round + ' 账已收敛，但差额来自 poison/drift（见上一条日志），' +
            '本轮 records 必然少于 idemKeys，三方对齐判据不适用')
    }
    elseif ($state -ne 'ok') {
        Write-Host ('!!! round ' + $round + ' 未追平（' + $state + '），账目判据不可用；吞吐与 RT 仍然有效')
    }
}

# 收尾判据：一定要用 stat 验证，不能只看 hammer 报的成功数
Write-Host ''
Write-Host '=== final stat ==='
CGet ($Base + '/api/claim/dev/stat?resourceId=1')
Write-Host ''
Write-Host '=== probe stat ==='
CGet ($Base + '/api/dev/lua/count')
Write-Host ''
Write-Host ('log: ' + $log)
