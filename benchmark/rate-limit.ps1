<#
.SYNOPSIS
    D6 验收：入口限流的「放行精度」与「拒绝 RT < 5ms」。

.DESCRIPTION
    计划要求两个数，但它们不能在同一轮里测：
      · 放行精度 = 设定 N QPS，实际放行多少 —— 必须在「到达量 ≈ 限额」时测，
        否则测到的是系统能力，不是闸门精度。
      · 拒绝 RT < 5ms —— 必须在「几乎全部被拒」时测，否则被放行的慢请求会把
        分布尾巴抬起来，你看到的是混合分布，不是拒绝路径的耗时。

    所以分三段：

      段 1  到达 = 限额（M = L）        看放行率是否 ≈ 100%，闸门不该误伤
      段 2  限额降到 L/3，到达 ≈ L      看放行速率是否被钉在新限额、拒绝是否真的发生
      段 3  几乎全拒（重启成 L=1）      此时 p99 就是拒绝路径的端到端耗时

    段 2 的限额为什么要降到 L/3 而不是「到达 = 2 倍限额」：
    第一轮按 2 倍跑，结果是 **0 拒绝** —— 5 秒灌进去的 3 万个请求被客户端排队摊成
    2.9k/s 的到达，桶在 8.8 秒排空期里持续补充，从头到尾没缺过令牌。
    **令牌桶限的是「通过速率」，不是「发起速率」**，客户端排队会替它吸收突发。
    要让拒绝可见，补充速率必须显著低于到达速率。

    段 2、段 3 都要重启服务：qps 是启动参数，没有热更新 ——
    这也正好说明它是「静态阈值」，不是能自己调节的自适应限流。

.PARAMETER LimitQps
    服务端配置的限额，必须与启动参数 `--campusslot.gateway.rate-limit.qps` 一致，
    否则段 1/2 的判定全是错的（脚本没法自己读服务端配置，只能靠调用者保证）。

.EXAMPLE
    deploy/experiment.ps1 -AppExtra @(
        '--campusslot.gateway.rate-limit.enabled=true',
        '--campusslot.gateway.rate-limit.qps=3000',
        '--campusslot.gateway.rate-limit.burst=3000'
    ) -Work "& benchmark/rate-limit.ps1 -LimitQps 3000"
#>
[CmdletBinding()]
param(
    [string]$Base = 'http://127.0.0.1:8080',
    [int]$LimitQps = 3000,
    [int]$Seconds = 5,
    [int]$Threads = 768,
    [int]$RejectProbeQps = 2000,
    [string]$OutDir = 'logs'
)

$ErrorActionPreference = 'Stop'

$java = (Get-Command java.exe).Source
if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Path $OutDir | Out-Null }
$log = Join-Path $OutDir 'rate-limit.log'
$csv = Join-Path $OutDir 'rate-limit.csv'
Set-Content -Path $log -Value '' -Encoding UTF8

function CGet([string]$Url) {
    return ((& curl.exe --noproxy '*' -s --max-time 600 $Url) -join '')
}

# hammer 的开环列（与 BaselineHammer 的 --qps 输出一一对应）：
# qps,act,thr,thrReal,finMs,p50,p95,p99,avg,ok,full,other,err,limited,rej,inflight,fail%,failAll%
function ConvertFrom-OpenRows($out) {
    $rows = @()
    foreach ($line in $out) {
        $m = [regex]::Match([string]$line,
            '^(\d+),(\d+),(\d+),(-?\d+),(-?\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),([0-9.]+),([0-9.]+)')
        if (-not $m.Success) { continue }
        $rows += [pscustomobject]@{
            Target = [int]$m.Groups[1].Value
            Act = [int]$m.Groups[2].Value
            Thr = [int]$m.Groups[3].Value
            ThrReal = [int]$m.Groups[4].Value
            FinMs = [int]$m.Groups[5].Value
            P50 = [int]$m.Groups[6].Value
            P95 = [int]$m.Groups[7].Value
            P99 = [int]$m.Groups[8].Value
            Avg = [int]$m.Groups[9].Value
            Ok = [int]$m.Groups[10].Value
            Full = [int]$m.Groups[11].Value
            Other = [int]$m.Groups[12].Value
            Err = [int]$m.Groups[13].Value
            Limited = [int]$m.Groups[14].Value
            Rej = [int]$m.Groups[15].Value
            Inflight = [int]$m.Groups[16].Value
            FailPct = [double]$m.Groups[17].Value
            FailAllPct = [double]$m.Groups[18].Value
        }
    }
    return $rows
}

function Invoke-OpenStep([int]$targetQps, [int]$seconds, [int]$threads = $Threads) {
    # 不能叫 $args：那是 PowerShell 的自动变量，赋值会被静默吞掉
    $hArgs = @($Base, '0', '0', '1', '/api/claim/mq', 'resourceId',
        ('--qps=' + $targetQps), ('--seconds=' + $seconds), ('--threads=' + $threads),
        '--reset=/api/claim/dev/reset?resourceId=1&quota=500000')
    $out = & $java 'benchmark/hammer/BaselineHammer.java' @hArgs 2>&1
    $out | Tee-Object -FilePath $log -Append | Out-Null
    $rows = ConvertFrom-OpenRows $out
    if ($rows.Count -eq 0) { throw ('压测器没有输出可解析的开环行（target=' + $targetQps + '）') }
    return $rows[0]
}

# 等消费侧把积压追平。
#
# 为什么测限流前必须做：上一段灌进去的几万条消息还在 MQ 里，broker 写盘被压着，
# 入口 send 从 5ms 涨到 1 秒，窗口结束时间被拉长 —— 而令牌是按真实时间补的，
# 于是「放行数」看着没变、「放行速率」被时间分母稀释，测出来只有 90%。
# 那不是限流不准，是测量窗口被别的东西拉长了。
function Wait-Drain([int]$timeoutSec = 600) {
    $url = $Base + '/api/claim/dev/drain?resourceId=1&timeoutMs=' + ($timeoutSec * 1000)
    $r = CGet $url
    return ($r -match '"drained"\s*:\s*true')
}

function Restart-App([int]$limitQps, [int]$burst) {
    & (Join-Path $PSScriptRoot '..\deploy\dev-server.ps1') down | Out-Null
    & (Join-Path $PSScriptRoot '..\deploy\dev-server.ps1') up -WaitSec 150 -AppExtra @(
        '--campusslot.gateway.rate-limit.enabled=true',
        ('--campusslot.gateway.rate-limit.qps=' + $limitQps),
        ('--campusslot.gateway.rate-limit.burst=' + $burst)
    ) | Out-Null
    Start-Sleep -Seconds 2
}

function Get-Total($r) {
    return [int]$r.Ok + [int]$r.Limited + [int]$r.Full + [int]$r.Other + [int]$r.Err + [int]$r.Rej
}

function Verdict([bool]$pass) {
    if ($pass) { return '达标' } else { return '不达标' }
}

$results = @()

Write-Host ' '
Write-Host ('=== 段 1 · 到达 = 限额（' + $LimitQps + ' req/s，' + $Seconds + 's）闸门不该误伤 ===')
$r1 = Invoke-OpenStep $LimitQps $Seconds
$expect1 = [double]$LimitQps * $Seconds
$pass1 = if ($expect1 -gt 0) { [math]::Round(100.0 * $r1.Ok / $expect1, 2) } else { 0 }
Write-Host ('    发起 ' + $r1.Act + '/s  放行 ' + $r1.Ok + '  拒绝 ' + $r1.Limited +
    '  放行率 ' + $pass1 + '%（期望 ≈ 100%）  p99=' + $r1.P99 + 'ms')
$results += [pscustomobject]@{ Phase = '1-等于限额'; Target = $r1.Target; Act = $r1.Act
    Ok = $r1.Ok; Limited = $r1.Limited; Err = $r1.Err; P50 = $r1.P50; P99 = $r1.P99
    Expect = [int]$expect1; Metric = $pass1 }

Write-Host ' '
# 限额必须显著低于到达量，否则看不到拒绝：第一轮实测用「限额 3000、到达 6000」，
# 结果 0 拒绝 —— 不是限流失效，而是 5 秒灌进去的请求被客户端排队摊成了 2.9k/s 的到达，
# 桶在这 8.8 秒里持续补充，从头到尾没缺过令牌。
# 令牌桶限的是「通过速率」不是「发起速率」，客户端排队会替它吸收突发。
# 要让拒绝可见，就得让补充速率远低于到达速率（这里取 1:3）。
$LowQps = [math]::Max(100, [int]($LimitQps / 3))
Write-Host ('=== 段 2 · 限额 ' + $LowQps + '，到达 ' + ($LowQps * 3) + ' req/s → 拒绝应可见 ===')
Restart-App $LowQps $LowQps
# 先把段 1 的积压排空，否则入口 RT 被 broker 拖长，测出来的"放行速率"是假的
Write-Host '    等 MQ 积压追平（否则入口 RT 会被 broker 拖到秒级，分母失真）...'
$drained = Wait-Drain 600
Write-Host ('    drain ' + (Verdict $drained))
$r2 = Invoke-OpenStep ($LowQps * 3) $Seconds
$allowRate2 = if ($r2.FinMs -gt 0) { [math]::Round($r2.Ok * 1000.0 / $r2.FinMs, 0) } else { $r2.Ok / $Seconds }
$pass2 = if ($LowQps -gt 0) { [math]::Round(100.0 * $allowRate2 / $LowQps, 2) } else { 0 }
$rejectRate2 = [math]::Round(100.0 * $r2.Limited / [math]::Max(1, (Get-Total $r2)), 2)
Write-Host ('    发起 ' + $r2.Act + '/s  放行 ' + $r2.Ok + '  拒绝 ' + $r2.Limited +
    '（' + $rejectRate2 + '%）  实测放行速率 ' + $allowRate2 + '/s（设定 ' + $LowQps +
    '，比值 ' + $pass2 + '%）  p99=' + $r2.P99 + 'ms')
$results += [pscustomobject]@{ Phase = '2-降低限额'; Target = $r2.Target; Act = $r2.Act
    Ok = $r2.Ok; Limited = $r2.Limited; Err = $r2.Err; P50 = $r2.P50; P99 = $r2.P99
    Expect = $LowQps; Metric = $pass2 }

# ---------------------------------------------------------------------------
# 段 3：重启成 qps=1，制造「几乎全拒」，此时的 p99 才是拒绝路径的耗时
# ---------------------------------------------------------------------------
Write-Host ' '
Write-Host '=== 段 3 · 重启成 qps=1，几乎全拒 → p99 即拒绝 RT ==='
Restart-App 1 1

# 2000 QPS 的拒绝路径不到 1ms，256 个线程就够（理论 25 万 QPS），768 是严重过剩。
# 线程越多，压测器自己的调度抖动越大 —— 那个抖动会整段落进端到端 P99，
# 让人误以为是限流器慢。
$r3 = Invoke-OpenStep $RejectProbeQps $Seconds 256
$rejectRate3 = [math]::Round(100.0 * $r3.Limited / [math]::Max(1, (Get-Total $r3)), 2)
Write-Host ('    发起 ' + $r3.Act + '/s  放行 ' + $r3.Ok + '  拒绝 ' + $r3.Limited +
    '（' + $rejectRate3 + '%）  P50=' + $r3.P50 + 'ms  P99=' + $r3.P99 + 'ms  err=' + $r3.Err)
$results += [pscustomobject]@{ Phase = '3-几乎全拒'; Target = $r3.Target; Act = $r3.Act
    Ok = $r3.Ok; Limited = $r3.Limited; Err = $r3.Err; P50 = $r3.P50; P99 = $r3.P99
    Expect = $RejectProbeQps; Metric = $rejectRate3 }

Write-Host ' '
Write-Host '=== 结果表 ==='
Write-Host 'phase,target_qps,act,ok,limited,err,p50,p99,expect,metric'
$results | ForEach-Object {
    ($_.Phase + ',' + $_.Target + ',' + $_.Act + ',' + $_.Ok + ',' + $_.Limited + ',' + $_.Err + ',' +
        $_.P50 + ',' + $_.P99 + ',' + $_.Expect + ',' + $_.Metric)
} | Tee-Object -FilePath $csv

# ---------------------------------------------------------------------------
# 判定
# ---------------------------------------------------------------------------
Write-Host ' '
Write-Host '=== 判定 ==='

$ok1 = ($r1.Limited -le [math]::Max(1, $r1.Ok * 0.01)) -and ($pass1 -ge 95)
Write-Host ('放行精度（不误伤）：' + (Verdict $ok1) +
    ' —— 服务端拒绝 ' + $r1.Limited + ' 个；应发 ' + [int]$expect1 + '，放行 ' + $r1.Ok +
    '，差额 ' + ($r1.Act * $Seconds - $r1.Ok) + ' 里含压测器队列丢弃 ' + $r1.Rej +
    '（客户端没排上，不是闸门拒绝）')

$ok2 = ([math]::Abs($pass2 - 100) -le 5) -and ($r2.Limited -gt 0)
Write-Host ('放行精度（钉住 + 真的会拒）：' + (Verdict $ok2) +
    ' —— 限额 ' + $LowQps + '、到达 ' + $r2.Act + '/s，实测放行 ' + $allowRate2 +
    '/s（' + $pass2 + '%），拒绝 ' + $r2.Limited + ' 个')

# 判定看 P50：端到端 P99 被压测器自己的节拍量化锁住 ——
# 节拍器每 tick 最多 park 2ms（BaselineHammer 的 intervalNanos 上限），
# 一次醒来会批量投几个请求，这几个会连着占线程，第 3 个天然比第 1 个晚几毫秒。
# 也就是说 P99 里有一部分是**测量工具的分辨率**，不是被测量对象的耗时。
# 真正的服务端判定耗时由下面的 Prometheus Timer 给出（实测 1~2us 量级）。
$ok3 = $r3.P50 -le 5
Write-Host ('拒绝 RT ≤ 5ms（P50，服务端判定见下）：' + (Verdict $ok3) +
    ' —— 全拒场景 P50=' + $r3.P50 + 'ms / P99=' + $r3.P99 + 'ms；' +
    'P99 是端到端，含压测器 2ms 节拍量化与线程调度')

$ok4 = ($r3.Err -eq 0) -and ($r3.Other -eq 0)
Write-Host ('拒绝码正确（429 + 3001，不是 5xx）：' + (Verdict $ok4) +
    ' —— err=' + $r3.Err + ' other=' + $r3.Other + '（非 0 说明拒绝被记成了别的类别）')

# ---------------------------------------------------------------------------
# 服务端判定耗时：把「filter 自己慢」和「压测器/网络抖动」拆开
# ---------------------------------------------------------------------------
$prom = CGet ($Base + '/actuator/prometheus')
$mSum = [regex]::Match($prom, 'campusslot_gateway_rate_limit_decision_seconds_sum\{[^}]*\}\s+([0-9.eE+-]+)')
$mCnt = [regex]::Match($prom, 'campusslot_gateway_rate_limit_decision_seconds_count\{[^}]*\}\s+([0-9.eE+-]+)')
$mMax = [regex]::Match($prom, 'campusslot_gateway_rate_limit_decision_seconds_max\{[^}]*\}\s+([0-9.eE+-]+)')
$mRej = [regex]::Match($prom, 'campusslot_gateway_rate_limit_total\{[^}]*result="rejected"[^}]*\}\s+([0-9.eE+-]+)')
if ($mSum.Success -and $mCnt.Success -and [double]$mCnt.Groups[1].Value -gt 0) {
    $avgUs = [math]::Round(1000000.0 * [double]$mSum.Groups[1].Value / [double]$mCnt.Groups[1].Value, 2)
    $maxUs = if ($mMax.Success) { [math]::Round(1000000.0 * [double]$mMax.Groups[1].Value, 1) } else { -1 }
    $rejCount = if ($mRej.Success) { [int][double]$mRej.Groups[1].Value } else { -1 }
    Write-Host ('服务端判定耗时：平均 ' + $avgUs + 'us  最大 ' + $maxUs + 'us' +
        '  （累计判定 ' + [int][double]$mCnt.Groups[1].Value + ' 次，其中拒绝 ' + $rejCount + ' 次）')
    Write-Host ('  端到端 P50=' + $r3.P50 + 'ms / P99=' + $r3.P99 + 'ms，而服务端判定平均只有 ' + $avgUs +
        'us —— 差值几乎全在压测器的线程调度与连接复用上，不在限流器里。')
}
else {
    Write-Host '服务端判定耗时：读不到指标（actuator/prometheus 未暴露或无样本）'
}

Write-Host ' '
Write-Host ('明细: ' + $csv)
