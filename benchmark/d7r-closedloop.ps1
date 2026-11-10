<#
.SYNOPSIS
    Q7 遗留项 · 与 D7 等长的**闭环**复跑：9,256（B1，800 并发）对 10,650/10,766（D7）。

.DESCRIPTION
    为什么这一轮必须做，以及为什么不能只跑一臂（2026-11-10）：

    B1 的异步主链路在 800 并发下测到 9,256，而 D7 的 evalsha 臂是 400 档 10,650、
    800 档 10,766 —— 13% 的差一直没有对照。查 D7 的跑法（LOGBOOK「跑法」那段）
    发现它和 B1 之间隔着**四个**没被控制的变量，其中一个是致命的：

      | 变量      | D7（10,766）        | B1（9,256）      |
      |-----------|--------------------|-----------------|
      | consumer  | **停着**（原文：实验期间停 consumer，账本对齐、无 DB 写竞争） | 在跑 |
      | 并发      | 400 / 800          | 800             |
      | 窗口      | 3 秒               | 6 秒            |
      | 积压水位  | pending 279k~498k  | 档间 drain 清空 |

    其中「consumer 停不停」根本不在任何一张对照表里，而它同时影响两件事：
    消费端进程抢 CPU、以及 MySQL 上少了一路写入。这正好是最可能的解释，
    却从来没被单独称过。

    所以本轮只改**一个**自变量：consumer 在跑 / 停。其余全部固定 ——

        · 同一个 app JVM（全程不重启，只在两臂之间 kill/start consumer）
        · 同一端点 /api/claim/mq
        · 同 800 并发
        · 同 30 秒窗口（比 D7 的 3 秒、B1 的 6 秒都长，JIT/建连早已过去）
        · 每臂起跑前 reset 到满额 + pending=0

    判读写死在跑之前：

      | 结果                          | 结论 |
      |-------------------------------|------|
      | A(在跑) ≈ 9.3k 且 B(停) ≈ 10.7k | 13% 由 consumer 解释，归因完成 |
      | A ≈ B ≈ 9.3k                  | consumer 不是原因，差在别处（要查 broker 侧） |
      | A ≈ B ≈ 10.7k                 | B1 的 9,256 是窗口太短的产物 |
      | A ≈ 9.3k 且 B 在中间          | 两层叠加，按实测比例写 |

    两臂都跑完必须排空再收尾：B 臂（consumer 停）会积压约 30 万条，
    不排空就 reset 会把「还在队列里」读成「丢消息」，这是 D3 第一次就踩过的坑。

.PARAMETER Concurrency
    闭环并发。800 与 B1、与 D7 的 800 档对齐。

.PARAMETER Seconds
    单臂窗口。30 秒：800 并发下约 30 万样本，分位够稳，且把两臂的排空压在 45 分钟量级。

.NOTES
    必须由 experiment.ps1 调起（本机环境在每次工具调用结束时回收进程树）：
        powershell -NoProfile -ExecutionPolicy Bypass -File deploy/experiment.ps1 `
            -Work "& ./benchmark/d7r-closedloop.ps1"
#>
[CmdletBinding()]
param(
    [string]$Base = 'http://127.0.0.1:8080',
    [int]$Concurrency = 800,
    [int]$Seconds = 30,
    [int]$Quota = 1000000,
    [int]$ResourceId = 1,
    [int]$DrainTimeoutSec = 7200,
    [string]$OutDir = 'docs/acceptance/2026-11-10',

    # ---- A-B-A 的补臂支持 ----
    # 首轮跑出来 A=6,476、B=9,363，但 A 是**第一个**跑的：800 个线程同时建连 + JIT
    # 都在那 30 秒里，冷启动的份额全记在 A 头上。本项目早就栽过一次同样的坑
    # （abab.ps1 头注释：「A restart-level cold start is a ~0.62 factor,
    # the same size as the effect」），所以必须补一个 A2 把顺序效应挤出来。
    [ValidateSet('A', 'B')]
    [string[]]$Arms = @('A', 'B'),
    [string]$Tag = '',
    [switch]$Append
)

$ErrorActionPreference = 'Stop'

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
$OutDir = Join-Path $Root $OutDir
if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Path $OutDir | Out-Null }
$DevServer = Join-Path $Root 'deploy/dev-server.ps1'

function CGet([string]$Url, [int]$TimeoutSec = 30) {
    return ((& curl.exe --noproxy '*' -s --max-time $TimeoutSec $Url) -join '')
}

# reset 是 @PostMapping（ClaimDevController:122）。必须显式 POST：
# hammer 的 --reset 走 GET 会 405，而 405 与真复位都是 HTTP 200，只看状态码会把"没复位"当"复位了"。
function CPost([string]$Url, [int]$TimeoutSec = 300) {
    return ((& curl.exe --noproxy '*' -s --max-time $TimeoutSec -X POST $Url) -join '')
}

function Get-Progress {
    $raw = CGet ($Base + '/api/claim/dev/progress?resourceId=' + $ResourceId) 10
    try { return ($raw | ConvertFrom-Json) } catch { return $null }
}

function Wait-Drained([int]$TimeoutSec) {
    $sw = [Diagnostics.Stopwatch]::StartNew()
    $last = $null
    while ($sw.Elapsed.TotalSeconds -lt $TimeoutSec) {
        $p = Get-Progress
        if ($null -ne $p -and $null -ne $p.data) {
            $last = $p.data
            if ([int]$last.pending -le 0) {
                return @{ ok = $true; sec = [int]$sw.Elapsed.TotalSeconds; last = $last }
            }
        }
        Start-Sleep -Seconds 5
    }
    return @{ ok = $false; sec = [int]$sw.Elapsed.TotalSeconds; last = $last }
}

function Show-Snapshot([string]$Tag) {
    $p = Get-Progress
    if ($null -eq $p -or $null -eq $p.data) { Write-Host ('    [' + $Tag + '] (progress 不可读)'); return }
    $d = $p.data
    Write-Host ('    [{0}] records={1} dbRemain={2} redisRemain={3} pending={4}' -f `
            $Tag, $d.records, $d.dbRemain, $d.redisRemain, $d.pending)
}

function Stop-Consumer {
    $pidFile = Join-Path $Root 'logs/consumer.pid'
    if (-not (Test-Path $pidFile)) { throw 'logs/consumer.pid 不存在，无法只停 consumer' }
    $cid = (Get-Content $pidFile -Raw).Trim()
    if ([string]::IsNullOrWhiteSpace($cid)) { throw 'consumer.pid 为空' }
    $proc = Get-Process -Id ([int]$cid) -ErrorAction SilentlyContinue
    if ($null -eq $proc) { Write-Host ('    consumer pid ' + $cid + ' 已不在'); return }
    Stop-Process -Id ([int]$cid) -Force
    # 必须等到端口松开：kill 返回不等于进程已退出，紧接着起压会有一小段时间两个消费端同时在跑。
    $sw = [Diagnostics.Stopwatch]::StartNew()
    while ($sw.Elapsed.TotalSeconds -lt 60) {
        $busy = (Get-NetTCPConnection -LocalPort 8082 -ErrorAction SilentlyContinue)
        if ($null -eq $busy) { break }
        Start-Sleep -Milliseconds 500
    }
    Write-Host ('    consumer 已停（pid ' + $cid + '，等端口 8082 松开 ' + [int]$sw.Elapsed.TotalSeconds + 's）')
}

function Start-Consumer {
    # 复用 dev-server.ps1：app 的 8080 还占着，它会 SKIP app、只起 consumer。
    # 这样两臂之间 app 的 JVM 不被重启 —— 冷启动系数约 0.62，比要测的效应还大。
    & $DevServer up -Profile bench -WaitSec 120
    $sw = [Diagnostics.Stopwatch]::StartNew()
    while ($sw.Elapsed.TotalSeconds -lt 120) {
        if ($null -ne (Get-NetTCPConnection -LocalPort 8082 -ErrorAction SilentlyContinue)) {
            Write-Host ('    consumer 已起（等 ' + [int]$sw.Elapsed.TotalSeconds + 's）')
            return
        }
        Start-Sleep -Seconds 2
    }
    Write-Host '    !!! consumer 120 秒内没起来，见 logs/consumer.log'
}

$java = (Get-Command java.exe).Source
$csvAll = Join-Path $OutDir 'd7r-closedloop-arms.csv'
$sampleAll = Join-Path $OutDir 'd7r-closedloop-samples.csv'
$csvHead = 'arm,consumer,conc,thr,p50,p95,p99,avg,ok,full,other,err,limited,failPct,failAllPct,hit,inflightAvg,inflightPeak,idem,drainSec'
$sampleHead = 'arm,ts,pending,records,dbRemain,redisRemain'
if ($Append -and (Test-Path $csvAll)) {
    # 补臂不能重写表头，否则首轮两行就被冲掉了。
}
else {
    Set-Content -Path $csvAll -Value $csvHead -Encoding UTF8
    Set-Content -Path $sampleAll -Value $sampleHead -Encoding UTF8
}

Write-Host ('=== D7 闭环复跑  conc=' + $Concurrency + ' seconds=' + $Seconds + ' quota=' + $Quota + ' ===')

$allArms = @(
    @{ Name = 'A'; Consumer = 'on'; Label = 'consumer 在跑（对齐 B1）' },
    @{ Name = 'B'; Consumer = 'off'; Label = 'consumer 停（对齐 D7）' }
)
# 顺序必须按 -Arms 传入的顺序走，不能按上面的字面顺序：
# 反序跑（B 先 A 后）正是为了把"第一臂吃冷启动"这个变量翻过来看。
#
# 变量名不能叫 $arms：PowerShell 变量大小写不敏感，$arms 与参数 $Arms 是同一个变量，
# 赋值会**覆盖掉传进来的参数**，于是 foreach 拿到空数组、一臂都不跑、还不报错。
# 2026-11-10 第一次反序复跑就是这么空转 13 秒退出的（日志里 `=== 两臂对照 ===` 后面
# 直接就是表头，一行数据都没有）。
$armList = @()
foreach ($n in $Arms) {
    $hit = $allArms | Where-Object { $_.Name -eq $n }
    if ($null -ne $hit) { $armList += $hit }
}

foreach ($arm in $armList) {
    $name = $arm['Name'] + $Tag
    Write-Host ''
    Write-Host ('--- arm ' + $name + '  ' + $arm.Label + ' ---')

    if ($arm.Consumer -eq 'off') { Stop-Consumer }

    $r = CPost ($Base + '/api/claim/dev/reset?resourceId=' + $ResourceId + '&quota=' + $Quota)
    Write-Host ('    reset: ' + $r.Substring(0, [Math]::Min(200, $r.Length)))
    if ($r -notmatch '"code":0') { throw ('arm ' + $name + ' 复位失败，本轮作废') }

    $zero = Wait-Drained 300
    if (-not $zero.ok) { throw ('arm ' + $name + ' 起跑前 pending 未归零，本轮作废') }
    Write-Host ('    起跑前 pending=0（等 ' + $zero.sec + 's）')

    $armLog = Join-Path $OutDir ('d7r-closedloop-' + $name + '.log')
    $hArgs = @('benchmark/hammer/BaselineHammer.java', $Base, '0', '0', [string]$ResourceId,
        '/api/claim/mq', 'resourceId',
        ('--sweep=' + $Concurrency), ('--seconds=' + $Seconds))
    $proc = Start-Process -FilePath $java -ArgumentList $hArgs -WorkingDirectory $Root `
        -RedirectStandardOutput $armLog -RedirectStandardError ($armLog + '.err') `
        -PassThru -WindowStyle Hidden

    while (-not $proc.HasExited) {
        $p = Get-Progress
        if ($null -ne $p -and $null -ne $p.data) {
            Add-Content -Path $sampleAll -Encoding UTF8 -Value ($name + ',' + (Get-Date -Format 'HH:mm:ss') + ',' +
                $p.data.pending + ',' + $p.data.records + ',' + $p.data.dbRemain + ',' + $p.data.redisRemain)
        }
        Start-Sleep -Seconds 3
    }
    $proc.WaitForExit()

    $out = Get-Content $armLog -ErrorAction SilentlyContinue
    $row = $out | Where-Object { $_ -match ('^' + $Concurrency + ',\d+,') } | Select-Object -Last 1
    if (-not $row) {
        Write-Host ('    !!! arm ' + $name + ' 没有可解析的结果行，见 ' + $armLog)
        Get-Content ($armLog + '.err') -ErrorAction SilentlyContinue | Select-Object -Last 10
        continue
    }

    $c = $row -split ','
    Write-Host ('    conc=' + $c[0] + '  thr=' + $c[1] + '  p50=' + $c[2] + ' p95=' + $c[3] + ' p99=' + $c[4] +
        '  ok=' + $c[6] + '  failAll=' + $c[12] + '%  inflight=' + $c[14] + '/' + $c[15])
    Show-Snapshot '压测结束'

    if ($arm.Consumer -eq 'off') {
        # 不恢复消费端，B 臂的积压永远不消；恢复之后才能排空与对账。
        Start-Consumer
    }

    Write-Host '    drain 中...'
    $d = Wait-Drained $DrainTimeoutSec
    if ($d.ok) {
        Write-Host ('    drain 完成，用时 ' + $d.sec + 's，pending=0')
        Show-Snapshot 'drain 后'
    }
    else {
        Write-Host ('    !!! drain 超时 ' + $d.sec + 's，pending=' + $(if ($d.last) { $d.last.pending } else { 'n/a' }))
    }

    Add-Content -Path $csvAll -Encoding UTF8 -Value ($name + ',' + $arm.Consumer + ',' + ($c[0..16] -join ',') + ',' + $d.sec)
}

Write-Host ''
Write-Host '=== 两臂对照 ==='
Get-Content $csvAll | ForEach-Object { Write-Host $_ }
Write-Host ''
Write-Host ('采样: ' + $sampleAll)

# 判读表写死在文件头，脚本只摆数不判定 —— 判定要看 p50 与 inflight 一起读，
# 单看 thr 会把"排队消失"当成"服务变快"（Q7 那次就是这么差点写错的）。
