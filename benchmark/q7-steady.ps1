<#
.SYNOPSIS
    Q7 · 开环 8000 QPS 的裁决版：**两档线程数**，看 thrReal 与 p99 的联动方向。

.DESCRIPTION
    为什么不是 D5 原样重跑（2026-11-10 的判断）：

    D5 的开环器写死 `ClaimThreads = 768`，而 768 × (1 ÷ 200ms) ≈ 3,840/s 恰好就是
    实测 thrReal 的 3,845。也就是说「3,845 是系统的墙」与「3,845 是压测器的墙」
    这两个假设，在 768 这一档上给出**完全相同**的预测值 —— 复现它不构成任何证据。

    唯一能把它们分开的自变量是线程数。Little 定律给两档各自的预测：

        若墙在系统：thrReal 不动（≈3.85k），p99 被拉开到 2048 ÷ 3845 ≈ 533ms
        若墙在压测器：thrReal 跟到 2048 ÷ 200ms ≈ 10.2k（被 8000 的目标速率截断），
                      p99 仍在 200ms 量级

    判据用**联动方向**而不是"thrReal 涨没涨"：光看吞吐，争用的来源分不清。

    ---- 这一步跑完，Q7 与 Q19 一起收口 ----

    每臂的流程（严格同序，臂间必须清账，否则第二臂会在积压状态下开跑，
    那正好是 Q19 候选 C，会把两件事混成一件）：

        reset(quota) → 起 5 秒采样 → 压 60 秒 → 停下采样 → drain 到 pending=0

.PARAMETER Threads
    两档线程数。默认 768 / 2048：768 是复现档（与 D5 同参），2048 是裁决档。

.PARAMETER Seconds
    单臂窗口。60 秒：8000 QPS 下 48 万样本，分位够稳；再长只会让 drain 更久。

.NOTES
    必须由 experiment.ps1 调起（本机环境在每次工具调用结束时回收进程树）：
        powershell -NoProfile -ExecutionPolicy Bypass -File deploy/experiment.ps1 `
            -Work "& ./benchmark/q7-steady.ps1"
#>
[CmdletBinding()]
param(
    [string]$Base = 'http://127.0.0.1:8080',
    [int]$Qps = 8000,
    [int]$Seconds = 60,
    [int[]]$Threads = @(768, 2048),
    [int]$Quota = 2000000,
    [int]$ResetId = 1,
    [int]$DrainTimeoutSec = 5400,
    [string]$OutDir = 'docs/acceptance/2026-11-10'
)

$ErrorActionPreference = 'Stop'

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
$OutDir = Join-Path $Root $OutDir
if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Path $OutDir | Out-Null }

function CGet([string]$Url, [int]$TimeoutSec = 30) {
    return ((& curl.exe --noproxy '*' -s --max-time $TimeoutSec $Url) -join '')
}

# reset 是 @PostMapping（ClaimDevController:122）。这里必须显式 POST ——
# 口径 2 记的"B1 的 reset 从未生效"就是 hammer 的 --reset 走 GET、405 被兜底成 9001，
# 而兜底码是 9001 不是 0，脚本当时没校验返回体，于是整轮都在打同一份旧账。
function CPost([string]$Url, [int]$TimeoutSec = 300) {
    return ((& curl.exe --noproxy '*' -s --max-time $TimeoutSec -X POST $Url) -join '')
}

function Get-Progress {
    $raw = CGet ($Base + '/api/claim/dev/progress?resourceId=' + $ResetId) 10
    try { return ($raw | ConvertFrom-Json) } catch { return $null }
}

# pending 归零才算账平。判据与 open-loop.ps1 的 Wait-Drain 同源，
# 但这里不用 /drain 长轮询：长轮询超时会静默返回空串，而本脚本要的是
# 「每 5 秒看一眼」的采样节奏，两者不能混用。
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

$java = (Get-Command java.exe).Source
$csvAll = Join-Path $OutDir 'q7-steady-arms.csv'
$sampleAll = Join-Path $OutDir 'q7-steady-samples.csv'
Set-Content -Path $csvAll -Value ('arm,threads,qps_target,qps_actual,thr,thrReal,finMs,p50,p95,p99,avg,ok,full,other,err,limited,rej,inflight,failPct,failAllPct,drainSec') -Encoding UTF8
Set-Content -Path $sampleAll -Value ('arm,ts,pending,records,dbRemain,redisRemain') -Encoding UTF8

Write-Host ('=== Q7 裁决版  qps=' + $Qps + ' seconds=' + $Seconds + ' threads=' + ($Threads -join '/') + ' quota=' + $Quota + ' ===')

$armNo = 0
foreach ($t in $Threads) {
    $armNo++
    $arm = 'T' + $armNo

    Write-Host ''
    Write-Host ('--- arm ' + $arm + '  threads=' + $t + ' ---')

    # 臂前必须清账：quota 复位会把 remain / Redis stock / 记录一起归零（顺序由 reset 保证）。
    # 返回值必须校验：405/9001 与真正的复位都是 HTTP 200，只看状态码会把"没复位"当成"复位了"。
    $r = CPost ($Base + '/api/claim/dev/reset?resourceId=' + $ResetId + '&quota=' + $Quota)
    Write-Host ('    reset: ' + $r.Substring(0, [Math]::Min(160, $r.Length)))
    if ($r -notmatch '"code":0') {
        throw ('arm ' + $arm + ' 复位失败，本轮作废（返回体见上）——绝不在脏账上开跑')
    }

    $zero = Wait-Drained 300
    if (-not $zero.ok) { throw ('arm ' + $arm + ' 起跑前 pending 未归零，本轮作废') }
    Write-Host ('    起跑前 pending=0（等 ' + $zero.sec + 's）')

    $armLog = Join-Path $OutDir ('q7-steady-' + $arm + '.log')
    $hArgs = @('benchmark/hammer/BaselineHammer.java', $Base, '0', '0', [string]$ResetId,
        '/api/claim/mq', 'resourceId',
        ('--qps=' + $Qps), ('--seconds=' + $Seconds), ('--threads=' + $t))
    $proc = Start-Process -FilePath $java -ArgumentList $hArgs -WorkingDirectory $Root `
        -RedirectStandardOutput $armLog -RedirectStandardError ($armLog + '.err') `
        -PassThru -WindowStyle Hidden

    # 压测期间每 5 秒采一次，采的是"过程中"而不是"结束后"——
    # 结束后再问 pending 只能知道积压有多少，看不出它是怎么涨上去的。
    while (-not $proc.HasExited) {
        $p = Get-Progress
        if ($null -ne $p -and $null -ne $p.data) {
            $line = ($arm + ',' + (Get-Date -Format 'HH:mm:ss') + ',' + $p.data.pending + ',' +
                $p.data.records + ',' + $p.data.dbRemain + ',' + $p.data.redisRemain)
            Add-Content -Path $sampleAll -Value $line -Encoding UTF8
        }
        Start-Sleep -Seconds 5
    }
    $proc.WaitForExit()

    $out = Get-Content $armLog -ErrorAction SilentlyContinue
    $row = $out | Where-Object { $_ -match ('^' + $Qps + ',\d+,') } | Select-Object -Last 1
    if (-not $row) {
        Write-Host ('    !!! arm ' + $arm + ' 没有可解析的结果行，见 ' + $armLog)
        Get-Content ($armLog + '.err') -ErrorAction SilentlyContinue | Select-Object -Last 10
        continue
    }

    $c = $row -split ','
    Write-Host ('    目标 ' + $c[0] + ' → 发起 ' + $c[1] + '  窗口内完成 ' + $c[2] +
        '  真实完成 ' + $c[3] + ' (finMs=' + $c[4] + ')  p50=' + $c[5] + ' p95=' + $c[6] + ' p99=' + $c[7] +
        '  rej=' + $c[14] + '  failAll=' + $c[17] + '%')

    Write-Host '    drain 中...'
    $d = Wait-Drained $DrainTimeoutSec
    if ($d.ok) {
        Write-Host ('    drain 完成，用时 ' + $d.sec + 's，pending=0')
    }
    else {
        Write-Host ('    !!! drain 超时 ' + $d.sec + 's，pending=' + $(if ($d.last) { $d.last.pending } else { 'n/a' }))
    }

    Add-Content -Path $csvAll -Encoding UTF8 -Value ($arm + ',' + $t + ',' +
        ($c[0..17] -join ',') + ',' + $d.sec)
}

Write-Host ''
Write-Host '=== 两臂对照 ==='
Get-Content $csvAll | ForEach-Object { Write-Host $_ }
Write-Host ''
Write-Host ('采样: ' + $sampleAll)

# 判读三分支在跑之前就写死在报告里，这里只回显最后一行，不做判定 ——
# 判定要人对着 p99 与 thrReal 的联动方向写，脚本只负责把两个数摆在一起。
