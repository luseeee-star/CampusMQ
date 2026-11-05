# Pool A/B, re-measured at the UNLOCKED path (~27k). 0a was taken at 5.3k while the
# Spring Data Redis lock was the bottleneck, so its "pooling gives 0.1%" is not reusable here.
#
# Two things are decided at once:
#   1. does pooling move throughput at 27k?
#   2. how many lettuce-nioEventLoop threads exist in each phase?
#      Lettuce (Netty) starts one event loop per *registered channel*. A single shared
#      connection can only ever occupy one loop, which is what the ceiling run saw
#      (one loop thread at 89% of a core). If pooling still shows one loop thread, then
#      pooling cannot add event-loop parallelism and the wall is ioThreads, not the pool.
#
# Guards added after the 2026-11-03 run where Docker was down: the app failed to start,
# the script kept going anyway, and every request came back as an error -- the CSV row then
# looked like a plausible "5.8k throughput" when it was really 58,171 errors.
# Any phase with err>0 or a dead app now aborts loudly instead of producing a table.
$ErrorActionPreference = 'Continue'
$cores = (Get-CimInstance Win32_ComputerSystem).NumberOfLogicalProcessors
"logical_cores = $cores"
$H = 'benchmark/hammer/BaselineHammer.java'
$B = 'http://127.0.0.1:8080'

function Wait-AppPort([int]$timeoutSec) {
    $sw = [Diagnostics.Stopwatch]::StartNew()
    while ($sw.Elapsed.TotalSeconds -lt $timeoutSec) {
        $hit = (netstat -ano | Select-String ":8080\s" | Select-String "LISTENING")
        if ($hit) { return $true }
        Start-Sleep -Milliseconds 500
    }
    return $false
}

function WarmUp {
    java $H $B 0 0 9001 /api/dev/ping none --sweep=100 --seconds=3 --count=/api/dev/ping/count > logs\ab-w1.log 2>&1
    java $H $B 0 0 9001 /api/dev/lua/claim-sha resourceId --sweep=100 --seconds=3 --count=/api/dev/lua/count '--reset=/api/dev/lua/reset?resourceId=9001' > logs\ab-w2.log 2>&1
    java $H $B 0 0 9001 /api/dev/ping none --sweep=100,200,400,800 --seconds=5 --count=/api/dev/ping/count > logs\ab-w3.log 2>&1
    java $H $B 0 0 9001 /api/dev/lua/claim-sha resourceId --sweep=100,200 --seconds=5 --count=/api/dev/lua/count '--reset=/api/dev/lua/reset?resourceId=9001' > logs\ab-w4.log 2>&1
}

$results = New-Object System.Collections.ArrayList

function RunPhase([string]$tag, [string[]]$extra) {
    "=== phase $tag  extra=[$($extra -join ' ')] ==="
    & ./deploy/dev-server.ps1 down | Out-Null
    if ($extra) { & ./deploy/dev-server.ps1 up -WaitSec 120 -AppExtra $extra | Out-Null }
    else { & ./deploy/dev-server.ps1 up -WaitSec 120 | Out-Null }

    if (-not (Wait-AppPort 30)) {
        "  ABORT: app never listened on 8080 -- check logs/app.log (docker down? MQ on 8081?)"
        exit 1
    }
    $probe = & curl.exe -s --noproxy '*' "$B/api/dev/ping" 2>&1
    if ("$probe" -notmatch 'pong|ok') { "  ABORT: ping returned [$probe]"; exit 1 }

    WarmUp
    java $H $B 0 0 9001 /api/dev/lua/claim-sha resourceId --sweep=400,400,400 --seconds=10 --count=/api/dev/lua/count '--reset=/api/dev/lua/reset?resourceId=9001' > "logs\ab-$tag.log" 2>&1

    $rows = @(Get-Content "logs\ab-$tag.log" | Select-String -Pattern '^400,')
    if ($rows.Count -eq 0) { "  ABORT: no CSV row in logs\ab-$tag.log"; Get-Content "logs\ab-$tag.log" -Tail 8; exit 1 }

    $thrs = @()
    foreach ($r in $rows) {
        $f = $r.Line -split ','
        $thr = [int]$f[1]; $ok = [int]$f[6]; $err = [int]$f[9]
        "  400,$thr  ok=$ok err=$err p50=$($f[2]) p99=$($f[4])"
        if ($err -gt 0 -or $ok -eq 0) {
            "  ABORT: errors in this phase ($err err / $ok ok) -- the row is not a throughput number"
            exit 1
        }
        $thrs += $thr
    }
    $sorted = @($thrs | Sort-Object)
    $med = $sorted[[int][math]::Floor($sorted.Count / 2)]
    "  thr median = $med  min = $($sorted[0])  max = $($sorted[-1])"

    $appPid = [int](Get-Content logs\app.pid)
    $dump = @(& jstack $appPid 2>&1)
    $loop = @($dump | Select-String -Pattern 'lettuce-nioEventLoop')
    "  app_pid = $appPid   lettuce event loop threads = $($loop.Count)"
    foreach ($l in $loop) {
        $t = ($l.Line -replace '\s+', ' ')
        "    " + $t.Substring(0, [Math]::Min(80, $t.Length))
    }
    [void]$results.Add([pscustomobject]@{ phase = $tag; median = $med; loops = $loop.Count })
}

RunPhase 'base' @()
RunPhase 'pool' @('--campusslot.redis.dedicated-connections=true')

& ./deploy/dev-server.ps1 down | Out-Null

"=== summary ==="
$results | ForEach-Object { "  $($_.phase): thr_median=$($_.median)  event_loops=$($_.loops)" }
if ($results.Count -eq 2) {
    $base = $results[0].median; $pool = $results[1].median
    "  pool / base = $([math]::Round($pool / $base, 3))"
    "  linear bound if Redis is the next wall:       26995 / 0.827 = ~32.6k  (+21%)"
    "  linear bound if the single event loop is:     26995 / 0.89  = ~30.3k  (+12%)"
}
"done"
