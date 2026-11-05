# Who limits the unlocked path (27.8k)?  Three things in one run:
#   1. app process CPU per request      (was ~490us/req while the lock existed)
#   2. per-thread CPU of the app JVM    (is the single Lettuce event loop pegged?)
#   3. Redis INFO cpu delta + WINDOW commandstats (CONFIG RESETSTAT first, so
#      usec_per_call is a window value, not a since-startup average)
# Everything must run inside ONE tool call: the dev-server process is reaped when the call ends.
$ErrorActionPreference = 'Continue'
$cores = (Get-CimInstance Win32_ComputerSystem).NumberOfLogicalProcessors
"logical_cores = $cores"

powershell -NoProfile -ExecutionPolicy Bypass -File deploy/dev-server.ps1 up -WaitSec 120

$H = 'benchmark/hammer/BaselineHammer.java'
$B = 'http://127.0.0.1:8080'

# D5 warm-up path, but on the unlocked endpoint
java $H $B 0 0 9001 /api/dev/ping none --sweep=100 --seconds=3 --count=/api/dev/ping/count > logs\c0.log 2>&1
java $H $B 0 0 9001 /api/dev/lua/claim-sha resourceId --sweep=100 --seconds=3 --count=/api/dev/lua/count '--reset=/api/dev/lua/reset?resourceId=9001' > logs\c1.log 2>&1
java $H $B 0 0 9001 /api/dev/ping none --sweep=100,200,400,800 --seconds=5 --count=/api/dev/ping/count > logs\c2.log 2>&1
java $H $B 0 0 9001 /api/dev/lua/claim-sha resourceId --sweep=100,200 --seconds=5 --count=/api/dev/lua/count '--reset=/api/dev/lua/reset?resourceId=9001' > logs\c3.log 2>&1
"warmup done"

$appPid = (Get-NetTCPConnection -LocalPort 8080 -State Listen | Select-Object -First 1).OwningProcess
"app_pid = $appPid"
Invoke-WebRequest -Uri 'http://127.0.0.1:8080/api/dev/lua/reset?resourceId=9001' -Method POST -UseBasicParsing -TimeoutSec 180 | Out-Null

# Window counters start here: everything below is measured inside one 20s window.
docker exec cs-redis redis-cli config resetstat | Out-Null
$raw0 = docker exec cs-redis redis-cli info cpu
$u0 = [double](($raw0 | Where-Object { $_ -match '^used_cpu_user:' }) -replace 'used_cpu_user:', '')
$s0 = [double](($raw0 | Where-Object { $_ -match '^used_cpu_sys:' }) -replace 'used_cpu_sys:', '')
"redis baseline: user=$u0 sys=$s0"

function Get-ProcCpu([int]$procId) {
    try { return (Get-Process -Id $procId -ErrorAction Stop).CPU } catch { return $null }
}
function Get-ThreadSnapshot([int]$procId) {
    $res = @{}
    try {
        $p = Get-Process -Id $procId -ErrorAction Stop
        foreach ($t in $p.Threads) {
            try { $res[[int]$t.Id] = $t.TotalProcessorTime.TotalSeconds } catch { }
        }
    } catch { }
    return $res
}

$psi = [System.Diagnostics.ProcessStartInfo]::new()
$psi.FileName = 'java'
$psi.Arguments = "$H $B 0 0 9001 /api/dev/lua/claim-sha resourceId --sweep=400 --seconds=20 --count=/api/dev/lua/count"
$psi.UseShellExecute = $false
$psi.RedirectStandardOutput = $true
$psi.RedirectStandardError = $true
$psi.WorkingDirectory = (Get-Location).Path
$hp = [System.Diagnostics.Process]::Start($psi)
$outTask = $hp.StandardOutput.ReadToEndAsync()
$errTask = $hp.StandardError.ReadToEndAsync()

$sw = [Diagnostics.Stopwatch]::StartNew()
$frames = New-Object System.Collections.ArrayList
$stackDone = $false
$loadStart = 0.0
while (-not $hp.HasExited) {
    $el = $sw.Elapsed.TotalSeconds
    # the first ~2s is JVM start + client connect, keep it out of the steady window
    if ($loadStart -eq 0.0 -and $el -ge 2.0) { $loadStart = $el }
    [void]$frames.Add([pscustomobject]@{
        sec  = $el
        app  = (Get-ProcCpu $appPid)
        ham  = (Get-ProcCpu $hp.Id)
        thrd = (Get-ThreadSnapshot $appPid)
    })
    if (-not $stackDone -and $el -ge 12) {
        & jstack $appPid 2>&1 | Out-File -FilePath logs\jstack-ceiling.txt -Encoding utf8
        $stackDone = $true
    }
    Start-Sleep -Milliseconds 800
}
$sw.Stop()
$totalSec = $sw.Elapsed.TotalSeconds
$hp.WaitForExit()
Set-Content -Path logs\ceiling-load.log -Value $outTask.Result -Encoding UTF8

$raw1 = docker exec cs-redis redis-cli info cpu
$u1 = [double](($raw1 | Where-Object { $_ -match '^used_cpu_user:' }) -replace 'used_cpu_user:', '')
$s1 = [double](($raw1 | Where-Object { $_ -match '^used_cpu_sys:' }) -replace 'used_cpu_sys:', '')
$cs = docker exec cs-redis redis-cli info commandstats

$row = (Get-Content logs\ceiling-load.log | Select-String -Pattern '^400,').Line
if (-not $row) { "NO CSV ROW - load failed"; Get-Content logs\ceiling-load.log -Tail 10; exit 1 }
$f = $row -split ','
$conc = [int]$f[0]; $thr = [int]$f[1]; $p50 = $f[2]; $p99 = $f[4]; $avg = $f[5]
$ok = [double]$f[6]
"csv  conc=$conc thr=$thr p50=$p50 p99=$p99 avg=$avg ok=$ok inflight=$($f[14])"

# --- steady window: from loadStart+3 to the last frame before exit
$w = @($frames | Where-Object { $_.sec -ge ($loadStart + 3) -and $_.app -ne $null -and $_.ham -ne $null })
if ($w.Count -lt 4) { "not enough frames: $($w.Count)"; exit 1 }
$first = $w[0]; $last = $w[$w.Count - 1]
$dt = $last.sec - $first.sec
$appCore = ($last.app - $first.app) / $dt
$hamCore = ($last.ham - $first.ham) / $dt
"window = $([math]::Round($first.sec,1))..$([math]::Round($last.sec,1))s  dt=$([math]::Round($dt,1))s  frames=$($w.Count)"
"app      = $([math]::Round($appCore,2)) core  ($([math]::Round($appCore/$cores*100,1))% of box)"
"hammer   = $([math]::Round($hamCore,2)) core  ($([math]::Round($hamCore/$cores*100,1))% of box)"
"app+ham  = $([math]::Round($appCore+$hamCore,2)) / $cores core"

if ($ok -gt 0) {
    # NOTE: cores are measured over the steady sub-window (dt), while $ok covers the whole 20s.
    # Divide by $thr (req/s) - not by $ok - or the result is off by the window ratio.
    "app   cpu per request = $([math]::Round($appCore*1000000/$thr,1)) us"
    "ham   cpu per request = $([math]::Round($hamCore*1000000/$thr,1)) us"
}

# --- top threads by CPU over the same window
$deltas = New-Object System.Collections.ArrayList
foreach ($k in $last.thrd.Keys) {
    if ($first.thrd.ContainsKey($k)) {
        $d = $last.thrd[$k] - $first.thrd[$k]
        if ($d -gt 0.1) { [void]$deltas.Add([pscustomobject]@{ tid = $k; sec = $d; core = $d / $dt }) }
    }
}
"top threads (share of ONE core):"
$deltas | Sort-Object core -Descending | Select-Object -First 8 | ForEach-Object {
    $hex = '0x' + ([System.Convert]::ToString($_.tid, 16)).ToLower()
    "  nid=$hex core=$([math]::Round($_.core,2))  ($([math]::Round($_.core*100,0))%)"
}
"thread cores total = $([math]::Round((($deltas | Measure-Object -Property core -Sum).Sum),2))"

# --- redis, window values only
# INFO cpu is a monotonic counter, so the delta over the load interval is a real window value
# (unlike commandstats' usec_per_call when CONFIG RESETSTAT was not issued).
# The interval is measured, not assumed to be 20s: the child adds JVM start + drain on top.
$rcpu = ($u1 - $u0) + ($s1 - $s0)
"redis: user_delta=$([math]::Round($u1-$u0,2))s sys_delta=$([math]::Round($s1-$s0,2))s total=$([math]::Round($rcpu,2))s"
"redis elapsed = $([math]::Round($totalSec,1))s  (measured, not 20s)"
"redis core usage = $([math]::Round($rcpu/$totalSec*100,1))% of one core"
"redis cpu per request = $([math]::Round($rcpu*1000000/$ok,1)) us  (user $([math]::Round(($u1-$u0)*1000000/$ok,1)) + sys $([math]::Round(($s1-$s0)*1000000/$ok,1)))"
"--- window commandstats ---"
$cs | Where-Object { $_ -match 'cmdstat_(evalsha|eval|exists|hget|get|set|del|scan)' } | ForEach-Object { "  $_" }
