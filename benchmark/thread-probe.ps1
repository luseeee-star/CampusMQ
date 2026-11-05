# Per-thread CPU sampling of the app JVM under load, plus one jstack snapshot.
# Goal: find whether a single thread (Lettuce event loop / Tomcat poller / GC) is pegged,
# which would explain the 66ms RT while total CPU stays at ~9% of the box.
$ErrorActionPreference = 'Continue'
$cores = (Get-CimInstance Win32_ComputerSystem).NumberOfLogicalProcessors
"logical_cores = $cores"

powershell -NoProfile -ExecutionPolicy Bypass -File deploy/dev-server.ps1 up -WaitSec 120

$H = 'benchmark/hammer/BaselineHammer.java'
$B = 'http://127.0.0.1:8080'

java $H $B 0 0 9001 /api/dev/ping none --sweep=100 --seconds=3 --count=/api/dev/ping/count > logs\t1.log 2>&1
java $H $B 0 0 9001 /api/dev/lua/claim resourceId --sweep=100 --seconds=3 --count=/api/dev/lua/count '--reset=/api/dev/lua/reset?resourceId=9001' > logs\t2.log 2>&1
java $H $B 0 0 9001 /api/dev/ping none --sweep=100,200,400,800 --seconds=5 --count=/api/dev/ping/count > logs\t3.log 2>&1
java $H $B 0 0 9001 /api/dev/lua/claim resourceId --sweep=400,400 --seconds=10 --count=/api/dev/lua/count '--reset=/api/dev/lua/reset?resourceId=9001' > logs\t4.log 2>&1

$appPid = (Get-NetTCPConnection -LocalPort 8080 -State Listen | Select-Object -First 1).OwningProcess
"app_pid = $appPid"

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
$psi.Arguments = "$H $B 0 0 9001 /api/dev/lua/claim resourceId --sweep=400 --seconds=25 --count=/api/dev/lua/count --reset=/api/dev/lua/reset?resourceId=9001"
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
while (-not $hp.HasExited) {
    $el = $sw.Elapsed.TotalSeconds
    [void]$frames.Add([pscustomobject]@{ sec = $el; cpu = (Get-ThreadSnapshot $appPid) })
    if (-not $stackDone -and $el -ge 14) {
        & jstack $appPid 2>&1 | Out-File -FilePath logs\jstack-app.txt -Encoding utf8
        $stackDone = $true
        "jstack captured at $([math]::Round($el,1))s"
    }
    Start-Sleep -Milliseconds 900
}
$sw.Stop()
$hp.WaitForExit()
Set-Content -Path logs\thread-load.log -Value $outTask.Result -Encoding UTF8

$csvLine = (Get-Content logs\thread-load.log | Select-String -Pattern '^400,').Line
"csv: $csvLine"

function Nearest-Frame($frames, [double]$target) {
    $best = $null
    $bestD = 1e9
    foreach ($f in $frames) {
        $d = [math]::Abs($f.sec - $target)
        if ($d -lt $bestD) { $bestD = $d; $best = $f }
    }
    return $best
}

$f0 = Nearest-Frame $frames 6
$f1 = Nearest-Frame $frames 22
$dt = $f1.sec - $f0.sec
"window = $([math]::Round($f0.sec,1))s .. $([math]::Round($f1.sec,1))s  dt=$([math]::Round($dt,1))s  frames=$($frames.Count)"

$deltas = New-Object System.Collections.ArrayList
foreach ($k in $f1.cpu.Keys) {
    if ($f0.cpu.ContainsKey($k)) {
        $d = $f1.cpu[$k] - $f0.cpu[$k]
        if ($d -gt 0.05) {
            [void]$deltas.Add([pscustomobject]@{ tid = $k; sec = $d; core = $d / $dt })
        }
    }
}

"top threads by CPU share (core = share of ONE core):"
$deltas | Sort-Object core -Descending | Select-Object -First 12 | ForEach-Object {
    $hex = '0x' + ([System.Convert]::ToString($_.tid, 16))
    "  tid=$($_.tid)  nid=$hex  cpu=$([math]::Round($_.sec,2))s  core=$([math]::Round($_.core,3))  ($([math]::Round($_.core * 100,0))% of one core)"
}

$sum = ($deltas | Measure-Object -Property core -Sum).Sum
"total sampled cores = $([math]::Round($sum,2))"
