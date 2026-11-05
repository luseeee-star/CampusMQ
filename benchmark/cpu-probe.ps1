# Measure CPU of app / consumer / hammer processes during a single load window.
# Purpose: decide whether "16-core box shared by client and server" is the real ceiling,
# and obtain the "Java side usage" number that was previously back-derived as 100% - 40.4%.
$ErrorActionPreference = 'Continue'
$cores = (Get-CimInstance Win32_ComputerSystem).NumberOfLogicalProcessors
"logical_cores = $cores"

powershell -NoProfile -ExecutionPolicy Bypass -File deploy/dev-server.ps1 up -WaitSec 120

$H = 'benchmark/hammer/BaselineHammer.java'
$B = 'http://127.0.0.1:8080'

# Warm up following the D5 path so the server side reaches steady state.
java $H $B 0 0 9001 /api/dev/ping none --sweep=100 --seconds=3 --count=/api/dev/ping/count > logs\k1.log 2>&1
java $H $B 0 0 9001 /api/dev/lua/claim resourceId --sweep=100 --seconds=3 --count=/api/dev/lua/count '--reset=/api/dev/lua/reset?resourceId=9001' > logs\k2.log 2>&1
java $H $B 0 0 9001 /api/dev/ping none --sweep=100,200,400,800 --seconds=5 --count=/api/dev/ping/count > logs\k3.log 2>&1
java $H $B 0 0 9001 /api/dev/lua/claim resourceId --sweep=100,200 --seconds=5 --count=/api/dev/lua/count '--reset=/api/dev/lua/reset?resourceId=9001' > logs\k4.log 2>&1

$appPid = (Get-NetTCPConnection -LocalPort 8080 -State Listen | Select-Object -First 1).OwningProcess
$conPid = (Get-NetTCPConnection -LocalPort 8082 -State Listen | Select-Object -First 1).OwningProcess
"app_pid = $appPid"
"consumer_pid = $conPid"

function Get-CpuSafe([int]$id) {
    try {
        return (Get-Process -Id $id -ErrorAction Stop).CPU
    } catch {
        return $null
    }
}

function Invoke-SampledRun([string[]]$jargs, [string]$tag) {
    # Start-Process fails on this box: env holds both HTTPS_PROXY and https_proxy and
    # PS 5.1 keeps them in a case-insensitive dictionary. Use ProcessStartInfo instead.
    $psi = [System.Diagnostics.ProcessStartInfo]::new()
    $psi.FileName = 'java'
    $psi.Arguments = ($jargs -join ' ')
    $psi.UseShellExecute = $false
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.WorkingDirectory = (Get-Location).Path
    $hp = [System.Diagnostics.Process]::Start($psi)
    $outTask = $hp.StandardOutput.ReadToEndAsync()
    $errTask = $hp.StandardError.ReadToEndAsync()

    $sw = [Diagnostics.Stopwatch]::StartNew()
    $rows = New-Object System.Collections.ArrayList
    while (-not $hp.HasExited) {
        $a = Get-CpuSafe $appPid
        $c = Get-CpuSafe $conPid
        $h = Get-CpuSafe $hp.Id
        [void]$rows.Add([pscustomobject]@{
            ms  = $sw.Elapsed.TotalMilliseconds
            app = $a
            con = $c
            ham = $h
        })
        Start-Sleep -Milliseconds 300
    }
    $sw.Stop()
    $hp.WaitForExit()
    $out = $outTask.Result
    Set-Content -Path "logs\cpu-$tag.out.log" -Value $out -Encoding UTF8
    return ,$rows
}

function Show-Window($rows, [double]$fromMs, [double]$toMs, [string]$tag) {
    $w = @($rows | Where-Object {
            $_.ms -ge $fromMs -and $_.ms -le $toMs -and
            $_.app -ne $null -and $_.ham -ne $null -and $_.con -ne $null
        })
    if ($w.Count -lt 2) { "  $tag : not enough samples"; return }
    $f = $w[0]
    $l = $w[$w.Count - 1]
    $dt = ($l.ms - $f.ms) / 1000.0
    $ac = ($l.app - $f.app) / $dt
    $cc = ($l.con - $f.con) / $dt
    $hc = ($l.ham - $f.ham) / $dt
    $srv = $ac + $cc
    $tot = $srv + $hc
    "  $tag  window=$([math]::Round($dt,1))s samples=$($w.Count)"
    "    app      = $([math]::Round($ac,2)) core  ($([math]::Round($ac / $cores * 100,1))% of box)"
    "    consumer = $([math]::Round($cc,2)) core"
    "    hammer   = $([math]::Round($hc,2)) core  ($([math]::Round($hc / $cores * 100,1))% of box)"
    "    server   = $([math]::Round($srv,2)) core"
    "    TOTAL    = $([math]::Round($tot,2)) / $cores core = $([math]::Round($tot / $cores * 100,1))% of box"
    "    redis/app ratio (core) = $([math]::Round(1.0 * $hc / $ac,2))"
}

# Run 1: single 400 level, 20s, cold client JVM (matches how most numbers were taken today).
"=== RUN 1: 400 x 20s (cold hammer JVM) ==="
$r1 = Invoke-SampledRun @($H, $B, '0', '0', '9001', '/api/dev/lua/claim', 'resourceId',
    '--sweep=400', '--seconds=20', '--count=/api/dev/lua/count',
    '--reset=/api/dev/lua/reset?resourceId=9001') 'r1'
$thr1 = (Get-Content logs\cpu-r1.out.log | Select-String -Pattern '^400,').Line
"  csv: $thr1"
Show-Window $r1 2000 19900 'steady'

# Run 2: two 400 levels in one JVM; only the second half counts (hammer JIT warmed).
"=== RUN 2: 400 x 10s, then 400 x 10s (warm hammer) ==="
$r2 = Invoke-SampledRun @($H, $B, '0', '0', '9001', '/api/dev/lua/claim', 'resourceId',
    '--sweep=400,400', '--seconds=10', '--count=/api/dev/lua/count',
    '--reset=/api/dev/lua/reset?resourceId=9001') 'r2'
$lines2 = (Get-Content logs\cpu-r2.out.log | Select-String -Pattern '^400,') | ForEach-Object { $_.Line }
$lines2 | ForEach-Object { "  csv: $_" }
Show-Window $r2 11500 19900 'second-level'
