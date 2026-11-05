# ABA verdict: baseline /claim (DefaultScriptExecutor, locked) vs /claim-sha (precomputed SHA1 + EVALSHA).
# Interleaved A-B-A so "warm-up drift" cannot masquerade as the effect.
$ErrorActionPreference = 'Continue'
powershell -NoProfile -ExecutionPolicy Bypass -File deploy/dev-server.ps1 up -WaitSec 120

$H = 'benchmark/hammer/BaselineHammer.java'
$B = 'http://127.0.0.1:8080'

# D5 warm-up path so the server side is at steady state before any measurement.
java $H $B 0 0 9001 /api/dev/ping none --sweep=100 --seconds=3 --count=/api/dev/ping/count > logs\v0.log 2>&1
java $H $B 0 0 9001 /api/dev/lua/claim resourceId --sweep=100 --seconds=3 --count=/api/dev/lua/count '--reset=/api/dev/lua/reset?resourceId=9001' > logs\v1.log 2>&1
java $H $B 0 0 9001 /api/dev/ping none --sweep=100,200,400,800 --seconds=5 --count=/api/dev/ping/count > logs\v2.log 2>&1
java $H $B 0 0 9001 /api/dev/lua/claim resourceId --sweep=100,200 --seconds=5 --count=/api/dev/lua/count '--reset=/api/dev/lua/reset?resourceId=9001' > logs\v3.log 2>&1
"warmup done"

$segs = @(
    @{ tag = 'A'; name = 'baseline  /claim';     path = '/api/dev/lua/claim' },
    @{ tag = 'B'; name = 'evalsha   /claim-sha'; path = '/api/dev/lua/claim-sha' },
    @{ tag = 'C'; name = 'baseline  /claim';     path = '/api/dev/lua/claim' }
)

foreach ($s in $segs) {
    # two 400-levels in one JVM: first discards client cold-JIT, second is the measurement
    java $H $B 0 0 9001 $s.path resourceId --sweep=400,400 --seconds=5 --count=/api/dev/lua/count '--reset=/api/dev/lua/reset?resourceId=9001' > "logs\verdict-$($s.tag).log" 2>&1
    $rows = @(Get-Content "logs\verdict-$($s.tag).log" | Select-String -Pattern '^400,')
    "--- $($s.tag)  $($s.name) ---"
    foreach ($r in $rows) { "    $($r.Line)" }
}
