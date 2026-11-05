# After the lock is removed: how much of Redis's single core is now used, and what shape does the curve take?
$ErrorActionPreference = 'Continue'
powershell -NoProfile -ExecutionPolicy Bypass -File deploy/dev-server.ps1 up -WaitSec 120

$H = 'benchmark/hammer/BaselineHammer.java'
$B = 'http://127.0.0.1:8080'

java $H $B 0 0 9001 /api/dev/ping none --sweep=100 --seconds=3 --count=/api/dev/ping/count > logs\w0.log 2>&1
java $H $B 0 0 9001 /api/dev/lua/claim-sha resourceId --sweep=100 --seconds=3 --count=/api/dev/lua/count '--reset=/api/dev/lua/reset?resourceId=9001' > logs\w1.log 2>&1
java $H $B 0 0 9001 /api/dev/ping none --sweep=100,200,400,800 --seconds=5 --count=/api/dev/ping/count > logs\w2.log 2>&1
java $H $B 0 0 9001 /api/dev/lua/claim-sha resourceId --sweep=400,400 --seconds=5 --count=/api/dev/lua/count '--reset=/api/dev/lua/reset?resourceId=9001' > logs\w3.log 2>&1
"warmup done"
Get-Content logs\w3.log | Select-String -Pattern '^400,' | ForEach-Object { "  warmup: $($_.Line)" }

Invoke-WebRequest -Uri 'http://127.0.0.1:8080/api/dev/lua/reset?resourceId=9001' -Method POST -UseBasicParsing -TimeoutSec 120 | Out-Null
$raw0 = docker exec cs-redis redis-cli info cpu
$u0 = [double](($raw0 | Where-Object { $_ -match '^used_cpu_user:' }) -replace 'used_cpu_user:', '')
$s0 = [double](($raw0 | Where-Object { $_ -match '^used_cpu_sys:' }) -replace 'used_cpu_sys:', '')

java $H $B 0 0 9001 /api/dev/lua/claim-sha resourceId --sweep=400 --seconds=20 --count=/api/dev/lua/count > logs\v-sha-400.log 2>&1

$raw1 = docker exec cs-redis redis-cli info cpu
$u1 = [double](($raw1 | Where-Object { $_ -match '^used_cpu_user:' }) -replace 'used_cpu_user:', '')
$s1 = [double](($raw1 | Where-Object { $_ -match '^used_cpu_sys:' }) -replace 'used_cpu_sys:', '')
$thrLine = (Get-Content logs\v-sha-400.log | Select-String -Pattern '^400,').Line
"sha 400x20s: $thrLine"
"redis_user_delta = $([math]::Round($u1 - $u0, 3))s   sys_delta = $([math]::Round($s1 - $s0, 3))s"
$cpu = ($u1 - $u0) + ($s1 - $s0)
"redis_core_usage  = $([math]::Round($cpu / 20 * 100, 1))% of one core (over 20s)"
"usec_per_call_now = $([math]::Round($cpu * 1000000 / [double](($thrLine -split ',')[6]), 1))us"

docker exec cs-redis redis-cli info commandstats | Select-String "cmdstat_evalsha"

# curve shape with the lock gone: first 400 is the client warm-up and gets discarded
java $H $B 0 0 9001 /api/dev/lua/claim-sha resourceId --sweep=400,100,200,400,800 --seconds=5 --count=/api/dev/lua/count '--reset=/api/dev/lua/reset?resourceId=9001' > logs\v-sha-curve.log 2>&1
"--- curve (conc,thr,p50,p95,p99,avg,...) ---"
Get-Content logs\v-sha-curve.log | Select-String -Pattern '^[0-9]+,|^conc' | ForEach-Object { "  $($_.Line)" }
