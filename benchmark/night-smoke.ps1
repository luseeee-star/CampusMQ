# 2026-11-10 night smoke -- three criteria, one app life
#
# Scope (tonight's code changes only):
#   S1  studentId length rule   : over-long id -> 1001, stock untouched, no message
#   S2  metaCache single-flight : N concurrent cold misses must produce 1 DB load, not N
#   S3  CLOSED auto writer      : close_at passes -> status CLOSED + quota_report appears
#                                 (no manual trigger of any kind)
#
# ASCII only on purpose: PS 5.1 reads a BOM-less .ps1 as ANSI, and any non-ASCII byte
# turns into a parse error ("unexpected token }"). ASCII keeps this file runnable
# no matter how it was saved.
#
# Never touches r1 / r3: a claim on r1 would DECR its stock, and the whole point of
# tomorrow's 15:19 / 16:31 auto-verification is that r1 and r3 stay exactly as they are.

$ErrorActionPreference = 'Stop'
$base = 'http://127.0.0.1:8080'
$script:fail = 0

function Check($name, $ok, $detail) {
    if ($ok) { Write-Host ("PASS  {0}  [{1}]" -f $name, $detail) }
    else { Write-Host ("FAIL  {0}  [{1}]" -f $name, $detail); $script:fail++ }
}

function GetJson($url) {
    $raw = (curl.exe -s --noproxy '*' $url) -join ''
    return $raw | ConvertFrom-Json
}

function PostJson($url, $json) {
    $f = [IO.Path]::GetTempFileName()
    Set-Content -Path $f -Value $json -Encoding ascii
    $raw = (curl.exe -s --noproxy '*' -X POST -H 'Content-Type: application/json' -d "@$f" $url) -join ''
    Remove-Item $f -Force
    return $raw | ConvertFrom-Json
}

function Sql($q) {
    $out = & docker exec cs-mysql mysql -uroot -pcampusslot -N -B -e $q
    return ($out | Out-String).Trim()
}

function RedisCli($parts) {
    # redis-cli needs one argv element per token; a single "GET key" string is NOT split by it
    $out = & docker exec cs-redis redis-cli @parts
    return ($out | Out-String).Trim()
}

$now = Get-Date
$openAt = $now.AddHours(-1).ToString('yyyy-MM-ddTHH:mm:ss')
$far = $now.AddHours(24).ToString('yyyy-MM-ddTHH:mm:ss')

# ---- 0. baseline of the two activities that must not move ----
$r1Before = Sql "SELECT CONCAT(total_quota,'/',remain,'/',status) FROM campusslot.quota_resource WHERE id=1"
$r3Before = Sql "SELECT CONCAT(total_quota,'/',remain,'/',status) FROM campusslot.quota_resource WHERE id=3"
Write-Host "baseline r1=$r1Before r3=$r3Before"

# ---- 1. three throwaway activities ----
# A: quota 0 -> every claim is rejected by Lua on the full check, so 200 concurrent
#    requests exercise the meta-cache read path without consuming stock or sending MQ.
# B: quota 5 -> target of the over-long studentId test (stock must stay 5).
# C: close_at = now + 15s -> the only thing that closes it is the scheduled job.
$a = PostJson "$base/api/admin/resources" (@{
        deptId = 1; title = 'night smoke A single-flight'; totalQuota = 0
        openAt = $openAt; closeAt = $far
    } | ConvertTo-Json -Compress)
$b = PostJson "$base/api/admin/resources" (@{
        deptId = 1; title = 'night smoke B studentId'; totalQuota = 5
        openAt = $openAt; closeAt = $far
    } | ConvertTo-Json -Compress)
$c = PostJson "$base/api/admin/resources" (@{
        deptId = 1; title = 'night smoke C auto-close'; totalQuota = 2
        openAt = $openAt; closeAt = $now.AddSeconds(15).ToString('yyyy-MM-ddTHH:mm:ss')
    } | ConvertTo-Json -Compress)
$idA = $a.data.resourceId; $idB = $b.data.resourceId; $idC = $c.data.resourceId
Write-Host "created A=$idA B=$idB C=$idC"
foreach ($id in @($idA, $idB, $idC)) {
    # publish is POST with an empty body
    $p = PostJson "$base/api/admin/resources/$id/publish" '{}'
    Write-Host ("publish {0} -> after={1} stockRemain={2} collegeKeys={3} rosterKeys={4}" -f $id, $p.data.afterStatus, $p.data.stockRemain, $p.data.collegeKeysWritten, $p.data.rosterKeysWritten)
}

# ---- S1: studentId length rule ----
$long = 'S' + ('9' * 60)
# /api/claim/mq is POST (a GET there answers 9001 "method not supported" -- learned the hard way)
$resp = PostJson "$base/api/claim/mq?resourceId=$idB&studentId=$long" '{}' 
Check 'S1 reject over-long studentId with 1001' ($resp.code -eq 1001) ("code=$($resp.code) msg=$($resp.msg)")
$stockB = RedisCli @('GET', "quota:stock:$idB")
Check 'S1 stock untouched' ($stockB -eq '5') "quota:stock:$idB=$stockB (expect 5)"
$dbRemainB = Sql "SELECT remain FROM campusslot.quota_resource WHERE id=$idB"
Check 'S1 db remain untouched' ($dbRemainB -eq '5') "remain=$dbRemainB"
$recB = Sql "SELECT COUNT(*) FROM campusslot.quota_record WHERE resource_id=$idB"
Check 'S1 no record written' ($recB -eq '0') "quota_record rows=$recB"
$statB = GetJson "$base/api/claim/dev/stat?resourceId=$idB"
Check 'S1 no message in flight' ($statB.data.pending -eq 0) ("pending=$($statB.data.pending)")

# ---- S2: single-flight ----
# Five rounds. Each round deletes the meta key (forcing a cold miss) and then fires
# 200 threads that all start at once. Without coalescing, the threads that land in
# the 1-3ms DB window each issue their own selectById + countByResource; with it,
# exactly one does and the rest wait for its result.
$rounds = 5
for ($i = 1; $i -le $rounds; $i++) {
    $null = RedisCli @('DEL', "quota:res:$idA")
    java benchmark/hammer/BaselineHammer.java $base 200 1 $idA /api/claim/mq resourceId 2>$null | Out-Null
}
$m = ((curl.exe -s --noproxy '*' "$base/actuator/prometheus") -join "`n")
$leader = 0; $follower = 0; $fallback = 0; $hit = 0
foreach ($line in ($m -split "`n")) {
    if ($line -match 'campusslot_claim_meta_cache_total\{(.*)\}\s+([\d.]+)$') {
        $tags = $Matches[1]; $v = [double]$Matches[2]
        if ($tags -match 'miss_leader') { $leader = $v }
        if ($tags -match 'miss_follower') { $follower = $v }
        if ($tags -match 'follower_fallback') { $fallback = $v }
        if ($tags -match 'result="hit"') { $hit = $v }
    }
}
Write-Host ("S2 counters: leader={0} follower={1} fallback={2} hit={3} (rounds={4})" -f $leader, $follower, $fallback, $hit, $rounds)
Check 'S2 one load per expiry window' ($leader -le ($rounds + 1)) "miss_leader=$leader expect <= $($rounds+1)"
Check 'S2 followers were actually coalesced' ($follower -ge 20) "miss_follower=$follower expect >= 20"
Check 'S2 no fallback (leader never stalled)' ($fallback -eq 0) "follower_fallback=$fallback"

# ---- S3: CLOSED auto writer, zero manual trigger ----
Write-Host 'waiting for close_at + one scan period (25s)...'
Start-Sleep -Seconds 25
$stC = Sql "SELECT status FROM campusslot.quota_resource WHERE id=$idC"
Check 'S3 status flipped by the job' ($stC -eq 'CLOSED') "status=$stC (expect CLOSED)"
$repC = Sql "SELECT CONCAT(total_quota,'/',granted_cnt,'/',released_cnt,'/',locked_cnt,'/',remain,'/',balanced) FROM campusslot.quota_report WHERE resource_id=$idC"
Check 'S3 final report written' ([bool]$repC) "quota_report=$repC (expect total/granted/released/locked/remain/balanced)"
# The bench profile pins com.campusslot to WARN, so the job's INFO line never reaches
# the log. The decisive evidence is the time gap instead: the report must have been
# generated a few seconds AFTER close_at, by a process, without any manual trigger.
$gap = Sql "SELECT TIMESTAMPDIFF(SECOND, r.close_at, p.generated_at) FROM campusslot.quota_resource r JOIN campusslot.quota_report p ON p.resource_id = r.id WHERE r.id=$idC"
Check 'S3 report followed close_at automatically' (([int]$gap -ge 0) -and ([int]$gap -le 10)) "generated_at - close_at = $gap s (expect 0..10)"

# ---- 4. the two protected activities must be byte-identical ----
$r1After = Sql "SELECT CONCAT(total_quota,'/',remain,'/',status) FROM campusslot.quota_resource WHERE id=1"
$r3After = Sql "SELECT CONCAT(total_quota,'/',remain,'/',status) FROM campusslot.quota_resource WHERE id=3"
Check 'r1 untouched' ($r1After -eq $r1Before) "before=$r1Before after=$r1After"
Check 'r3 untouched' ($r3After -eq $r3Before) "before=$r3Before after=$r3After"

Write-Host ''
if ($script:fail -gt 0) { Write-Host ("SMOKE RED: {0} failure(s)" -f $script:fail); exit 1 }
Write-Host 'SMOKE GREEN: all criteria passed'
