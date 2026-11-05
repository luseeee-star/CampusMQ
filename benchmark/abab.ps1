# Verdict harness for the DefaultRedisScript.getSha1() lock (2026-11-03).
#
# Two modes:
#   -Mode abab      alternating A B A B at one concurrency. Established A/B = 2.27.
#   -Mode gradient  same JVM, both modes at 100/200/400/800, ascending + descending.
#                   Asks whether X depends on N (is the lock a pure serial section?)
#                   and whether 800 is already overloaded (optimum near 400).
#
# Why the same JVM / alternating arms:
#   A restart-level cold start is a ~0.62 factor, the same size as the effect.
#   Only alternating arms inside one process average warm-up and drift.
#
# Per-step protocol (both modes):
#   hammer posts the toggle -> prints "#step" -> sleeps 800ms. The script uses that
#   window to CONFIG RESETSTAT (commandstats are cumulative since boot), snapshot
#   INFO cpu (CPU counters are not resettable -> differenced), and VERIFY via GET
#   /lua-mode that the switch really moved. An unverified toggle is how the first
#   run of this script silently measured six no-lock arms.
#
# Data is written to logs/abab-<mode>-<timestamp>.csv. Console-only output is not
# acceptable: a month later the per-arm detail is unrecoverable (same bug as the
# old dev-server.ps1 deleting logs).
#
# NOTE on "pending": /api/claim/dev/progress computes redisConsumed - dbConsumed.
# dev reset currently clears Redis but NOT the DB (that is a separate TODO), so the
# absolute value is offset/negative. Only the per-arm DELTA is meaningful here.
# Do not read the absolute number as queue depth.
#
# Usage:
#   powershell -NoProfile -ExecutionPolicy Bypass -File benchmark/abab.ps1 -Mode abab
#   powershell -NoProfile -ExecutionPolicy Bypass -File benchmark/abab.ps1 -Mode gradient

param(
    [ValidateSet('abab','gradient')] [string]$Mode = 'abab'
)

$ErrorActionPreference = 'Stop'
# PowerShell 5.1: Invoke-WebRequest writes a progress bar to the information stream.
# When the caller pipes 2>&1 through Select-Object, that record can terminate the run
# under ErrorActionPreference=Stop. Silence it (this exact failure happened 2026-11-03).
$ProgressPreference = 'SilentlyContinue'

$Base   = 'http://127.0.0.1:8080'
$Hammer = 'benchmark/hammer/BaselineHammer.java'
$ResId  = 1
$Quota  = 1000000
$TogglePath = "/api/claim/dev/lua-mode"
$DrainTimeoutMs = 60000

$toggleUrl = { param($v) "$TogglePath" + '?legacy=' + $v }

if ($Mode -eq 'gradient') {
    $WinSec    = 3
    $WarmSteps = 4                 # warm-up steps per mode (discarded)
    $asc  = @('100','200','400','800')
    $desc = @('800','400','200','100')
    # per mode: warm(4 asc) + meas(4 asc) + meas(4 desc); ascending+descending
    # cancels the monotonic drift from broker backlog / JIT / GC.
    $sweepParts   = @()
    $toggleParts  = @()
    foreach ($modeVal in @('true','false')) {   # true=legacy(lock), false=evalsha(no lock)
        foreach ($cc in $asc)  { $sweepParts += $cc; $toggleParts += (& $toggleUrl $modeVal) }
        foreach ($cc in $asc)  { $sweepParts += $cc; $toggleParts += (& $toggleUrl $modeVal) }
        foreach ($cc in $desc) { $sweepParts += $cc; $toggleParts += (& $toggleUrl $modeVal) }
    }
    $PerModeSteps = 12
} else {
    $WinSec    = 5
    $WarmSteps = 4
    $armModes  = @('false','true','false','true')
    $warmModes = @('false','true','false','true')
    $sweep = (@('400') * ($WarmSteps + 4)) -join ','
    $toggles = (($warmModes | ForEach-Object { & $toggleUrl $_ }) + ($armModes | ForEach-Object { & $toggleUrl $_ }))
    $toggle = ($toggles -join ',')
}
if ($Mode -eq 'gradient') {
    $sweep  = $sweepParts -join ','
    $toggle = $toggleParts -join ','
}

function Get-RedisCpu {
    $raw = docker exec cs-redis redis-cli info cpu 2>$null
    $usr = 0.0; $sys = 0.0
    foreach ($l in $raw) {
        if ($l -match '^used_cpu_user:') { $usr = [double]($l -replace '^used_cpu_user:', '') }
        if ($l -match '^used_cpu_sys:')  { $sys = [double]($l -replace '^used_cpu_sys:', '') }
    }
    return @($usr, $sys)
}

function Get-CmdstatEvalsha {
    $raw = docker exec cs-redis redis-cli info commandstats 2>$null
    foreach ($l in $raw) {
        if ($l -match '^cmdstat_evalsha:calls=(\d+),usec=(\d+)') {
            return @{ calls = [long]$Matches[1]; usec = [long]$Matches[2] }
        }
    }
    return @{ calls = 0; usec = 0 }
}

function Get-Backlog {
    try {
        $pr = Invoke-RestMethod -Uri "$Base/api/claim/dev/progress?resourceId=$ResId" -Method GET -TimeoutSec 30
        $pv = -1; $rv = -1
        if ($pr.data -and $null -ne $pr.data.pending)  { $pv = [int]$pr.data.pending }
        if ($pr.data -and $null -ne $pr.data.records)  { $rv = [int]$pr.data.records }
        return @{ pending = $pv; records = $rv }
    } catch {
        return @{ pending = -1; records = -1 }
    }
}

# ---- 1. app must be up (bench profile, dev endpoints on) ----
$healthy = $false
try {
    $resp0 = Invoke-WebRequest -Uri "$Base/actuator/health" -UseBasicParsing -TimeoutSec 5
    $healthy = ($resp0.StatusCode -eq 200)
} catch {}
if (-not $healthy) {
    powershell -NoProfile -ExecutionPolicy Bypass -File deploy/dev-server.ps1 up -WaitSec 150
}
try {
    $resp = Invoke-WebRequest -Uri "$Base/actuator/health" -UseBasicParsing -TimeoutSec 10
    if ($resp.StatusCode -ne 200) { throw "app health $($resp.StatusCode)" }
} catch { throw "app not healthy, aborting (a run with a dead server produces fake data)" }

# ---- 2. toggle endpoint must exist ----
try {
    $modeCheck = Invoke-RestMethod -Uri "$Base$TogglePath" -TimeoutSec 10
    if ($null -eq $modeCheck.data.mode) { throw "lua-mode endpoint returned unexpected body" }
} catch { throw "lua-mode endpoint missing: the running jar predates this change, rebuild first" }

# ---- 3. reset once, big quota, no reset between arms ----
$base0 = Get-Backlog
"reset: pendingStart=$($base0.pending) recordsStart=$($base0.records) (absolute pending is offset; delta only)"
$force = 'false'
if ($base0.pending -ne 0) { $force = 'true' }
$null = Invoke-WebRequest -Uri "$Base/api/claim/dev/reset?resourceId=$ResId&quota=$Quota&force=$force" `
        -Method POST -UseBasicParsing -TimeoutSec 600

# ---- 4. run: one JVM ----
"running: mode=$Mode sweep=$sweep winSec=$WinSec"
if ($Mode -eq 'abab') {
    "NOTE: the old '5,400-5,700 predicted band' was carried over from the D7 probe"
    "      (another endpoint, another day). It is NOT an independent prediction and"
    "      is no longer used as a verdict criterion. Report the measured ratio only."
}

$rows = @()
$cpuBefore = @(0.0, 0.0)
$curStep = -1
$stepPending = -1
$stepRecords = -1
$expectedMode = '-'

& java $Hammer $Base 0 0 $ResId /api/claim/mq resourceId --sweep=$sweep --seconds=$WinSec --step-toggle=$toggle 2>&1 |
ForEach-Object {
    $line = "$_"
    if ($line -match '^#step (\d+)') {
        $curStep = [int]$Matches[1]
        if ($Mode -eq 'gradient') {
            $modeIdx = [int][math]::Floor($curStep / $PerModeSteps)
            $expectedMode = 'legacy'; if ($modeIdx -eq 1) { $expectedMode = 'evalsha' }
        } else {
            $isArm = ($curStep -ge $WarmSteps)
            if ($isArm) { $expectedMode = $armModes[$curStep - $WarmSteps] } else { $expectedMode = '-' }
        }
        # commandstats are cumulative since boot -> RESETSTAT gives this arm its own window
        $null = docker exec cs-redis redis-cli config resetstat 2>$null
        $cpuBefore = Get-RedisCpu
        # verify the switch really moved; an unverified toggle produced six no-lock arms
        $ver = Invoke-RestMethod -Uri "$Base$TogglePath" -TimeoutSec 10
        $actual = $ver.data.mode
        $modeNote = 'evalsha'
        if ($actual -eq 'legacy') { $modeNote = 'legacy' }
        $mismatch = ''
        if ($expectedMode -ne '-') {
            $want = 'evalsha'; if ($expectedMode -eq 'true' -or $expectedMode -eq 'legacy') { $want = 'legacy' }
            if ($actual -ne $want) { $mismatch = "  !!! MISMATCH want=$want" }
        }
        $bk = Get-Backlog
        $stepPending = $bk.pending
        $stepRecords = $bk.records
        "  [step $curStep] mode=$modeNote$($mismatch) pendingStart=$stepPending RESETSTAT done"
    }
    elseif ($line -match '^\d+,') {
        $f = $line -split ','
        $cpuAfter = Get-RedisCpu
        $cs = Get-CmdstatEvalsha
        $cpuDelta = ($cpuAfter[0] - $cpuBefore[0]) + ($cpuAfter[1] - $cpuBefore[1])
        $redisCore = if ($WinSec -gt 0) { [math]::Round($cpuDelta / $WinSec * 100, 1) } else { 0 }
        $upc = if ($cs.calls -gt 0) { [math]::Round($cs.usec / $cs.calls, 1) } else { 0 }
        $rowMode = 'warm'
        if ($Mode -eq 'gradient') {
            $modeIdx = [int][math]::Floor($curStep / $PerModeSteps)
            $localIdx = $curStep % $PerModeSteps
            $rowMode = 'legacy'; if ($modeIdx -eq 1) { $rowMode = 'evalsha' }
            if ($localIdx -lt $WarmSteps) { $rowMode = 'warm' }
        } else {
            if ($curStep -ge $WarmSteps) {
                $rowMode = 'evalsha'; if ($armModes[$curStep - $WarmSteps] -eq 'true') { $rowMode = 'legacy' }
            }
        }
        $inflight = 0
        if ($f.Count -gt 14) { $inflight = [long]$f[14] }
        $rows += [pscustomobject]@{
            step = $curStep; mode = $rowMode; conc = [long]$f[0]
            thr = [long]$f[1]; p50 = [long]$f[2]; p99 = [long]$f[4]
            ok = [long]$f[6]; other = [long]$f[8]; err = [long]$f[9]
            inflightAvg = $inflight
            redisCorePct = $redisCore; evalCalls = $cs.calls; usecPerCall = $upc
            pendingStart = $stepPending; recordsStart = $stepRecords
        }
        "  [step $curStep] row captured conc=$($f[0]) thr=$($f[1])"
    }
    elseif ($line -match '^#') {
        "  $line"
    }
}

# ---- 5. persist the whole table (console-only output was the old bug) ----
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$csvPath = "logs/abab-$Mode-$stamp.csv"
$rows | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8
"csv written: $csvPath  rows=$($rows.Count)"

""
"==== $Mode table (mode: evalsha=no-lock, legacy=framework-lock) ===="
"step mode    conc   thr    p50  p99  inflight redis%  evalCalls  us/call  ok      pendStart"
foreach ($r in $rows) {
    "{0,4} {1,-7} {2,5} {3,6} {4,5} {5,5} {6,7} {7,5}%  {8,9}  {9,7}  {10,7} {11,9}" -f `
        $r.step, $r.mode, $r.conc, $r.thr, $r.p50, $r.p99, $r.inflightAvg, $r.redisCorePct, `
        $r.evalCalls, $r.usecPerCall, $r.ok, $r.pendingStart
}

# ---- 6. verdict ----
if ($Mode -eq 'abab') {
    $arms = @($rows | Where-Object { $_.mode -ne 'warm' })
    if ($arms.Count -eq 4) {
        $a1 = $arms[0]; $b1 = $arms[1]; $a2 = $arms[2]; $b2 = $arms[3]
        $aAvg = [math]::Round(($a1.thr + $a2.thr) / 2)
        $bAvg = [math]::Round(($b1.thr + $b2.thr) / 2)
        $noiseA = [math]::Round([math]::Abs($a1.thr - $a2.thr) / $aAvg * 100, 1)
        $noiseB = [math]::Round([math]::Abs($b1.thr - $b2.thr) / $bAvg * 100, 1)
        $ratio  = [math]::Round($aAvg / [double]$bAvg, 3)
        ""
        "A(no-lock)  avg=$aAvg  arms=$($a1.thr)/$($a2.thr)  noise=$noiseA%"
        "B(legacy)   avg=$bAvg  arms=$($b1.thr)/$($b2.thr)  noise=$noiseB%"
        "A/B = $ratio"
        "verdict: report the ratio only. No predicted-band comparison (see NOTE above)."
    } else {
        "WARNING: captured $($arms.Count) measured arms, expected 4 -- data incomplete, do not use"
    }
} else {
    $meas = @($rows | Where-Object { $_.mode -ne 'warm' })
    if ($meas.Count -ne 16) {
        "WARNING: captured $($meas.Count) measured steps, expected 16 -- data incomplete, do not use"
    } else {
        foreach ($md in @('legacy','evalsha')) {
            $g = @($meas | Where-Object { $_.mode -eq $md } | Group-Object conc)
            ""
            "---- $md : X vs N (ascending+descending averaged) ----"
            $byConc = @{}
            foreach ($grp in $g) {
                $sorted = @($grp.Group | Sort-Object step)
                $avg = [math]::Round((($sorted | Measure-Object -Property thr -Average).Average))
                $byConc[[int]$grp.Name] = $avg
                "{0,5} : first={1,7} second={2,7} avg={3,7}" -f $grp.Name, `
                    $sorted[0].thr, $sorted[1].thr, $avg
            }
            $v200 = $byConc[200]; $v400 = $byConc[400]; $v800 = $byConc[800]
            if ($md -eq 'legacy') {
                $spread = [math]::Round([math]::Abs($v800 - $v400) / $v400 * 100, 1)
                "legacy spread 400->800 = $spread%  (plateau means X is independent of N)"
                if ([math]::Abs($spread) -le 6 -and $v200 -ge $v400 * 0.85) {
                    "verdict: X is flat in N -> lock is a pure serial section on the MAIN link;"
                    "         S_main = 1/X is measured on this link itself (no probe comparison needed)"
                } elseif ($v800 -gt $v400 * 1.10) {
                    "verdict: X rises with N -> the lock was NOT saturated at 400;"
                    "         supply-side effects are real, section 4b must be rewritten"
                } else {
                    "verdict: X falls at 800 -> overload; optimum at or below 400"
                }
            } else {
                $drop = [math]::Round(($v400 - $v800) / $v400 * 100, 1)
                "evalsha change 400->800 = -$drop%"
                if ($v800 -lt $v400 * 0.92) {
                    "verdict: 800 is already overloaded -> optimum near 400 (same-JVM evidence)"
                } else {
                    "verdict: 800 not clearly below 400 -> 'optimum at 400' NOT confirmed"
                }
            }
        }
    }
}

# ---- 7. drain at the end: do not leave a backlog for the next run ----
""
"drain: waiting up to $DrainTimeoutMs ms for the queue to catch up"
try {
    $dr = Invoke-RestMethod -Uri "$Base/api/claim/dev/drain?resourceId=$ResId&timeoutMs=$DrainTimeoutMs" -TimeoutSec ($DrainTimeoutMs / 1000 + 60)
    "drain: pending=$($dr.data.pending) drained=$($dr.data.drained) driftNegative=$($dr.data.driftNegative) timedOut=$($dr.data.timedOut) costMs=$($dr.data.costMs)"
    if ($dr.data.driftNegative) {
        "drain NOTE: driftNegative=true means DB consumed more than Redis (dev reset does not"
        "            clear the DB). The endpoint returns immediately; it is NOT proof of an empty"
        "            queue. Confirm via records stabilization before starting S-3."
    }
    if ($dr.data.timedOut) {
        "drain WARNING: timed out with pending=$($dr.data.pending). Backlog left in MQ; do NOT"
        "               start S-3 acceptance until another drain confirms it is clear."
    }
} catch {
    "drain ERROR: $($_.Exception.Message) -- backlog state unknown, do NOT start S-3"
}
