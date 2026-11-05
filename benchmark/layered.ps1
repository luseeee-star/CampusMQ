<#
.SYNOPSIS
    D5 补齐：三层防线对照（§8.2）+ 失败率（NFR-2）+ Redis commandstats。

.DESCRIPTION
    与 d5-sweep.ps1 回答的是两个不同的问题，不能互相替代：

        d5-sweep.ps1  同一个 JVM 里逐层加组件（ping → +Redis → +MQ）
                      → 答"时间花在哪一层"

        本脚本        三个不同实现打同一个语义的接口（裸 DB / 同步 / 异步）
                      → 答"每层防线挡住了多少"

    后者才是计划 §8.2「分层效果」要的东西。三个端点至今都还在：
        /api/baseline/claim   无 Redis、无锁、无事务          → 会超卖
        /api/claim            Redis Lua + 同步落库            → 不超卖，但要等两次写
        /api/claim/mq         Redis Lua + MQ 异步落库          → 不超卖，入口不等 DB

    <b>每层必须用同一个 JVM 跑完整条曲线。</b>这不是性能优化，是数据有效性：
    如果每档都起一个新 java 进程，每一档都是一次冷启动 —— 客户端要重建连接、
    重新做 topic 路由发现，800 个线程会同时排队等同一次查询。
    实测这一项能把 D3 的 800 并发档从 3701 打到 314 req/s，差十倍，
    而曲线形状看起来像"过载"，会让人得出完全错误的结论。
    所以档间的 drain / reset 由压测器自己发（--drain / --reset），不回到 PowerShSh。

    两段实验，缺一不可：

    段 A「吞吐层」quota 给得很大（默认 20 万），保证任何一档都耗不尽名额。
        这样测到的才是"抢名额有多快"。如果名额在半途耗尽，第 2 档起每个请求都
        直接返回 2001，测出来的其实是"拒绝一个请求有多快"。

    段 B「正确性层」quota 故意给得很小（默认 500），并发远大于它。
        只有请求数真的超过名额，"会不会超卖"这个问题才被问到。
        D1 会留下负的 remain，D2/D3 的 issued 精确等于名额，这就是三道防线的直接证据。

    判据三个层不是一个：
        D1     remain < 0 即超卖（裸表没有 CHECK，超卖就表现为负数本身）
        D2/D3  remain + issued == totalQuota（D8 取消之后 records 可以合法超过名额，
               所以不能用 records > totalQuota，那个判据会天天误报）

    必须由 experiment.ps1 调用（本机执行环境每次调用结束会回收整个进程树）：
        powershell -NoProfile -ExecutionPolicy Bypass -File deploy/experiment.ps1 `
            -Work "& benchmark/layered.ps1"

.PARAMETER Steps
    并发档位。三层用同一组，才能横向比。

.PARAMETER ClaimSeconds
    D3 每档的窗口。它比另外两层短，是因为消费侧只有 300~350 条/秒（见 LOGBOOK D5），
    5 秒窗口能压进 2 万条、要 60 秒才追平。2 秒窗口的吞吐仍然稳定（D5 实测两轮差 <8%）。

.PARAMETER RedisContainer
    Redis 容器名。用来读 INFO commandstats，算 claim.lua 的真实单核占用。
#>
[CmdletBinding()]
param(
    [string]$Base = 'http://127.0.0.1:8080',
    [string]$Steps = '100,200,400,800',
    [int]$Seconds = 5,
    [int]$ClaimSeconds = 2,
    [int]$SoftQuota = 200000,
    [int]$TightQuota = 500,
    [int]$TightConc = 100,
    [int]$TightSeconds = 3,
    [int]$WarmupSeconds = 5,
    [int]$WarmupConc = 100,
    [string]$RedisContainer = 'cs-redis',
    [string[]]$Layers = @('D1', 'D2', 'D3'),
    [switch]$SkipWarmup,
    [switch]$SkipCorrectness
)

$ErrorActionPreference = 'Stop'

$java = (Get-Command java.exe).Source
$logDir = Join-Path (Get-Location) 'logs'
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }
$log = Join-Path $logDir 'layered.log'
Set-Content -Path $log -Value '' -Encoding UTF8
$csvPath = Join-Path $logDir 'layered.csv'

# 统一用 curl.exe + --noproxy：PowerShell 5.1 的 Invoke-* 会读系统代理，
# 本机的 HTTP(S)_PROXY 指向不存在的代理，本地回环会被拦成超时。
function CGet([string]$Url) {
    return ((& curl.exe --noproxy '*' -s --max-time 600 $Url) -join '')
}

function CPost([string]$Url) {
    return ((& curl.exe --noproxy '*' -s --max-time 600 -X POST $Url) -join '')
}

function Get-Layer([string]$Code) {
    switch ($Code) {
        'D1' {
            return [pscustomobject]@{
                Code = 'D1'
                Name = 'bare DB (no redis / no lock / no tx)'
                Path = '/api/baseline/claim'
                IdParam = 'activityId'
                Id = 1
                ResetFmt = '/api/baseline/reset?activityId=1&quota={0}'
                DrainPath = $null
                Stat = '/api/baseline/stat?activityId=1'
                Window = $Seconds
                Kind = 'baseline'
            }
        }
        'D2' {
            return [pscustomobject]@{
                Code = 'D2'
                Name = 'redis lua + sync db persist'
                Path = '/api/claim'
                IdParam = 'resourceId'
                Id = 1
                ResetFmt = '/api/claim/dev/reset?resourceId=1&quota={0}'
                DrainPath = $null
                Stat = '/api/claim/dev/stat?resourceId=1'
                Window = $Seconds
                Kind = 'claim'
            }
        }
        'D3' {
            return [pscustomobject]@{
                Code = 'D3'
                Name = 'redis lua + mq async persist'
                Path = '/api/claim/mq'
                IdParam = 'resourceId'
                Id = 1
                ResetFmt = '/api/claim/dev/reset?resourceId=1&quota={0}'
                DrainPath = '/api/claim/dev/drain?resourceId=1&timeoutMs=300000'
                Stat = '/api/claim/dev/stat?resourceId=1'
                Window = $ClaimSeconds
                Kind = 'claim'
            }
        }
        default { throw "未知层：$Code（只支持 D1/D2/D3）" }
    }
}

# ---------------------------------------------------------------------------
# Redis：claim.lua 的真实开销
# ---------------------------------------------------------------------------
# INFO commandstats 的 usec 是该命令在 Redis 主线程里累计执行的时间。
# 它比 CPU 百分比更有用：CPU 会被网络线程、fork、持久化稀释，而 usec 只算脚本本身。
# usec 增量 / 窗口时长 = 单核占用率，直接回答"6.7k 是 Redis 到顶了还是 HTTP 层到顶了"。
function Get-RedisEval {
    try {
        $out = & docker exec $RedisContainer redis-cli INFO commandstats 2>$null
    }
    catch {
        return $null
    }
    if (-not $out) { return $null }
    $line = ($out | Where-Object { $_ -match '^cmdstat_evalsha:' } | Select-Object -First 1)
    if (-not $line) { return $null }
    $m = [regex]::Match([string]$line, 'calls=(\d+),usec=(\d+)')
    if (-not $m.Success) { return $null }
    return [pscustomobject]@{ Calls = [long]$m.Groups[1].Value; Usec = [long]$m.Groups[2].Value }
}

function Wait-Drain([int]$TimeoutSec = 300) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    $pending = $null
    while ((Get-Date) -lt $deadline) {
        $raw = CGet ($Base + '/api/claim/dev/progress?resourceId=1')
        try {
            $obj = $raw | ConvertFrom-Json
        }
        catch {
            Start-Sleep -Milliseconds 200
            continue
        }
        $pending = $obj.data.pending
        if ($null -ne $pending -and [int]$pending -le 0) {
            return $true
        }
        Start-Sleep -Milliseconds 200
    }
    Write-Host ('    !!! drain 超时，pending=' + $pending)
    return $false
}

function Reset-Layer($L, [int]$Quota) {
    if ($L.DrainPath) {
        Wait-Drain | Out-Null
    }
    $url = $Base + ($L.ResetFmt -f $Quota)
    $resp = CPost $url
    if ($resp -notmatch '"code":0') {
        throw ("复位失败 " + $url + " → " + $resp)
    }
}

# 把 hammer 的 stdout 拆成「每档一行」的观测值。
# 表头那行（conc,thr,...）以字母开头，天然不会被数字正则匹配到。
# 列：conc,thr,p50,p95,p99,avg,ok,full,other,err,limited,fail%,failAll%,hit
# 末列 hit 是「服务端命中/客户端总数」或 '-'，所以正则到 failAll% 后面那个逗号就停。
function ConvertFrom-HammerRows($out, $L) {
    $rows = @()
    foreach ($line in $out) {
        $m = [regex]::Match([string]$line,
            '^(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),([0-9.]+),([0-9.]+),')
        if (-not $m.Success) { continue }
        $rows += [pscustomobject]@{
            Layer = $L.Code
            Conc = [int]$m.Groups[1].Value
            Thr = [int]$m.Groups[2].Value
            P50 = [int]$m.Groups[3].Value
            P95 = [int]$m.Groups[4].Value
            P99 = [int]$m.Groups[5].Value
            Avg = [int]$m.Groups[6].Value
            Ok = [int]$m.Groups[7].Value
            Full = [int]$m.Groups[8].Value
            Other = [int]$m.Groups[9].Value
            Err = [int]$m.Groups[10].Value
            Limited = [int]$m.Groups[11].Value
            FailPct = [double]$m.Groups[12].Value
            FailAllPct = [double]$m.Groups[13].Value
        }
    }
    return $rows
}

function Get-Verdict($L) {
    $stat = (CGet ($Base + $L.Stat)) | ConvertFrom-Json
    if ($L.Kind -eq 'baseline') {
        $remain = [int]$stat.data.remain
        $verdict = if ($remain -lt 0) { 'oversold=' + (-$remain) } else { 'no-oversell' }
        $detail = 'remain=' + $remain + ' records=' + $stat.data.records
    }
    else {
        $invariant = [bool]$stat.data.quotaInvariant
        $verdict = if ($invariant -and [int]$stat.data.oversold -le 0) { 'invariant-ok' } else { 'BROKEN' }
        $detail = 'issued=' + $stat.data.issued + ' dbRemain=' + $stat.data.dbRemain
        $detail = $detail + ' delta=' + $stat.data.invariantDelta + ' pending=' + $stat.data.pending
    }
    return [pscustomobject]@{ Verdict = $verdict; Detail = $detail }
}

# ---------------------------------------------------------------------------
# 段 A：吞吐层（每层一次 JVM，跑完整条曲线）
# ---------------------------------------------------------------------------
$all = @()
$layerMeta = @()

Write-Host ('=== 段 A · 分层吞吐对照  steps=' + $Steps + ' seconds=' + $Seconds + '/' + $ClaimSeconds +
    ' quota=' + $SoftQuota + ' ===')

foreach ($code in $Layers) {
    $L = Get-Layer $code
    Write-Host ' '
    Write-Host ('--- ' + $L.Code + '  ' + $L.Name + '  [' + $L.Path + ']')

    # 预热档跟着同一条曲线一起跑：把 50 并发那一档挂在最前面，跑完直接丢掉。
    # 这样预热和正式测量在同一个 JVM 里，JIT 和连接池都是热的，而第一档的冷启动数据不会进表。
    # 不能"先单独跑一次预热"—— 那又是一次进程边界，等于把要避免的问题原样搬回来。
    $warmPrefix = if ($SkipWarmup) { '' } else { [string]$WarmupConc + ',' }
    $fullSteps = $warmPrefix + $Steps
    $totalWindows = ($fullSteps.Split(',').Count) * $L.Window

    $before = Get-RedisEval
    $hammerArgs = @(
        $Base, '0', '0', [string]$L.Id, $L.Path, $L.IdParam,
        ('--sweep=' + $fullSteps), ('--seconds=' + $L.Window),
        ('--reset=' + ($L.ResetFmt -f $SoftQuota))
    )
    if ($L.DrainPath) {
        $hammerArgs += ('--drain=' + $L.DrainPath)
    }
    $out = & $java 'benchmark/hammer/BaselineHammer.java' @hammerArgs 2>&1
    $out | Tee-Object -FilePath $log -Append | Out-Null
    $after = Get-RedisEval

    $rows = ConvertFrom-HammerRows $out $L
    if ($rows.Count -eq 0) {
        throw ('压测器没有输出任何 CSV 行（' + $L.Code + '）')
    }
    if (-not $SkipWarmup) {
        $rows = @($rows | Select-Object -Skip 1)
    }

    # Redis 单核占用按整条曲线算：evalsha 的总 usec / 总墙钟。
    # 不分档，因为一次 JVM 调用只取得到首尾两个采样点 —— 分档要么再起进程（回到冷启动陷阱），
    # 要么在压测中途插 HTTP 请求（污染窗口）。层级别的均值足够回答"Redis 是不是到顶了"。
    $luaCalls = '-'
    $luaUsec = '-'
    $core = '-'
    if ($null -ne $before -and $null -ne $after -and $totalWindows -gt 0) {
        $dc = $after.Calls - $before.Calls
        $du = $after.Usec - $before.Usec
        if ($dc -gt 0) {
            $luaCalls = $dc
            $luaUsec = [math]::Round($du / $dc, 2)
            $core = [math]::Round(100.0 * $du / ($totalWindows * 1000000.0), 1)
        }
    }
    $layerMeta += [pscustomobject]@{
        Layer = $L.Code; LuaCalls = $luaCalls; LuaUs = $luaUsec; RedisCore = $core
    }

    # 整层压完必须先把队列落干净再读 stat，两个后果都很难看：
    #   1. 读到的 pending 是"还在队列里"，会被判成 BROKEN —— 上一轮就是这么误判的；
    #   2. 遗留积压会让下一层的复位被守卫拒绝（9001），整轮实验在这里断掉。
    # hammer 只负责档间的 drain，最后一档之后没人管，所以这里补一次。
    $drainSec = 0
    if ($L.DrainPath) {
        $drainStart = Get-Date
        Wait-Drain | Out-Null
        $drainSec = [math]::Round(((Get-Date) - $drainStart).TotalSeconds, 1)
    }

    $v = Get-Verdict $L
    if ($drainSec -gt 0) {
        $v.Detail = $v.Detail + ' drain=' + $drainSec + 's'
    }
    foreach ($r in $rows) {
        $r | Add-Member -NotePropertyName Verdict -NotePropertyValue $v.Verdict -Force
        $r | Add-Member -NotePropertyName Detail -NotePropertyValue $v.Detail -Force
        $r | Add-Member -NotePropertyName LuaCalls -NotePropertyValue $luaCalls -Force
        $r | Add-Member -NotePropertyName LuaUs -NotePropertyValue $luaUsec -Force
        $r | Add-Member -NotePropertyName RedisCore -NotePropertyValue $core -Force
        $all += $r
        Write-Host ('    ' + $r.Conc + ' 并发  ' + $r.Thr + ' req/s  p50=' + $r.P50 +
            ' p95=' + $r.P95 + ' p99=' + $r.P99 + '  fail=' + $r.FailPct + '%  ' + $v.Verdict)
    }
    Write-Host ('    lua=' + $luaUsec + 'us/次  累计 ' + $luaCalls + ' 次  Redis 单核 ' + $core + '%')
}

Write-Host ' '
Write-Host '=== 段 A 结果表（fail% 只算系统失败：err；2001/1005/2002 是业务拒绝，不算）==='
Write-Host 'layer,conc,thr,p50,p95,p99,avg,ok,full,other,err,fail%,failAll%,lua_us,redis_core%,verdict,detail'
$all | ForEach-Object {
    ($_.Layer + ',' + $_.Conc + ',' + $_.Thr + ',' + $_.P50 + ',' + $_.P95 + ',' + $_.P99 + ',' +
        $_.Avg + ',' + $_.Ok + ',' + $_.Full + ',' + $_.Other + ',' + $_.Err + ',' + $_.FailPct + ',' +
        $_.FailAllPct + ',' + $_.LuaUs + ',' + $_.RedisCore + ',' + $_.Verdict + ',' + $_.Detail)
} | Tee-Object -FilePath $csvPath

# ---------------------------------------------------------------------------
# 段 B：正确性层（quota 故意给少，必须耗尽才问得到"会不会超卖"）
# ---------------------------------------------------------------------------
if (-not $SkipCorrectness) {
    Write-Host ' '
    Write-Host ('=== 段 B · 正确性（quota=' + $TightQuota + ' 并发=' + $TightConc +
        ' 窗口=' + $TightSeconds + 's，请求数必然超过名额）===')
    $tight = @()
    foreach ($code in $Layers) {
        $L = Get-Layer $code
        Write-Host ('--- ' + $L.Code + '  ' + $L.Name)
        Reset-Layer $L $TightQuota
        $hammerArgs = @(
            $Base, '0', '0', [string]$L.Id, $L.Path, $L.IdParam,
            ('--sweep=' + $TightConc), ('--seconds=' + $TightSeconds)
        )
        & $java 'benchmark/hammer/BaselineHammer.java' @hammerArgs 2>&1 |
            Tee-Object -FilePath $log -Append | Out-Null
        if ($L.DrainPath) {
            Wait-Drain | Out-Null
        }
        $stat = (CGet ($Base + $L.Stat)) | ConvertFrom-Json
        if ($L.Kind -eq 'baseline') {
            $line = [pscustomobject]@{
                Layer = $L.Code; Quota = [int]$stat.data.totalQuota
                Issued = [int]$stat.data.records; Remain = [int]$stat.data.remain
                Oversold = [int]$stat.data.oversold
                Verdict = if ([int]$stat.data.remain -lt 0) {
                    '超卖 ' + (-[int]$stat.data.remain) + ' 件'
                }
                else { '未超卖（请求数不够，本档无效）' }
            }
        }
        else {
            $line = [pscustomobject]@{
                Layer = $L.Code; Quota = [int]$stat.data.totalQuota
                Issued = [int]$stat.data.issued; Remain = [int]$stat.data.dbRemain
                Oversold = [int]$stat.data.oversold
                Verdict = if ($stat.data.quotaInvariant -and [int]$stat.data.oversold -le 0) {
                    '不变量成立（未超卖）'
                }
                else { '不变量被破坏' }
            }
        }
        $tight += $line
        Write-Host ('    ' + $line.Verdict + '  quota=' + $line.Quota + ' issued=' + $line.Issued +
            ' remain=' + $line.Remain + ' oversold=' + $line.Oversold)
    }

    Write-Host ' '
    Write-Host '=== 段 B 结果 ==='
    $tight | ForEach-Object {
        ($_.Layer + '  quota=' + $_.Quota + '  issued=' + $_.Issued + '  remain=' + $_.Remain +
            '  oversold=' + $_.Oversold + '  ' + $_.Verdict)
    } | Tee-Object -FilePath (Join-Path $logDir 'layered-correctness.csv')
}

# ---------------------------------------------------------------------------
# NFR 判定
# ---------------------------------------------------------------------------
# NFR-1 P99 < 200ms / NFR-2 失败率 < 0.5%。
# 只在段 A（名额没耗尽）上判：名额耗尽后大量 2001 是设计行为，不是可用性问题。
Write-Host ' '
Write-Host '=== NFR 判定（段 A，名额未耗尽）==='
$nfr1 = 0
$nfr2 = 0
foreach ($r in $all) {
    $p99ok = $r.P99 -lt 200
    $failok = $r.FailPct -lt 0.5
    if ($p99ok) { $nfr1++ }
    if ($failok) { $nfr2++ }
    Write-Host ('    ' + $r.Layer + ' @' + $r.Conc + '  P99=' + $r.P99 + 'ms [' +
        $(if ($p99ok) { 'ok' } else { 'HIGH' }) + ']  fail=' + $r.FailPct + '% [' +
        $(if ($failok) { 'ok' } else { 'HIGH' }) + ']')
}
Write-Host ('    NFR-1 达标 ' + $nfr1 + '/' + $all.Count + ' 档    NFR-2 达标 ' + $nfr2 + '/' + $all.Count + ' 档')
Write-Host ' '
Write-Host ('明细: ' + $csvPath)
