<#
.SYNOPSIS
    D5 补齐：开环加压（固定 QPS），测可用性边界（NFR-2 / NFR-6）。

.DESCRIPTION
    闭合环（d5-sweep.ps1 / layered.ps1）的 QPS 是<b>结果</b>：客户端发一个、等回来、再发下一个，
    服务端一慢就自然少发。它的失败率永远接近 0，看上去很稳 —— 但那只是压力没真加上去。

    开环的 QPS 是<b>输入</b>：节拍器按固定速率投递，不管上一个回没回来。
    服务端跟不上时，表现为排队（RT 涨）、超时（err）、队列满（rej）。
    只有它能回答两个问题：

        NFR-2  失败率 < 0.5%  —— 超载时还剩下多少成功率
        NFR-6  8000 QPS        —— 本机能灌到多少，灌不进去时是什么形状

    两组实验：
        ping  /api/dev/ping     客户端天花板。它决定"8000 QPS 是不是压测器自己做不到"
        mq    /api/claim/mq     业务链路，从容量的 1/4 灌到接近上限

    判据不是"吞吐够不够"，而是三件事一起看：
        act / qps   节拍器有没有跟上（< 90% 说明客户端或服务端在反压）
        thr / act   完成速率 / 发起速率（< 100% 就是在堆积）
        fail%       系统失败率（err / 完成数），NFR-2 的口径

    注意 {@code rej} 不是压测器缺陷，它是"服务端处理不过来"的直接证据：
    线程池里的请求还没返回、队列也满了，压力就只能被丢掉。闭合环里这个信号永远看不到。

    必须由 experiment.ps1 调用：
        powershell -NoProfile -ExecutionPolicy Bypass -File deploy/experiment.ps1 `
            -Work "& benchmark/open-loop.ps1"

.PARAMETER Seconds
    每档时长。开环下样本量 = qps × seconds，5 秒在 8000 QPS 时有 4 万个样本，
    分位数足够稳。调大只会让高 QPS 档的消息堆得更多、drain 更久。

.PARAMETER ClaimQps
    业务链路的档位。上限（闭合环实测 3868 req/s）附近要留两档：
    只有越过上限，失败率才会从 0 变成正数 —— 那个点就是可用性边界。
#>
[CmdletBinding()]
param(
    [string]$Base = 'http://127.0.0.1:8080',
    [string]$PingQps = '2000,4000,8000,16000',
    # 默认值必须是「实际会用来出结论的那一组」，否则默认跑永远到不了边界：
    # 旧默认 1000,2000,3000,4000 最高只到 4000，而 rej 第一次出现在 8000 档 ——
    # 用它跑复现不出可用性边界，还会让人误以为 NFR-2 在任何速率下都达标。
    # 500/1000 是低载基准（NFR-1 的 P99 < 200ms 只在低载段成立），
    # 6000 是最后一个 rej=0 的档，8000 是第一个 rej>0 的档 —— 边界就在这两档之间。
    [string]$ClaimQps = '500,1000,2000,4000,6000,8000',
    [int]$Seconds = 5,
    [int]$PingThreads = 1024,
    [int]$ClaimThreads = 768,
    [int]$Quota = 500000,
    # 档间不 drain、也不指望 reset 成功：quota 给 50 万，六档加起来才发 12.75 万条，
    # 累加消耗不会耗尽名额。省下来的时间很关键 —— 每档 drain 要等消费侧按 323 条/秒追平，
    # 8000 档一次就要 2 分钟，六档串起来会撞上工具调用的时限。
    # 只有在"每档必须从干净账本开始"时（比如验超卖）才需要关掉它。
    [switch]$SkipStepDrain,
    [string[]]$Groups = @('ping', 'mq')
)

$ErrorActionPreference = 'Stop'

$java = (Get-Command java.exe).Source
$logDir = Join-Path (Get-Location) 'logs'
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }
$log = Join-Path $logDir 'open-loop.log'
Set-Content -Path $log -Value '' -Encoding UTF8
$csvPath = Join-Path $logDir 'open-loop.csv'

function CGet([string]$Url) {
    return ((& curl.exe --noproxy '*' -s --max-time 600 $Url) -join '')
}

# 队列满被判成 rej 时，说明"已经投出去但没处理完"的请求超过了「线程数 + 队列容量」。
# 队列容量是 max(2048, qps*2)，固定 300 秒足够——真到这一步，说明不是慢，是堵死了。
function Wait-Drain([int]$TimeoutSec = 600) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    $pending = $null
    while ((Get-Date) -lt $deadline) {
        $raw = CGet ($Base + '/api/claim/dev/progress?resourceId=1')
        try { $obj = $raw | ConvertFrom-Json } catch { Start-Sleep -Milliseconds 300; continue }
        $pending = $obj.data.pending
        if ($null -ne $pending -and [int]$pending -le 0) { return $true }
        Start-Sleep -Milliseconds 300
    }
    Write-Host ('    !!! drain 超时 pending=' + $pending)
    return $false
}

# hammer 的列：qps,act,thr,thrReal,finMs,p50,p95,p99,avg,ok,full,other,err,limited,rej,inflight,fail%,failAll%
# act/thr/thrReal 是每秒速率，ok..rej 是本档总数；thrReal 与 finMs 可能是 -1（60 秒没排空）。
function ConvertFrom-OpenRows($out) {
    $rows = @()
    foreach ($line in $out) {
        $m = [regex]::Match([string]$line,
            '^(\d+),(\d+),(\d+),(-?\d+),(-?\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),([0-9.]+),([0-9.]+)')
        if (-not $m.Success) { continue }
        $rows += [pscustomobject]@{
            Target = [int]$m.Groups[1].Value
            Act = [int]$m.Groups[2].Value
            Thr = [int]$m.Groups[3].Value
            ThrReal = [int]$m.Groups[4].Value
            FinMs = [int]$m.Groups[5].Value
            P50 = [int]$m.Groups[6].Value
            P95 = [int]$m.Groups[7].Value
            P99 = [int]$m.Groups[8].Value
            Avg = [int]$m.Groups[9].Value
            Ok = [int]$m.Groups[10].Value
            Full = [int]$m.Groups[11].Value
            Other = [int]$m.Groups[12].Value
            Err = [int]$m.Groups[13].Value
            Limited = [int]$m.Groups[14].Value
            Rej = [int]$m.Groups[15].Value
            Inflight = [int]$m.Groups[16].Value
            FailPct = [double]$m.Groups[17].Value
            FailAllPct = [double]$m.Groups[18].Value
        }
    }
    return $rows
}

$results = @()

foreach ($g in $Groups) {
    Write-Host ' '
    if ($g -eq 'ping') {
        Write-Host ('=== 开环 · 客户端天花板  qps=' + $PingQps + ' seconds=' + $Seconds +
            ' threads=' + $PingThreads + ' ===')
        $target = $PingQps
        $path = '/api/dev/ping'
        $idParam = 'none'
        $threads = $PingThreads
        $extra = @()
    }
    else {
        Write-Host ('=== 开环 · 业务链路  qps=' + $ClaimQps + ' seconds=' + $Seconds +
            ' threads=' + $ClaimThreads + ' quota=' + $Quota + ' ===')
        $target = $ClaimQps
        $path = '/api/claim/mq'
        $idParam = 'resourceId'
        $threads = $ClaimThreads
        $extra = @('--reset=/api/claim/dev/reset?resourceId=1&quota=' + $Quota)
        if (-not $SkipStepDrain) {
            # 600000ms 而不是默认 10s：drain 是长轮询，超时会静默返回空串，
            # 既判不出"追平没追平"，还会让下一档的 reset 被守卫拒绝。
            $extra += '--drain=/api/claim/dev/drain?resourceId=1&timeoutMs=600000'
        }
    }

    $hammerArgs = @($Base, '0', '0', '1', $path, $idParam,
        ('--qps=' + $target), ('--seconds=' + $Seconds), ('--threads=' + $threads)) + $extra
    $out = & $java 'benchmark/hammer/BaselineHammer.java' @hammerArgs 2>&1
    $out | Tee-Object -FilePath $log -Append | Out-Null

    $rows = ConvertFrom-OpenRows $out
    if ($rows.Count -eq 0) { throw ('压测器没有输出可解析的开环行（' + $g + '）') }
    foreach ($r in $rows) {
        $r | Add-Member -NotePropertyName Group -NotePropertyValue $g -Force
        $results += $r
        $ratio = if ($r.Target -gt 0) { [math]::Round(100.0 * $r.Act / $r.Target, 0) } else { 0 }
        # thrReal 是 -1 时表示 60 秒内没排空，分母未知，如实写 n/a 而不是编一个数
        $realText = if ($r.FinMs -gt 0) { [string]$r.ThrReal } else { 'n/a' }
        Write-Host ('    目标 ' + $r.Target + ' req/s → 发起 ' + $r.Act + ' (' + $ratio +
            '%)  窗口内完成 ' + $r.Thr + '  真实完成 ' + $realText + ' (finMs=' + $r.FinMs +
            ')  p99=' + $r.P99 + 'ms  fail=' + $r.FailPct + '%  failAll=' + $r.FailAllPct +
            '%  rej=' + $r.Rej + '  inflight=' + $r.Inflight)
    }

    if ($g -ne 'ping') {
        $drained = Wait-Drain
        if (-not $drained) {
            Write-Host '    !!! 队列未追平，这一组的账目不可用（吞吐与失败率仍然有效）'
        }
    }
}

Write-Host ' '
Write-Host '=== 开环结果表 ==='
Write-Host 'group,qps_target,qps_actual,thr,thrReal,finMs,p50,p95,p99,avg,ok,full,other,err,fail%,failAll%,rej,inflight'
$results | ForEach-Object {
    ($_.Group + ',' + $_.Target + ',' + $_.Act + ',' + $_.Thr + ',' + $_.ThrReal + ',' + $_.FinMs + ',' +
        $_.P50 + ',' + $_.P95 + ',' + $_.P99 + ',' + $_.Avg + ',' + $_.Ok + ',' + $_.Full + ',' +
        $_.Other + ',' + $_.Err + ',' + $_.FailPct + ',' + $_.FailAllPct + ',' + $_.Rej + ',' + $_.Inflight)
} | Tee-Object -FilePath $csvPath

# ---------------------------------------------------------------------------
# 判定
# ---------------------------------------------------------------------------
Write-Host ' '
Write-Host '=== 判定 ==='

$pingRows = @($results | Where-Object { $_.Group -eq 'ping' })
if ($pingRows.Count -gt 0) {
    $best = $pingRows | Sort-Object -Property Act -Descending | Select-Object -First 1
    Write-Host ('客户端天花板：' + $best.Act + ' req/s（目标 ' + $best.Target + '）')
    # 判「跟不上」用 90% 而不是 100%：节拍器的补投会让实际速率在目标值上下 1~2% 抖动，
    # 那是测量精度，不是能力不足。真跟不上时差距是数量级的（队列一满就丢一大截）。
    if ($best.Act -lt $best.Target * 0.9) {
        Write-Host '  节拍器在最高档已经跟不上 —— 8000 QPS 这一格的失败率不能归咎于服务端，'
        Write-Host '  必须先扣掉客户端自己的上限，否则结论会反。'
    }
}

$mqRows = @($results | Where-Object { $_.Group -eq 'mq' })
if ($mqRows.Count -gt 0) {
    $edge = $null
    foreach ($r in $mqRows) {
        # 边界按「含客户端丢弃」判：只按 fail% 判的话，rej 爆了也永远是"未越界"
        if ($r.FailAllPct -ge 0.5 -or $r.Rej -gt 0) { $edge = $r; break }
    }
    if ($null -eq $edge) {
        Write-Host '业务链路：含客户端丢弃的总失败率全程 < 0.5%，可用性边界在最高档之上（还没灌到）'
    }
    else {
        $prev = @($mqRows | Where-Object { $_.Target -lt $edge.Target } | Sort-Object -Property Target)
        $prevText = if ($prev.Count -gt 0) { [string]$prev[-1].Target } else { '(最低档)' }
        Write-Host ('业务链路：可用性边界在 ' + $prevText + '~' + $edge.Target + ' req/s 之间 —— ' +
            $edge.Target + ' 档 failAll=' + $edge.FailAllPct + '%，rej=' + $edge.Rej)
        Write-Host '  该档 fail% 仍是 0（服务端一条没错），失败全在客户端丢弃 —— 两个数必须一起报，'
        Write-Host '  只报前者会得出「NFR-2 达标」，只报后者会让人以为是服务端扛不住。'
    }

    # NFR-2 拆两行：同一批数据两种口径，结论相反，合成一个"达标"就是自欺
    $nfr2a = @($mqRows | Where-Object { $_.FailPct -lt 0.5 })
    $nfr2b = @($mqRows | Where-Object { $_.FailAllPct -lt 0.5 })
    Write-Host ('NFR-2a（已发出请求的失败率 < 0.5%）：' + $nfr2a.Count + '/' + $mqRows.Count + ' 档达标')
    Write-Host ('NFR-2b（含客户端丢弃的总失败率 < 0.5%）：' + $nfr2b.Count + '/' + $mqRows.Count + ' 档达标')

    # thr 与 thrReal 差超 10% 就是在虚报：分子含窗口外才回来的请求，分母只有窗口时长
    $skewed = @($mqRows | Where-Object {
            $_.FinMs -gt 0 -and $_.Thr -gt 0 -and ($_.Thr - $_.ThrReal) -gt ($_.Thr * 0.1)
        })
    if ($skewed.Count -gt 0) {
        $worst = @($skewed | Sort-Object -Property Target)[-1]
        Write-Host ('  !!! ' + $worst.Target + ' 档 thr=' + $worst.Thr + ' 而 thrReal=' + $worst.ThrReal +
            '（finMs=' + $worst.FinMs + '）—— 有请求在窗口结束后才回来，')
        Write-Host '      thr 是注入速率不是吞吐。这一档的"收得下"要按 thrReal 说。'
    }
}
Write-Host ' '
Write-Host ('明细: ' + $csvPath)
