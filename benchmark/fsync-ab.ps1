<#
.SYNOPSIS
    消费速度的 fsync 归因 A/B：同一条链路，只改 MySQL 的两个刷盘开关，量消费速率。

.DESCRIPTION
    要回答的问题是「323 条/秒到底是行锁排出来的，还是每条提交各自 fsync 出来的」。
    这两个成因指向完全不同的 D10 方案：

        行锁主导  → 必须去热点（分片、批量扣减），只做批量 INSERT 收益有限
        fsync 主导 → 批量提交就能拿到数量级，三层防线一个字都不用改

    所以不是「顺手优化一下 MySQL 参数」，而是给 D10 定方向。测完立刻改回 1/1。

    两个 arm：
        baseline   innodb_flush_log_at_trx_commit=1  sync_binlog=1   生产默认
        fast        innodb_flush_log_at_trx_commit=2  sync_binlog=0   只摘掉磁盘同步

    为什么必须两个一起改：只把 flush 改成 2 会漏掉 binlog 那一半 fsync
    （实测 2.24 次/条，不是整数 2 —— 多出来的就是 sync_binlog）。只改一个，
    测出来大约只涨 1.5 倍，然后会得出「fsync 不是主因」的错误结论。

    为什么用 800 并发造积压：fast arm 的消费速度可能接近生产速度，100 并发打出的
    几千条会在测量开始前就被吃掉，队列一直是空的，量不到「有积压时的持续速率」。

.USAGE
    powershell -NoProfile -ExecutionPolicy Bypass -File deploy/experiment.ps1 `
        -Work "& benchmark/fsync-ab.ps1"
#>
[CmdletBinding()]
param(
    [string]$Base = 'http://127.0.0.1:8080',
    [string]$ConsumerBase = 'http://127.0.0.1:8082',
    [string]$Container = 'cs-mysql',
    [string]$DbUser = 'root',
    [string]$DbPass = 'campusslot',
    [int]$LoadSeconds = 6,
    [int]$MeasureSec = 20,
    [int]$FullQuota = 500000,
    [int]$Threads = 20
)

$ErrorActionPreference = 'Stop'

$java = (Get-Command java.exe).Source
$logDir = Join-Path (Get-Location) 'logs'
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir | Out-Null }
$log = Join-Path $logDir 'fsync-ab.log'
Set-Content -Path $log -Value '' -Encoding UTF8

function CGet([string]$Url) {
    return ((& curl.exe --noproxy '*' -s --max-time 600 $Url) -join '')
}

function CPost([string]$Url) {
    return ((& curl.exe --noproxy '*' -s --max-time 600 -X POST $Url) -join '')
}

function SqlRaw([string]$Sql) {
    <#
        密码走 MYSQL_PWD 环境变量而不是 -p 参数：
        mysql 客户端看到命令行里有 -p 就会往 stderr 打一行「Using a password ... is insecure」，
        而本脚本是 ErrorActionPreference=Stop，那一行警告会被 PowerShell 包成
        NativeCommandError 直接抛出去 —— 第一次跑就是这么挂在第一句 SQL 上的。
        stderr 另外单独吞掉一次，防的是除密码以外的其他警告（时区、字符集）。
    #>
    $old = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $out = & docker exec -e "MYSQL_PWD=$DbPass" $Container mysql "-u$DbUser" -N -e $Sql 2>&1
    }
    finally {
        $ErrorActionPreference = $old
    }
    $lines = @($out |
        Where-Object { $_ -isnot [System.Management.Automation.ErrorRecord] } |
        ForEach-Object { [string]$_ })
    return ($lines -join "`n")
}

function SqlScalar([string]$Sql) {
    $raw = SqlRaw $Sql
    if (-not $raw) { return $null }
    $line = ($raw -split "`n")[0].Trim()
    return $line
}

function Get-Status([string]$Name) {
    $v = SqlScalar ("SHOW GLOBAL STATUS LIKE '" + $Name + "'")
    # 返回两列：Variable_name \t Value
    return [long](($v -split "`t")[1])
}

function Get-Setting([string]$Name) {
    return [long](SqlScalar ('SELECT @@' + $Name))
}

function Set-FlushMode([int]$Flush, [int]$Sync) {
    SqlRaw ('SET GLOBAL innodb_flush_log_at_trx_commit = ' + $Flush) | Out-Null
    SqlRaw ('SET GLOBAL sync_binlog = ' + $Sync) | Out-Null
    $now = 'flush=' + (Get-Setting 'innodb_flush_log_at_trx_commit') +
        ' sync_binlog=' + (Get-Setting 'sync_binlog')
    Write-Host ('    mysql -> ' + $now)
}

function Get-PersistCounters {
    <#
        从消费端 Prometheus 读落库结果计数。用这个而不是 /progress 的 records，
        原因是 records 走的是全表 COUNT(*)：测量窗口里每秒查 20 次，探针自己就变成
        被测系统的一部分。Prometheus 是内存里的 LongAdder，成本可以忽略。
    #>
    $text = ((& curl.exe --noproxy '*' -s --max-time 5 ($ConsumerBase + '/actuator/prometheus')) -join "`n")
    $map = @{}
    # 标签顺序是 application 在前、result 在后（micrometer 按字母序输出）：
    #   campusslot_claim_persist_total{application="campusslot-consumer",result="ok"} 12.0
    # 所以不能写成 {result=（那是给自己挖的坑，第一次跑就匹配不到，
    # 而报错信息是「没有这个指标」，会让人去怀疑 actuator 没暴露）。
    foreach ($m in [regex]::Matches($text,
            'campusslot_claim_persist_total\{[^}]*result="([a-z]+)"[^}]*\}\s+([0-9]+(?:\.[0-9]+)?)')) {
        $map[$m.Groups[1].Value] = [long][double]$m.Groups[2].Value
    }
    return $map
}

function Get-Progress {
    $raw = CGet ($Base + '/api/claim/dev/progress?resourceId=1')
    return ($raw | ConvertFrom-Json)
}

function Reset-Path {
    # 诊断实验，不走 d5-sweep 那套严格判据：force 复位，目的是让两个 arm 从同一起点出发。
    $resp = CPost ($Base + '/api/claim/dev/reset?resourceId=1&quota=' + $FullQuota + '&force=true')
    if ($resp -notmatch '"code":0') {
        throw ('reset 失败：' + $resp)
    }
    Write-Host ('    reset ok, pendingBeforeReset=' +
        ([regex]::Match($resp, '"pendingBeforeReset":(-?\d+)').Groups[1].Value))
}

function Wait-QueueEmpty([int]$TimeoutSec = 240) {
    $t0 = Get-Date
    while (((Get-Date) - $t0).TotalSeconds -lt $TimeoutSec) {
        $p = Get-Progress
        if ($null -ne $p.data.pending -and [int]$p.data.pending -le 0) { return $true }
        Start-Sleep -Milliseconds 500
    }
    return $false
}

function Show-Metrics {
    Write-Host '--- 消费端指标探测'
    $c = Get-PersistCounters
    if ($c.Count -eq 0) {
        throw ('消费端没有 campusslot_claim_persist_total，无法测速率：' + $ConsumerBase)
    }
    $c.GetEnumerator() | Sort-Object Name | ForEach-Object { Write-Host ('    ' + $_.Name + ' = ' + $_.Value) }
}

function Invoke-Arm {
    param([string]$Name, [int]$Flush, [int]$Sync)

    Write-Host ''
    Write-Host ('##### arm ' + $Name + ' #####')
    Set-FlushMode -Flush $Flush -Sync $Sync
    Reset-Path

    Write-Host ('--- 造积压：800 并发 x ' + $LoadSeconds + 's')
    $hammerArgs = @($Base, '0', '0', '1', '/api/claim/mq', 'resourceId',
        '--sweep=800', ('--seconds=' + $LoadSeconds))
    & $java 'benchmark/hammer/BaselineHammer.java' @hammerArgs 2>&1 |
        Tee-Object -FilePath $log -Append | Out-Null

    $p = Get-Progress
    $peak = [int]$p.data.pending
    Write-Host ('    队列峰值 pending=' + $peak)
    if ($peak -le 1000) {
        Write-Host '!!! 队列没积起来，这一档测不到持续速率（生产 < 消费）'
    }

    # ---- 测量窗口 ----
    # 起止同时取所有指标：必须是同一个时间窗，否则比值毫无意义。
    $fs0 = Get-Status 'Innodb_os_log_fsyncs'
    $lw0 = Get-Status 'Innodb_row_lock_waits'
    $lt0 = Get-Status 'Innodb_row_lock_time'
    $cm0 = Get-Status 'Com_commit'
    $ok0 = (Get-PersistCounters)['ok']
    if ($null -eq $ok0) { $ok0 = 0 }

    $t0 = Get-Date
    $prevOk = $ok0
    $prevT = 0.0
    $rates = New-Object System.Collections.ArrayList
    $samples = 0
    while (((Get-Date) - $t0).TotalSeconds -lt $MeasureSec) {
        Start-Sleep -Milliseconds 400
        $t = ((Get-Date) - $t0).TotalSeconds
        $ok = (Get-PersistCounters)['ok']
        if ($null -eq $ok) { $ok = 0 }
        if ($ok -gt $prevOk) {
            $dt = $t - $prevT
            if ($dt -ge 0.3) {
                [void]$rates.Add(($ok - $prevOk) / $dt)
            }
        }
        $prevOk = $ok
        $prevT = $t
        $samples++
        if (($samples % 5) -eq 0) {
            $pp = Get-Progress
            if ($null -ne $pp.data.pending -and [int]$pp.data.pending -le 200) { break }
        }
    }
    $cost = ((Get-Date) - $t0).TotalSeconds
    $fs1 = Get-Status 'Innodb_os_log_fsyncs'
    $lw1 = Get-Status 'Innodb_row_lock_waits'
    $lt1 = Get-Status 'Innodb_row_lock_time'
    $cm1 = Get-Status 'Com_commit'
    $ok1 = (Get-PersistCounters)['ok']
    if ($null -eq $ok1) { $ok1 = 0 }

    $persisted = $ok1 - $ok0
    $fsyncs = $fs1 - $fs0
    $lockWaits = $lw1 - $lw0
    $lockMs = $lt1 - $lt0
    $commits = $cm1 - $cm0
    $sorted = @($rates | Sort-Object)
    $median = if ($sorted.Count -gt 0) { [math]::Round($sorted[[int]($sorted.Count / 2)], 0) } else { 0 }
    $max = if ($sorted.Count -gt 0) { [math]::Round($sorted[-1], 0) } else { 0 }
    $perRow = if ($persisted -gt 0) { [math]::Round($fsyncs / $persisted, 2) } else { 0 }
    $lockWaitsPerRow = if ($persisted -gt 0) { [math]::Round($lockWaits / $persisted, 2) } else { 0 }
    # 等锁耗时的单位坑：Innodb_row_lock_time 是**所有等待者的等待时长累加**，
    # 20 个线程同时各等 10ms，这里涨 200ms 而墙钟只过 10ms。所以
    #   lockMs / persisted  得到的是「并发累计的等锁时间 / 条」，会大于单条墙钟耗时
    #   （实测 12ms vs 1ms，看起来像 1200%，其实只是量纲不同）
    # 有意义的量是：lockMs / 窗口墙钟 = 窗口内**平均有几个人在等锁**。
    # 这个数直接和消费线程数比：接近线程数 → 线程全在排队，加线程毫无意义。
    $lockConcurrency = if ($cost -gt 0) { [math]::Round($lockMs / ($cost * 1000), 1) } else { 0 }
    $msPerRow = if ($persisted -gt 0) { [math]::Round(1000 * $cost / $persisted, 3) } else { 0 }

    Write-Host ('    测量 ' + [math]::Round($cost, 1) + 's  落库 ' + $persisted + ' 条  吞吐分摊 ' +
        $msPerRow + ' ms/条')
    Write-Host ('    速率中位 ' + $median + ' 条/秒  峰值 ' + $max + ' 条/秒')
    Write-Host ('    Δfsync=' + $fsyncs + ' (' + $perRow + '/条)  Δcommit=' + $commits +
        ' (' + $(if ($persisted -gt 0) { [math]::Round($commits / $persisted, 2) } else { 0 }) + '/条)')
    Write-Host ('    Δrow_lock_waits=' + $lockWaits + ' (' + $lockWaitsPerRow + '/条)  Δrow_lock_time=' +
        $lockMs + 'ms  平均同时等锁 ' + $lockConcurrency + ' / ' + $Threads + ' 线程')

    return [pscustomobject]@{
        Arm         = $Name
        Flush       = $Flush
        Sync        = $Sync
        Peak        = $peak
        Persisted   = $persisted
        Median      = $median
        Max         = $max
        FsyncPerRow = $perRow
        LockConc    = $lockConcurrency
        MsPerRow    = $msPerRow
    }
}

# ---- 主流程 ----
$origFlush = Get-Setting 'innodb_flush_log_at_trx_commit'
$origSync = Get-Setting 'sync_binlog'
Write-Host ('=== fsync A/B 原始设置 flush=' + $origFlush + ' sync_binlog=' + $origSync + ' ===')

$results = @()
try {
    Show-Metrics

    Write-Host ''
    Write-Host '=== 预清理 ==='
    Reset-Path
    if (-not (Wait-QueueEmpty -TimeoutSec 240)) {
        Write-Host '!!! 预清理没排空，第一次测量的起点会偏高'
    }

    $results += Invoke-Arm -Name 'baseline' -Flush 1 -Sync 1
    # 第一个 arm 结束时可能还有积压（测量窗口小于追平时间），先排空再进下一个，
    # 否则第二个 arm 的 pending 起点里混着第一个 arm 的消息。
    Write-Host ''
    Write-Host '--- 排空 baseline 的尾巴'
    if (-not (Wait-QueueEmpty -TimeoutSec 300)) {
        Write-Host '!!! 尾没排空'
    }

    $results += Invoke-Arm -Name 'fast' -Flush 2 -Sync 0
}
finally {
    # 无论成功失败都要改回去：2/0 下崩溃会丢事务，这台机器上还有 D8/D9 要跑。
    Write-Host ''
    Write-Host '=== 恢复 MySQL 参数 ==='
    Set-FlushMode -Flush $origFlush -Sync $origSync
    $chk = 'flush=' + (Get-Setting 'innodb_flush_log_at_trx_commit') +
        ' sync_binlog=' + (Get-Setting 'sync_binlog')
    if ($chk -ne ('flush=' + $origFlush + ' sync_binlog=' + $origSync)) {
        Write-Host ('!!! 恢复失败，当前 ' + $chk + '，手工执行：SET GLOBAL innodb_flush_log_at_trx_commit=' +
            $origFlush + '; SET GLOBAL sync_binlog=' + $origSync + ';')
    }
    else {
        Write-Host ('    已恢复 ' + $chk)
    }
}

Write-Host ''
Write-Host '=== 结果 ==='
$results | Format-Table -AutoSize | Out-String -Width 200 | Write-Host

if ($results.Count -ge 2) {
    $b = $results[0]
    $f = $results[1]
    if ($b.Median -gt 0) {
        $ratio = [math]::Round($f.Median / $b.Median, 1)
        Write-Host ('fast / baseline = ' + $ratio + 'x')
        if ($ratio -ge 3) {
            Write-Host '判定：fsync 是主因。D10 先做批量提交，去热点推迟到量完批量之后。'
        }
        elseif ($ratio -ge 1.5) {
            Write-Host '判定：fsync 与行锁各占一部分。D10 需要批量提交 + 去热点两条一起做。'
        }
        else {
            Write-Host '判定：fsync 不是主因，行锁/热点才是。D10 优先分片或批量扣减。'
        }

        # ---- 第二道墙 ----
        # fast arm = 「fsync 成本归零、其余一字不动」，所以它给的是批量提交的**参照值**，
        # 不是上界：批量提交还会顺带减少锁争用次数（N 条一次 commit，撞锁 1 次而不是 N 次），
        # 真实结果可能高过它。但再高也受锁限制 —— 剩下的时间花在哪里，决定第二步做什么。
        Write-Host ''
        Write-Host ('fast arm 的参照值：' + $f.Median + ' 条/秒（只摘 fsync，逻辑不变）')
        $share = if ($Threads -gt 0) { [math]::Round(100 * $f.LockConc / $Threads, 0) } else { 0 }
        Write-Host ('其中 ' + $share + '% 的消费线程时间在排队等锁（' + $f.LockConc + ' / ' + $Threads + ' 线程）')
        if ($share -ge 40) {
            Write-Host '→ 行锁是第二道墙：批量提交之后还得拆热点，否则加线程和加批量都会撞在这里'
        }
        elseif ($share -ge 15) {
            Write-Host '→ 行锁与 SQL 各占一部分，D10 第二步要同时减热点和减往返'
        }
        else {
            Write-Host '→ 第二道墙不在锁上，在 SQL 与网络往返：批量化 round trip 才是第二步'
        }
    }
}

# 收尾：把队列排干净，别把几万条消息留给下一轮实验
Write-Host ''
Write-Host '--- 收尾排空'
if (-not (Wait-QueueEmpty -TimeoutSec 300)) {
    Write-Host '!!! 收尾没排空，下一轮实验前先手动 drain'
}
Write-Host '=== done ==='
