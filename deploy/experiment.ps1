<#
.SYNOPSIS
    起服务 → 干活 → 收工，全程在**一次调用内**完成。

.DESCRIPTION
    为什么需要它（2026-10-27 实测结论）：

    本机这套执行环境在每次工具调用结束时回收整个进程树。实测证据：
    用 Start-Process 拉起的两个 java 进程，调用返回后再查 —— 端口没了、进程没了、
    日志停在 "Tomcat started on port 8080" 那一行，既没有 shutdown 日志也没有异常，
    属于 TerminateProcess 级的硬杀。

    所以"进程脱离托管后能跨调用存活"这个假设在本环境**不成立**。
    唯一可靠的用法是：把"起服务 + 跑实验 + 收数据 + 停服务"压进同一次调用。
    这样既不依赖跨调用的活进程，也不会留下任何后台任务通知。

    注意：deploy/dev-server.ps1 本身仍然有用 —— 你在**自己的终端**里跑它，
    进程是正常子进程，不会被执行环境回收，可以一直挂着开发用。

用法（仓库根目录）：
    powershell -NoProfile -ExecutionPolicy Bypass -File deploy/experiment.ps1 `
        -Work "curl.exe --noproxy '*' -X POST 'http://127.0.0.1:8080/api/claim/dev/reset?resourceId=1&quota=200'"

    -Work 里是任意 PowerShell 代码（会交给 Invoke-Expression），
    无论成功失败，finally 都会把两个服务停掉。
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$Work,

    [string]$Profile = 'bench',

    [switch]$NoMq,

    [string[]]$AppExtra = @(),

    # 压测跑的是稳态吞吐，DEBUG 日志既不是被测对象、又会自己变成瓶颈
    # （实测消费端 DEBUG 把 drain 压到 500 条/秒）。要看 SQL 就加 -VerboseLogs。
    [switch]$VerboseLogs,

    [int]$WaitSec = 90
)

$ErrorActionPreference = 'Stop'

$devServer = Join-Path $PSScriptRoot 'dev-server.ps1'
if (-not (Test-Path $devServer)) { throw "missing $devServer" }

$exitCode = 0

# 子进程继承环境变量，application.yml 里 com.campusslot 的级别读的就是这一份。
# 先设再 up，否则 up 起来的进程拿到的还是默认值。
if ($VerboseLogs) {
    $env:LOG_LEVEL = 'DEBUG'
}
else {
    $env:LOG_LEVEL = 'INFO'
    Write-Host '  env     LOG_LEVEL=INFO (加 -VerboseLogs 看 SQL)'
}

try {
    Write-Host '=== up ==='
    # up 必须在 try 里面。放在外面的话，up 中途抛异常（jar 过期、端口被占、健康检查超时）
    # 会跳过 finally，进程留在 8080 上；下一次 up 走 SKIP 分支，
    # 你以为跑的是新参数，其实连的还是旧进程 —— 这种错查起来能耗掉一整天。
    & $devServer up -Profile $Profile -NoMq:$NoMq -AppExtra $AppExtra -WaitSec $WaitSec

    Write-Host ''
    Write-Host '=== work ==='
    Invoke-Expression $Work
    Write-Host '=== work done ==='
}
catch {
    $exitCode = 1
    Write-Host ('=== work FAILED: ' + $_.Exception.Message + ' ===')
}
finally {
    # 收工前先 drain，再 down。
    # 直接强杀消费者的话，队列里没处理完的消息会跨轮存活：下一次 up 之后它们才被消费，
    # 而那时账本已经 reset 过，于是 DB 比 Redis 多扣一大截（实测 -27605），
    # 后面每一轮的判据都从"不平"开始。drain 是 best-effort：超时也照常 down，
    # 但至少不会再有"上一轮的 7 万条消息在新一轮里才落库"。
    try {
        & curl.exe --noproxy '*' -s --max-time 130 `
            'http://127.0.0.1:8080/api/claim/dev/drain?resourceId=1&timeoutMs=120000' | Out-Null
    }
    catch {
        Write-Host '  drain skipped (服务已不在)'
    }
    Write-Host ''
    Write-Host '=== down ==='
    & $devServer down
}

exit $exitCode
