<#
.SYNOPSIS
    CampusSlot 本地服务启停（脱离"后台任务托管"的启动方式）。

.DESCRIPTION
    为什么需要这个脚本（2026-10-27）：

    slot-app / slot-consumer 是长驻进程，它们没有"完成"这个状态。如果把它们
    交给某种"后台任务"机制托管，机制只能靠"进程退出"这个事件通知结果，于是：
    任务永远 pending，唯一会到来的通知是进程死亡 —— 看起来像"跑很久然后失败"。

    正确的做法是不要托管：
      1. 用 Start-Process 拉起独立进程，输出直接重定向到 logs/*.log（不接管道，
         否则拿到的退出码是管道末端程序的，不是 java 的）；
      2. 把 pid 落到 logs/*.pid，停止时按 pid 精确杀，不用 taskkill//IM 连坐；
      3. 存活靠端口 + 日志标记探测，不靠退出码 —— 一个永不退出的东西，
         本来就不该用退出码来描述它。

用法（在仓库根目录，PowerShell 里执行）：
    powershell -NoProfile -ExecutionPolicy Bypass -File deploy/dev-server.ps1 up
    powershell -NoProfile -ExecutionPolicy Bypass -File deploy/dev-server.ps1 status
    powershell -NoProfile -ExecutionPolicy Bypass -File deploy/dev-server.ps1 down

参数：
    -Profile bench        只作用于 slot-app（默认 bench：关 SQL 日志，压测必须用它）
    -NoMq                 不传 --campusslot.mq.enabled=true，D0/D1 无中间件时用
    -AppExtra @('--x=1')  slot-app 的额外参数（如 --campusslot.claim.college-quota-enabled=true）
    -WaitSec 90           等启动的超时

注意：脚本自身**立刻返回**，java 进程独立于本 shell 存活。
      所以它不会产生"后台任务"，也不会在会话结束时被连带回收。

启动方式只有两种，**不要混用**（2026-11-04）：
  ① 用本脚本：日志自动重定向到 logs/{app,consumer}.log，pid 自动落盘。
  ② 用工具托管的 run_in_background 直接跑 jar：此时**必须显式重定向**
     `java -jar ... >> logs/app.log 2>&1`，否则日志只存在于会话里，
     D13 要引用"1/2/4/8/16/32 退避序列"这种证据时只能重跑一遍 —— 已经踩过一次。
  两种方式拉起的进程，pid 文件都可能是 stale 的；`down` 已经改成
  "pid 不在就按端口取 owner"，所以停不干净时先看端口，别只看 pid 文件。
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('up', 'down', 'restart', 'status', 'log')]
    [string]$Command = 'status',

    [string]$Profile = 'bench',

    [switch]$NoMq,

    [string[]]$AppExtra = @(),

    [string[]]$ConsumerExtra = @(),

    [int]$WaitSec = 90
)

$ErrorActionPreference = 'Stop'

$Root = Split-Path -Parent $PSScriptRoot
$LogDir = Join-Path $Root 'logs'
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }

$Services = @(
    [pscustomobject]@{
        Name   = 'app'
        Jar    = 'slot-app/target/slot-app.jar'
        Port   = 8080
        Marker = 'Tomcat started on port'
    },
    [pscustomobject]@{
        Name   = 'consumer'
        Jar    = 'slot-consumer/target/slot-consumer.jar'
        Port   = 8082
        Marker = 'Started ConsumerApplication'
    }
)

function Get-JavaPath {
    $cmd = Get-Command java.exe -ErrorAction SilentlyContinue
    if (-not $cmd) { throw 'java.exe not found in PATH' }
    return $cmd.Source
}

function Get-PidFile([string]$Name) { Join-Path $LogDir ($Name + '.pid') }

function Get-LogFile([string]$Name) { Join-Path $LogDir ($Name + '.log') }

function Test-PortBusy([int]$Port) {
    $conn = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
    return [bool]$conn
}

function Get-PortOwner([int]$Port) {
    $conn = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue |
            Select-Object -First 1
    if ($conn) { return [int]$conn.OwningProcess }
    return 0
}

function Get-TrackedPid($Svc) {
    $file = Get-PidFile $Svc.Name
    if (-not (Test-Path $file)) { return 0 }
    $raw = Get-Content $file -ErrorAction SilentlyContinue | Select-Object -First 1
    if (-not $raw) { return 0 }
    return [int]$raw
}

# 日志里出现启动完成标记才算真的起来了。只看端口是骗人的：
# Tomcat 先绑端口、再初始化 Spring 上下文，中间那段窗口端口是通的但接口全是 503。
function Wait-Healthy($Svc, [int]$TimeoutSec) {
    $log = Get-LogFile $Svc.Name
    $tracked = Get-TrackedPid $Svc
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        if (Test-Path $log) {
            $hit = Select-String -Path $log -Pattern $Svc.Marker -SimpleMatch -Quiet -ErrorAction SilentlyContinue
            if ($hit) { return $true }
        }
        if ($tracked -gt 0 -and -not (Get-Process -Id $tracked -ErrorAction SilentlyContinue)) {
            return $false
        }
        Start-Sleep -Milliseconds 500
    }
    return $false
}

# PowerShell 5.1 的 Start-Process 会把当前进程环境变量灌进一个大小写不敏感的字典。
# 如果环境里同时存在 HTTPS_PROXY 和 https_proxy（Git Bash / 全局代理软件经常这么干），
# 灌进去的瞬间就抛：
#   Start-Process : 已添加项。字典中的关键字:"HTTPS_PROXY"所添加的关键字:"https_proxy"
# 这个错看起来像脚本写错了，其实是环境重复。子进程（本地 java 服务）不需要代理，
# 所以直接把小写那一份摘掉。用 -ceq 做大小写敏感比较，避免误删唯一的那一份。
function Remove-ProxyEnvDuplicates {
    $keys = @([System.Environment]::GetEnvironmentVariables().Keys)
    foreach ($name in @('http_proxy', 'https_proxy', 'no_proxy', 'all_proxy')) {
        $upper = $name.ToUpperInvariant()
        $hasUpper = @($keys | Where-Object { $_ -ceq $upper }).Count -gt 0
        $hasLower = @($keys | Where-Object { $_ -ceq $name }).Count -gt 0
        if ($hasUpper -and $hasLower) {
            Remove-Item ('Env:' + $name) -ErrorAction SilentlyContinue
            Write-Host ("  env     dropped duplicate " + $name)
        }
    }
}

function Start-DevService($Svc, [string[]]$Extra, [string]$Java) {
    Remove-ProxyEnvDuplicates
    if (Test-PortBusy $Svc.Port) {
        $owner = Get-PortOwner $Svc.Port
        Write-Host ("  {0,-9} SKIP  port {1} already taken by pid {2}" -f $Svc.Name, $Svc.Port, $owner)
        return
    }

    $jarPath = Join-Path $Root $Svc.Jar
    if (-not (Test-Path $jarPath)) {
        throw ("missing {0} -- run: mvn -o clean package -DskipTests" -f $Svc.Jar)
    }

    $out = Get-LogFile $Svc.Name
    $err = Join-Path $LogDir ($Svc.Name + '.err.log')

    # 每次启动前清空日志。两个原因，都不是洁癖：
    #   ① Start-Process 的重定向是**追加**模式，一轮 D5 的 DEBUG 日志能写到 100+MB，
    #      写盘本身会变成消费者的瓶颈（实测单条消息 6 行 SQL，drain 卡在 500 条/秒）；
    #   ② Wait-Healthy 是「在日志里找启动标记」。不清空的话，上一次运行留下的
    #      "Tomcat started on port 8080" 会让这一次**立刻**判定 READY，
    #      而那时 Spring 上下文还没初始化完，第一批请求全是 503。
    #
    # 但"清空"不等于"丢掉"（D7）：复盘 12,717 那笔账时，正需要看"上一轮跑之前
    # 日志长什么样"，而 Set-Content 已经把文件抹平了，只能从 hammer 的 CSV 反推。
    # 所以先归档再清空 —— 归档是移动，不复制，不影响启动耗时。
    $archiveDir = Join-Path $LogDir 'archive'
    $stamp = Get-Date -Format 'MMdd-HHmmss'
    foreach ($f in @($out, $err)) {
        if ((Test-Path $f) -and ((Get-Item $f).Length -gt 0)) {
            if (-not (Test-Path $archiveDir)) {
                New-Item -ItemType Directory -Path $archiveDir | Out-Null
            }
            $archived = Join-Path $archiveDir (([IO.Path]::GetFileNameWithoutExtension($f)) + "-$stamp.log")
            Move-Item -Path $f -Destination $archived -Force
        }
    }
    Set-Content -Path $out -Value '' -Encoding UTF8
    Set-Content -Path $err -Value '' -Encoding UTF8

    # 输出重定向到文件，绝不接管道：接管道时 $LASTEXITCODE 拿到的是管道末端的，不是 java 的。
    $argList = @('-jar', $jarPath, ('--server.port=' + $Svc.Port)) + $Extra
    $proc = Start-Process -FilePath $Java `
                          -ArgumentList $argList `
                          -WorkingDirectory $Root `
                          -RedirectStandardOutput $out `
                          -RedirectStandardError $err `
                          -PassThru -WindowStyle Hidden
    Set-Content -Path (Get-PidFile $Svc.Name) -Value $proc.Id
    Write-Host ("  {0,-9} START pid {1}  log {2}" -f $Svc.Name, $proc.Id, $out)
}

function Stop-DevService($Svc) {
    $tracked = Get-TrackedPid $Svc
    if ($tracked -eq 0 -and (Test-PortBusy $Svc.Port)) {
        $tracked = Get-PortOwner $Svc.Port
    }
    if ($tracked -eq 0) {
        Write-Host ("  {0,-9} ----  not running" -f $Svc.Name)
        Remove-Item (Get-PidFile $Svc.Name) -ErrorAction SilentlyContinue
        return
    }

    $p = Get-Process -Id $tracked -ErrorAction SilentlyContinue
    if (-not $p) {
        # pid 文件里的进程已经不在了 —— 这是常态，不是异常：
        # 进程被外部回收、或上一次 java 自己崩掉之后，pid 文件就是一条死记录。
        # 关键在下一步：端口可能已经被**另一个** pid 占着（重启后的新进程没写 pid 文件、
        # 或者是被工具托管拉起的那个）。只在 pid 文件上做判断，就会打印
        # "stale pid file, cleaned" 然后什么也没停 —— 端口还在，下一次 up 会 SKIP。
        # 所以这里必须按端口再取一次 owner。
        if (Test-PortBusy $Svc.Port) {
            $real = Get-PortOwner $Svc.Port
            if ($real -gt 0 -and $real -ne $tracked) {
                Write-Host ("  {0,-9} stale pid {1}, port {2} held by pid {3} -- stopping it" -f $Svc.Name, $tracked, $Svc.Port, $real)
                Stop-Process -Id $real -Force -ErrorAction SilentlyContinue
                for ($i = 0; $i -lt 50; $i++) {
                    if (-not (Test-PortBusy $Svc.Port)) { break }
                    Start-Sleep -Milliseconds 200
                }
            }
        }
        else {
            Write-Host ("  {0,-9} stale pid file ({1}), cleaned" -f $Svc.Name, $tracked)
        }
    }
    else {
        Stop-Process -Id $tracked -Force
        for ($i = 0; $i -lt 50; $i++) {
            if (-not (Test-PortBusy $Svc.Port)) { break }
            Start-Sleep -Milliseconds 200
        }
        Write-Host ("  {0,-9} STOP  pid {1}" -f $Svc.Name, $tracked)
    }
    Remove-Item (Get-PidFile $Svc.Name) -ErrorAction SilentlyContinue
}

function Show-Status {
    Write-Host "CampusSlot local services"
    foreach ($svc in $Services) {
        $tracked = Get-TrackedPid $svc
        $alive = $false
        if ($tracked -gt 0) { $alive = [bool](Get-Process -Id $tracked -ErrorAction SilentlyContinue) }
        $busy = Test-PortBusy $svc.Port
        if (-not $alive -and $busy) { $tracked = Get-PortOwner $svc.Port; $alive = $true }

        if ($alive -and $busy) { $state = 'UP' }
        elseif ($busy) { $state = 'PORT-ONLY' }
        else { $state = 'DOWN' }

        $pidText = if ($tracked -gt 0) { $tracked } else { '-' }
        Write-Host ("  {0,-9} {1,-10} port {2}  pid {3}" -f $svc.Name, $state, $svc.Port, $pidText)
    }
}

function Show-Log {
    foreach ($svc in $Services) {
        $log = Get-LogFile $svc.Name
        Write-Host ("--- {0} ({1}) ---" -f $svc.Name, $log)
        if (Test-Path $log) {
            Get-Content $log -Tail 12
        }
        else {
            Write-Host "  (no log yet)"
        }
    }
}

$java = Get-JavaPath

switch ($Command) {
    'up' {
        $appArgs = @()
        if ($Profile) { $appArgs += ('--spring.profiles.active=' + $Profile) }
        if (-not $NoMq) {
            $appArgs += '--campusslot.mq.enabled=true'
        }
        $appArgs += $AppExtra

        $consumerArgs = @()
        if (-not $NoMq) {
            $consumerArgs += '--campusslot.mq.enabled=true'
        }
        $consumerArgs += $ConsumerExtra

        Start-DevService $Services[0] $appArgs $java
        Start-DevService $Services[1] $consumerArgs $java

        foreach ($svc in $Services) {
            if ((Get-TrackedPid $svc) -eq 0) { continue }
            if (Wait-Healthy $svc $WaitSec) {
                Write-Host ("  {0,-9} READY" -f $svc.Name)
            }
            else {
                Write-Host ("  {0,-9} NOT READY -- check {1}" -f $svc.Name, (Get-LogFile $svc.Name))
            }
        }
        Show-Status
    }
    'down' {
        foreach ($svc in $Services) { Stop-DevService $svc }
        Show-Status
    }
    'restart' {
        foreach ($svc in $Services) { Stop-DevService $svc }
        Write-Host 'restarting...'
        & $PSCommandPath up -Profile $Profile -NoMq:$NoMq -AppExtra $AppExtra -ConsumerExtra $ConsumerExtra -WaitSec $WaitSec
    }
    'status' { Show-Status }
    'log' { Show-Log }
}
