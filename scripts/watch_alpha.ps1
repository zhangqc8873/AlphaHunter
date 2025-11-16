<#
 .SYNOPSIS
  后台看门狗：监控 AlphaHunter 实时服务，异常退出时自动重启。

 .USAGE
  在项目根目录执行：
    powershell -ExecutionPolicy Bypass -File .\scripts\watch_alpha.ps1

  可选参数：
    -CacheDir .cache        指定缓存目录（默认 .cache）
    -CheckIntervalSec 5     轮询间隔秒数（默认 5）
#>

[CmdletBinding()]
param(
    [string]$CacheDir = ".cache",
    [int]$CheckIntervalSec = 5
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$LogDir = Join-Path $ProjectRoot "logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$watchLog = Join-Path $LogDir "watchdog.log"

$py = (Get-Command python -ErrorAction SilentlyContinue).Source
if (-not $py) { throw "未找到 python 可执行文件，请先将 Python 加入 PATH" }

$rtDir = Join-Path $ProjectRoot "$CacheDir\realtime"
$statusPath = Join-Path $rtDir "service_status.json"
$pidPath = Join-Path $rtDir "service_pid.txt"
$rtOut = Join-Path $LogDir "realtime.out.log"
$rtErr = Join-Path $LogDir "realtime.err.log"

function Get-Status {
    if (Test-Path $statusPath) {
        try { return Get-Content -Raw -Path $statusPath | ConvertFrom-Json } catch { return $null }
    }
    return $null
}

function Is-Process-Alive {
    param([int]$Pid)
    if ($Pid -gt 0) {
        $p = Get-Process -Id $Pid -ErrorAction SilentlyContinue
        if ($p) { return $true }
    }
    return $false
}

function Start-Realtime-Service {
    "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] watchdog: 启动实时服务" | Add-Content $watchLog -Encoding UTF8
    Start-Process -FilePath $py -ArgumentList "-m alphahunter.realtime_service" -WorkingDirectory $ProjectRoot -WindowStyle Hidden -RedirectStandardOutput $rtOut -RedirectStandardError $rtErr | Out-Null
}

Write-Host "看门狗已启动（每 $CheckIntervalSec 秒检查一次）。Ctrl+C 可退出。" -ForegroundColor Yellow
while ($true) {
    try {
        $st = Get-Status
        if ($st) {
            if ($st.stop_requested) { break }
            if ($st.paused) {
                # 暂停时不重启
            } else {
                $running = $st.running
                $pid = 0
                try { if (Test-Path $pidPath) { $pid = [int](Get-Content -Path $pidPath -ErrorAction SilentlyContinue) } } catch { $pid = 0 }
                if (-not $running -or -not (Is-Process-Alive -Pid $pid)) {
                    Start-Realtime-Service
                }
            }
        } else {
            # 无状态文件也尝试启动一次
            Start-Realtime-Service
        }
    } catch {
        "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] watchdog: 异常 $_" | Add-Content $watchLog -Encoding UTF8
    }
    Start-Sleep -Seconds $CheckIntervalSec
}

Write-Host "检测到 stop_requested，看门狗退出。" -ForegroundColor Green