<#
 .SYNOPSIS
  优雅停止 AlphaHunter：写入 control.json 请求停止后台实时服务，并释放 UI 端口。

 .USAGE
  在项目根目录执行：
    powershell -ExecutionPolicy Bypass -File .\scripts\stop_alpha.ps1

  可选参数：
    -Port 8503          指定 UI 端口（默认 8503）
    -CacheDir .cache    指定缓存目录（默认 .cache）
    -TimeoutSec 30      等待优雅停止的最长秒数（默认 30）
    -Force              超时后强制结束进程
#>

[CmdletBinding()]
param(
    [int]$Port = 8503,
    [string]$CacheDir = ".cache",
    [int]$TimeoutSec = 30,
    [switch]$Force
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot

# 文件路径
$rtDir = Join-Path $ProjectRoot "$CacheDir\realtime"
$controlPath = Join-Path $rtDir "control.json"
$statusPath = Join-Path $rtDir "service_status.json"
$pidPath = Join-Path $rtDir "service_pid.txt"

# 写入停止请求
New-Item -ItemType Directory -Force -Path $rtDir | Out-Null
$ctl = @{ stop = $true; paused = $false }
$ctl | ConvertTo-Json | Set-Content -Path $controlPath -Encoding UTF8
Write-Host "已写入停止请求：$controlPath" -ForegroundColor Cyan

function Get-Status {
    if (Test-Path $statusPath) {
        try { return Get-Content -Raw -Path $statusPath | ConvertFrom-Json } catch { return $null }
    }
    return $null
}

function Try-Stop-Process {
    param([int]$Pid)
    if ($Pid -gt 0) {
        $p = Get-Process -Id $Pid -ErrorAction SilentlyContinue
        if ($p) { Stop-Process -Id $Pid -Force -ErrorAction SilentlyContinue }
    }
}

# 等待实时服务优雅退出
$elapsed = 0
while ($elapsed -lt $TimeoutSec) {
    $st = Get-Status
    $running = ($st -and $st.running)
    $stopRequested = ($st -and $st.stop_requested)
    if (-not $running) { break }
    Start-Sleep -Seconds 1
    $elapsed += 1
}

if ($elapsed -ge $TimeoutSec -and $Force) {
    Write-Host "优雅停止超时，尝试强制结束后台服务。" -ForegroundColor Yellow
    if (Test-Path $pidPath) {
        try {
            $pid = [int](Get-Content -Path $pidPath -ErrorAction SilentlyContinue)
            Try-Stop-Process -Pid $pid
        } catch { }
    }
}

# 释放 UI 端口（停止 Streamlit）
try {
    $conn = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
    if ($conn) {
        foreach ($c in $conn) {
            $pid = $c.OwningProcess
            # 尝试识别是否为 streamlit/python 进程；无论如何都停止占用端口的进程
            Try-Stop-Process -Pid $pid
        }
        Write-Host "已释放端口 $Port 的监听进程。" -ForegroundColor Green
    } else {
        Write-Host "端口 $Port 未被占用，无需释放。" -ForegroundColor Green
    }
} catch {
    Write-Host "释放端口失败：$_" -ForegroundColor Red
}

Write-Host "停止流程完成。" -ForegroundColor Yellow