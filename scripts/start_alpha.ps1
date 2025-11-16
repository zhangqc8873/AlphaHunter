<#
 .SYNOPSIS
  启动 AlphaHunter 的后台实时服务与（可选）Streamlit UI，并保持持续运行。

 .USAGE
  在项目根目录执行：
    powershell -ExecutionPolicy Bypass -File .\scripts\start_alpha.ps1

  可选参数：
    -Port 8503          指定 UI 端口（默认 8503）
    -CacheDir .cache    指定缓存目录（默认 .cache）
    -NoUI               仅启动后台服务，不启动 UI

  日志输出：logs\realtime.out.log / logs\realtime.err.log / logs\ui.out.log / logs\ui.err.log
#>

[CmdletBinding()]
param(
    [int]$Port = 8503,
    [string]$CacheDir = ".cache",
    [switch]$NoUI
)

$ErrorActionPreference = "Stop"

# 项目根目录：脚本位于 scripts/，其父目录即工程根
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$LogDir = Join-Path $ProjectRoot "logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

# 解析 python 可执行文件
$py = (Get-Command python -ErrorAction SilentlyContinue).Source
if (-not $py) { throw "未找到 python 可执行文件，请先将 Python 加入 PATH" }

function Is-Service-Running {
    param([string]$StatusPath)
    if (Test-Path $StatusPath) {
        try {
            $json = Get-Content -Raw -Path $StatusPath | ConvertFrom-Json
            if ($json.running -and $json.pid) {
                $p = Get-Process -Id [int]$json.pid -ErrorAction SilentlyContinue
                if ($p) { return $true }
            }
        } catch { }
    }
    return $false
}

# ===== 启动后台实时服务 =====
$rtStatusPath = Join-Path $ProjectRoot "$CacheDir\realtime\service_status.json"
$rtOut = Join-Path $LogDir "realtime.out.log"
$rtErr = Join-Path $LogDir "realtime.err.log"

if (Is-Service-Running -StatusPath $rtStatusPath) {
    Write-Host "实时服务已在运行中，跳过启动。" -ForegroundColor Green
} else {
    New-Item -ItemType Directory -Force -Path (Join-Path $ProjectRoot "$CacheDir\realtime") | Out-Null
    Write-Host "正在后台启动实时服务..." -ForegroundColor Cyan
    Start-Process -FilePath $py -ArgumentList "-m alphahunter.realtime_service" -WorkingDirectory $ProjectRoot -WindowStyle Hidden -RedirectStandardOutput $rtOut -RedirectStandardError $rtErr
}

if (-not $NoUI) {
    # ===== 启动 Streamlit UI =====
    $uiOut = Join-Path $LogDir "ui.out.log"
    $uiErr = Join-Path $LogDir "ui.err.log"

    $portInUse = $false
    try {
        $conn = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
        if ($conn) { $portInUse = $true }
    } catch { }

    if ($portInUse) {
        Write-Host "Streamlit UI 已在端口 $Port 监听，跳过启动。" -ForegroundColor Green
    } else {
        Write-Host "正在后台启动 Streamlit UI（端口 $Port）..." -ForegroundColor Cyan
        Start-Process -FilePath $py -ArgumentList "-m streamlit run src/alphahunter/ui_app.py --server.port $Port" -WorkingDirectory $ProjectRoot -WindowStyle Hidden -RedirectStandardOutput $uiOut -RedirectStandardError $uiErr
    }
}

Write-Host "完成。日志位于：$LogDir" -ForegroundColor Yellow