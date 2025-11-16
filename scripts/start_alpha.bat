@echo off
REM 启动 AlphaHunter 后台服务与（可选）UI 的简易入口
REM 传递所有参数给 PowerShell 脚本
powershell -ExecutionPolicy Bypass -File "%~dp0start_alpha.ps1" %*