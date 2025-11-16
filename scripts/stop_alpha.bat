@echo off
REM 停止 AlphaHunter 后台服务与 UI，优雅写入 control.json 并释放端口
powershell -ExecutionPolicy Bypass -File "%~dp0stop_alpha.ps1" %*