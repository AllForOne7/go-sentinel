@echo off
chcp 65001 >nul
echo Останавливаю Sentinel...

echo Останавливаю Go-процессы...
taskkill /f /im dashboard.exe >nul 2>&1
taskkill /f /im alerter.exe >nul 2>&1
taskkill /f /im processor.exe >nul 2>&1
taskkill /f /im agent.exe >nul 2>&1
taskkill /f /im bot.exe >nul 2>&1
taskkill /f /im go.exe >nul 2>&1

echo Останавливаю Docker-контейнеры...
docker compose down

echo Готово.
