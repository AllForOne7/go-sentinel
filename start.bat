@echo off
chcp 65001 >nul
echo Запускаю Sentinel (режим разработки)...

echo Останавливаю dashboard-контейнер если запущен...
docker compose stop dashboard 2>nul

echo Запускаю NATS...
docker compose up -d nats
echo Жду пока NATS запустится...
timeout /t 5 /nobreak >nul

echo Запускаю processor...
start "Processor" /d "%~dp0" cmd /k go run ./cmd/processor
timeout /t 2 /nobreak >nul

echo Запускаю alerter...
start "Alerter" /d "%~dp0" cmd /k go run ./cmd/alerter
timeout /t 2 /nobreak >nul

echo Запускаю dashboard...
start "Dashboard" /d "%~dp0" cmd /k go run ./cmd/dashboard
timeout /t 2 /nobreak >nul

echo Запускаю агент...
start "Agent" /d "%~dp0" cmd /k go run ./cmd/agent
timeout /t 2 /nobreak >nul

echo Запускаю бота...
start "Bot" /d "%~dp0" cmd /k go run ./cmd/bot
timeout /t 3 /nobreak >nul

echo Открываю дашборд...
start http://localhost:8080
echo Готово! Все компоненты запущены.
