@echo off
echo Запускаю Sentinel (Docker режим)...
docker compose up -d
timeout /t 3 /nobreak >nul
start http://localhost:8080
echo Готово! Sentinel запущен в Docker.