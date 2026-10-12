@echo off
chcp 65001 >nul
cd /d "%~dp0"
if not exist .venv py -3.11 -m venv .venv || py -3 -m venv .venv
.venv\Scripts\python -m pip install -r requirements-dev.txt || goto :fail
.venv\Scripts\python -m pytest -q || goto :fail
.venv\Scripts\pyinstaller --noconfirm --clean --onefile --windowed --name TgBroadcast main.py || goto :fail
echo.
echo Готово: dist\TgBroadcast.exe
pause
exit /b 0
:fail
echo Сборка не удалась.
pause
exit /b 1
