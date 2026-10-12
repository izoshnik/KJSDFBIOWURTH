@echo off
chcp 65001 >nul
cd /d "%~dp0"
if not exist .venv (
    echo Создание виртуального окружения...
    py -3.11 -m venv .venv || py -3 -m venv .venv || goto :nopython
    .venv\Scripts\python -m pip install --upgrade pip
    .venv\Scripts\python -m pip install -r requirements.txt || goto :fail
)
start "" .venv\Scripts\pythonw main.py
exit /b 0
:nopython
echo Не найден Python 3.11+. Установите его с https://www.python.org/downloads/ (отметьте "Add to PATH").
pause
exit /b 1
:fail
echo Не удалось установить зависимости. Проверьте подключение к интернету.
pause
exit /b 1
