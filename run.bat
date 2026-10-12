@echo off
setlocal
cd /d "%~dp0"

set "PY="
where py >nul 2>nul && set "PY=py -3"
if not defined PY (where python >nul 2>nul && set "PY=python")
if not defined PY goto nopython

if not exist ".venv\Scripts\python.exe" (
    echo Creating virtual environment...
    %PY% -m venv .venv
    if errorlevel 1 goto fail
)

".venv\Scripts\python.exe" -c "import telethon" >nul 2>nul
if errorlevel 1 (
    echo Installing dependencies, please wait...
    ".venv\Scripts\python.exe" -m pip install --upgrade pip
    ".venv\Scripts\python.exe" -m pip install -r requirements.txt
    if errorlevel 1 goto fail
    rem Pillow is optional: it only enables photo previews
    ".venv\Scripts\python.exe" -m pip install --only-binary=:all: Pillow
)

start "" ".venv\Scripts\pythonw.exe" main.py
exit /b 0

:nopython
echo Python not found. Install Python 3.11 - 3.13 from https://www.python.org/downloads/
echo and enable "Add python.exe to PATH".
pause
exit /b 1

:fail
echo ERROR: setup failed. Check the messages above and your internet connection.
echo You can delete the .venv folder and run this file again.
pause
exit /b 1
