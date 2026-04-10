@echo off
setlocal

echo [1/2] Checking PyInstaller...
py -3 -m PyInstaller --version >nul 2>&1
if errorlevel 1 (
  echo PyInstaller is not installed. Install it with:
  echo   py -3 -m pip install pyinstaller
  exit /b 1
)

echo [2/2] Building EXE...
py -3 -m PyInstaller --noconfirm --clean --windowed --onefile --name mediacleaner media_cleaner_gui.py
if errorlevel 1 (
  echo EXE build failed.
  exit /b 1
)

echo Done. Output: dist\mediacleaner.exe
endlocal
