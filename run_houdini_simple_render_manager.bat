@echo off
setlocal

cd /d "%~dp0"

if exist ".\.venv\Scripts\python.exe" (
    ".\.venv\Scripts\python.exe" ".\houdini_simple_render_manager.py"
    goto :end
)

where py >nul 2>nul
if %ERRORLEVEL%==0 (
    py -3 ".\houdini_simple_render_manager.py"
    goto :end
)

where python >nul 2>nul
if %ERRORLEVEL%==0 (
    python ".\houdini_simple_render_manager.py"
    goto :end
)

echo Python was not found.
echo Install Python 3.11+ and PySide6, then try again.
pause

:end
endlocal
