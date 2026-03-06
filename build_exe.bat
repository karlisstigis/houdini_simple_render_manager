@echo off
setlocal

set "ROOT_DIR=%~dp0"
cd /d "%ROOT_DIR%"

set "PYTHON_EXE=%ROOT_DIR%\.venv\Scripts\python.exe"
if not exist "%PYTHON_EXE%" (
    set "PYTHON_EXE=python"
)

set "PYI_CMD=%PYTHON_EXE% -m PyInstaller"
set "PYI_COMMON=--noconfirm --clean --distpath dist --workpath build\\pyinstaller --specpath build\\spec"
set "APP_ICON=%ROOT_DIR%assets\\app_icon.ico"

echo [1/5] Cleaning previous build outputs...
if exist "%ROOT_DIR%build" rmdir /s /q "%ROOT_DIR%build"
if exist "%ROOT_DIR%dist" rmdir /s /q "%ROOT_DIR%dist"

echo [2/5] Building main application...
%PYI_CMD% ^
    %PYI_COMMON% ^
    --onedir ^
    --windowed ^
    --name HoudiniSimpleRenderManager ^
    --icon "%APP_ICON%" ^
    --hidden-import PySide6.QtSvg ^
    --add-data "%ROOT_DIR%houdini_scripts;houdini_scripts" ^
    --add-data "%ROOT_DIR%assets;assets" ^
    houdini_simple_render_manager.py
if errorlevel 1 goto :error

echo [3/5] Building scan worker executable...
%PYI_CMD% ^
    %PYI_COMMON% ^
    --onefile ^
    --console ^
    --name scan_worker ^
    scan_worker.py
if errorlevel 1 goto :error

echo [4/5] Building render worker executable...
%PYI_CMD% ^
    %PYI_COMMON% ^
    --onefile ^
    --console ^
    --name render_worker ^
    render_worker.py
if errorlevel 1 goto :error

echo [5/5] Copying worker executables into app bundle...
copy /y "%ROOT_DIR%dist\scan_worker.exe" "%ROOT_DIR%dist\HoudiniSimpleRenderManager\scan_worker.exe" >nul
if errorlevel 1 goto :error
copy /y "%ROOT_DIR%dist\render_worker.exe" "%ROOT_DIR%dist\HoudiniSimpleRenderManager\render_worker.exe" >nul
if errorlevel 1 goto :error
del /q "%ROOT_DIR%dist\scan_worker.exe" >nul 2>nul
del /q "%ROOT_DIR%dist\render_worker.exe" >nul 2>nul

echo.
echo Build complete.
echo Main app: dist\HoudiniSimpleRenderManager\HoudiniSimpleRenderManager.exe
goto :eof

:error
echo.
echo Build failed.
exit /b 1
