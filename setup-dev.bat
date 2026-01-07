@echo off
setlocal enabledelayedexpansion

echo.
echo ========================================
echo   MCP-Sample-Data Dev Setup
echo ========================================
echo.

:: Check if Python 3.13 is available
echo Checking for Python 3.13...
set "PYTHON_CMD="

:: Try py launcher with 3.13
py -3.13 --version >nul 2>&1
if %errorlevel%==0 (
    set "PYTHON_CMD=py -3.13"
    goto :python_found
)

:: Try common installation paths
if exist "%LOCALAPPDATA%\Programs\Python\Python313\python.exe" (
    set "PYTHON_CMD=%LOCALAPPDATA%\Programs\Python\Python313\python.exe"
    goto :python_found
)

if exist "%PROGRAMFILES%\Python313\python.exe" (
    set "PYTHON_CMD=%PROGRAMFILES%\Python313\python.exe"
    goto :python_found
)

if exist "C:\Python313\python.exe" (
    set "PYTHON_CMD=C:\Python313\python.exe"
    goto :python_found
)

:: Python 3.13 not found - try to install
echo Python 3.13 not found. Attempting to install...
winget --version >nul 2>&1
if %errorlevel%==0 (
    echo Installing Python 3.13 via winget...
    winget install Python.Python.3.13 --accept-source-agreements --accept-package-agreements --silent
    timeout /t 3 /nobreak >nul

    if exist "%LOCALAPPDATA%\Programs\Python\Python313\python.exe" (
        set "PYTHON_CMD=%LOCALAPPDATA%\Programs\Python\Python313\python.exe"
        goto :python_found
    )
)

echo.
echo ERROR: Python 3.13 not found and could not be installed!
echo Please install Python 3.13 from: https://www.python.org/downloads/
echo.
pause
exit /b 1

:python_found
for /f "tokens=*" %%i in ('!PYTHON_CMD! --version') do echo Found: %%i
echo Using: !PYTHON_CMD!
echo.

:: Get script directory
set "SCRIPT_DIR=%~dp0"
set "PROJECT_DIR=%SCRIPT_DIR:~0,-1%"

echo Project directory: %PROJECT_DIR%
echo.

:: Create virtual environment
echo Step 1/4: Creating virtual environment...
!PYTHON_CMD! -m venv "%PROJECT_DIR%\venv"

if not exist "%PROJECT_DIR%\venv\Scripts\python.exe" (
    echo ERROR: Virtual environment creation failed!
    pause
    exit /b 1
)

echo Virtual environment created successfully.
echo.

:: Activate and install dependencies
echo Step 2/4: Installing dependencies...
call "%PROJECT_DIR%\venv\Scripts\activate.bat"
pip install --upgrade pip
pip install -r requirements.txt

if errorlevel 1 (
    echo WARNING: Some dependencies may have failed to install.
)

:: Create required directories
echo.
echo Step 3/4: Creating directories...
if not exist "%PROJECT_DIR%\logs" mkdir "%PROJECT_DIR%\logs"
if not exist "%PROJECT_DIR%\exports" mkdir "%PROJECT_DIR%\exports"
if not exist "%PROJECT_DIR%\projects" mkdir "%PROJECT_DIR%\projects"
echo Directories created.

:: Configure Claude Desktop
echo.
echo ========================================
echo   Claude Desktop Configuration
echo ========================================
echo.

set "configPath=%APPDATA%\Claude\claude_desktop_config.json"

echo Step 4/4: Updating Claude Desktop config...

:: Ensure Claude config directory exists
if not exist "%APPDATA%\Claude" mkdir "%APPDATA%\Claude"

:: Use PowerShell to handle JSON
powershell -ExecutionPolicy Bypass -Command ^
    "$configPath = '%configPath%';" ^
    "$projectDir = '%PROJECT_DIR%';" ^
    "$serverName = 'MCP-Sample-Data';" ^
    "$pythonPath = Join-Path $projectDir 'venv\Scripts\python.exe';" ^
    "$scriptPath = Join-Path $projectDir 'src\sample_data_server.py';" ^
    "$mcpServer = @{ 'command' = $pythonPath; 'args' = @($scriptPath) };" ^
    "if (Test-Path $configPath) { try { $config = Get-Content $configPath -Raw -Encoding UTF8 | ConvertFrom-Json } catch { $config = [PSCustomObject]@{} } } else { $config = [PSCustomObject]@{} };" ^
    "if (-not $config.PSObject.Properties['mcpServers']) { $config | Add-Member -NotePropertyName 'mcpServers' -NotePropertyValue ([PSCustomObject]@{}) };" ^
    "if ($config.mcpServers.PSObject.Properties[$serverName]) { $config.mcpServers.$serverName = $mcpServer; Write-Host 'Updated existing' $serverName 'configuration' -ForegroundColor Green } else { $config.mcpServers | Add-Member -NotePropertyName $serverName -NotePropertyValue $mcpServer; Write-Host 'Added' $serverName 'configuration' -ForegroundColor Green };" ^
    "$json = $config | ConvertTo-Json -Depth 10;" ^
    "[System.IO.File]::WriteAllText($configPath, $json, [System.Text.UTF8Encoding]::new($false));" ^
    "Write-Host '';" ^
    "Write-Host 'Config saved to:' $configPath -ForegroundColor Cyan;" ^
    "Write-Host 'Python:' $pythonPath;" ^
    "Write-Host 'Script:' $scriptPath;"

:: Success
echo.
echo ========================================
echo   Setup Complete!
echo ========================================
echo.
echo Project path: %PROJECT_DIR%
echo Claude config: %configPath%
echo MCP Server: MCP-Sample-Data
echo.
echo IMPORTANT: Restart Claude Desktop for changes to take effect!
echo.
echo To start manually:
echo   1. cd "%PROJECT_DIR%"
echo   2. venv\Scripts\activate.bat
echo   3. python src/sample_data_server.py
echo.
pause
