@echo off
set PATH=C:\Program Files\Python310\;c:\Program Files (x86)\Windows Kits\8.1\bin\x86\;%PATH%
REM Refuse empty/corrupt packaging zips (Build-dev-binaries #551 was ~352 bytes).
REM 1 MiB — keep in lockstep with SnowflakeCLI-Win-x86-64 upload refuse.
set MIN_ZIP_BYTES=1048576

python.exe --version
if errorlevel 1 exit /b 1
python.exe -c "import platform as p; print(f'{p.system()=}, {p.architecture()=}')"
if errorlevel 1 exit /b 1

python.exe -m pip install --upgrade pip click==8.2.1 hatch==1.15.1 virtualenv==20.39.1 -c scripts\packaging\uv.constraint
if errorlevel 1 (
  echo ERROR: pip install failed
  exit /b 1
)

REM cmd: "if errorlevel 1" is true when the previous exit code is >= 1, not exactly 1.
python.exe scripts\packaging\install_verified_rust.py
if errorlevel 1 exit /b 1
set PATH=%PATH%;%USERPROFILE%\.cargo\bin\

@echo off
FOR /F "delims=" %%I IN ('hatch run packaging:win-build-version') DO SET CLI_VERSION_WIN=%%I
if "%CLI_VERSION_WIN%"=="" (
  echo ERROR: empty CLI_VERSION_WIN from hatch
  exit /b 1
)
echo %CLI_VERSION_WIN%
FOR /F "delims=" %%I IN ('hatch version') DO SET CLI_VERSION=%%I
if "%CLI_VERSION%"=="" (
  echo ERROR: empty CLI_VERSION from hatch
  exit /b 1
)
echo %CLI_VERSION%

if exist dist (
  RMDIR /S /Q dist
  if exist dist (
    echo ERROR: could not remove dist
    exit /b 1
  )
)
DEL /Q *.wixobj 2>nul

@echo on
call :build_and_stage snow
if errorlevel 1 exit /b 1

if "%BUILD_SNOWFLAKE_MANAGED_TARBALL%"=="1" goto :managed
tar -a -c -f snowflake-cli-%CLI_VERSION_WIN%.zip dist\snow
if errorlevel 1 (
  echo ERROR: tar zip failed
  exit /b 1
)
call :assert_zip_ok snowflake-cli-%CLI_VERSION_WIN%.zip
if errorlevel 1 exit /b 1
goto :eof

:managed
echo Building snowflake-managed binary
set SNOWFLAKE_CLI_INSTALLATION_SOURCE=SNOWFLAKE_MANAGED
set SNOWFLAKE_CLI_PACK_MANAGED_TARBALL=0
call :build_and_stage snowflake-managed
if errorlevel 1 exit /b 1
tar -a -c -f snowflake-cli-%CLI_VERSION_WIN%.zip dist\snow dist\snowflake-managed
if errorlevel 1 (
  echo ERROR: tar managed zip failed
  exit /b 1
)
call :assert_zip_ok snowflake-cli-%CLI_VERSION_WIN%.zip
if errorlevel 1 exit /b 1
goto :eof

:build_and_stage
REM %1 = output dir under dist\ (snow or snowflake-managed)
set "OUT_DIR=%~1"
python.exe -m hatch -e packaging run build-isolated-binary
if errorlevel 1 (
  echo ERROR: hatch build-isolated-binary failed for %OUT_DIR%
  exit /b 1
)
if not exist "dist\binary\snow-%CLI_VERSION%.exe" (
  echo ERROR: missing dist\binary\snow-%CLI_VERSION%.exe after hatch
  exit /b 1
)
mkdir "dist\%OUT_DIR%" 2>nul
move "dist\binary\snow-%CLI_VERSION%.exe" "dist\%OUT_DIR%\snow.exe"
if errorlevel 1 (
  echo ERROR: move to dist\%OUT_DIR%\snow.exe failed
  exit /b 1
)
if not exist "dist\%OUT_DIR%\snow.exe" (
  echo ERROR: dist\%OUT_DIR%\snow.exe missing after move
  exit /b 1
)
"dist\%OUT_DIR%\snow.exe" --help
if errorlevel 1 (
  echo ERROR: dist\%OUT_DIR%\snow.exe --help failed
  exit /b 1
)
"dist\%OUT_DIR%\snow.exe" --version
if errorlevel 1 (
  echo ERROR: dist\%OUT_DIR%\snow.exe --version failed
  exit /b 1
)
exit /b 0

:assert_zip_ok
set "ZIP_PATH=%~1"
if not exist "%ZIP_PATH%" (
  echo ERROR: missing zip %ZIP_PATH%
  exit /b 1
)
for %%F in ("%ZIP_PATH%") do set ZIP_SIZE=%%~zF
if "%ZIP_SIZE%"=="" (
  echo ERROR: could not read size of %ZIP_PATH%
  exit /b 1
)
if %ZIP_SIZE% LSS %MIN_ZIP_BYTES% (
  echo ERROR: %ZIP_PATH% is %ZIP_SIZE% bytes ^< %MIN_ZIP_BYTES% ^(empty/corrupt packaging^)
  exit /b 1
)
set "ZIP_LIST=%ZIP_PATH%.list"
tar -tf "%ZIP_PATH%" > "%ZIP_LIST%"
if errorlevel 1 (
  echo ERROR: tar -tf failed for %ZIP_PATH% size=%ZIP_SIZE%
  exit /b 1
)
REM Windows tar -tf may list dist/snow/snow.exe or dist\snow\snow.exe.
findstr /I /C:"dist/snow/snow.exe" /C:"dist\snow\snow.exe" "%ZIP_LIST%" >nul
if errorlevel 1 (
  echo ERROR: %ZIP_PATH% missing member dist/snow/snow.exe
  type "%ZIP_LIST%"
  exit /b 1
)
if "%BUILD_SNOWFLAKE_MANAGED_TARBALL%"=="1" (
  findstr /I /C:"dist/snowflake-managed/snow.exe" /C:"dist\snowflake-managed\snow.exe" "%ZIP_LIST%" >nul
  if errorlevel 1 (
    echo ERROR: %ZIP_PATH% missing member dist/snowflake-managed/snow.exe
    type "%ZIP_LIST%"
    exit /b 1
  )
)
echo ZIP_OK=%ZIP_PATH% bytes=%ZIP_SIZE%
exit /b 0
