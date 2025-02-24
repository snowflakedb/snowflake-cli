@echo on

set PATH=C:\Program Files\7-Zip;C:\Users\jenkins\AppData\Local\Programs\Python\Python38;C:\Users\jenkins\AppData\Local\Programs\Python\Python38\Scripts;C:\Program Files (x86)\WiX Toolset v3.11\bin;%PATH%

python.exe --version
python.exe -c "import platform as p; print(f'{p.system()=}, {p.architecture()=}')"

python.exe -m pip install hatch
FOR /F "delims=" %%I IN ('hatch run packaging:win-build-version') DO SET CLI_VERSION=%%I
FOR /F "delims=" %%I IN ('git rev-parse %svnRevision%') DO SET REVISION=%%I
FOR /F "delims=" %%I IN ('echo %releaseType%') DO SET RELEASE_TYPE=%%I

echo CLI_VERSION = `%CLI_VERSION%`
echo REVISION = `%REVISION%`
echo RELEASE_TYPE = %RELEASE_TYPE%`

set CLI_ZIP=snowflake-cli-%CLI_VERSION%.zip
set CLI_MSI=snowflake-cli-%CLI_VERSION%-x86_64.msi
set STAGE_URL=s3://sfc-eng-jenkins/repository/snowflake-cli/staging/%RELEASE_TYPE%/windows_x86_64/%REVISION%
set RELEASE_URL=s3://sfc-eng-jenkins/repository/snowflake-cli/%RELEASE_TYPE%/windows_x86_64/%REVISION%

echo "[INFO] downloading artifacts"
cmd /c aws s3 cp %STAGE_URL%/%CLI_ZIP% . || goto :error

echo "[INFO] building installer"
7z x %CLI_ZIP% || goto :error

signtool sign /debug /sm /d "Snowflake CLI" /t http://timestamp.digicert.com /a dist\snow\snow.exe || goto :error
signtool verify /v /pa dist\snow\snow.exe || goto :error

candle.exe ^
  -arch x64 ^
  -dSnowflakeCLIVersion=%CLI_VERSION% ^
  scripts\packaging\win\snowflake_cli.wxs ^
  scripts\packaging\win\snowflake_cli_exitdlg.wxs || goto :error

light.exe ^
  -ext WixUIExtension ^
  -ext WixUtilExtension ^
  -cultures:en-us ^
  -loc scripts\packaging\win\snowflake_cli_en-us.wxl ^
  -out %CLI_MSI% ^
  snowflake_cli.wixobj ^
  snowflake_cli_exitdlg.wixobj || goto :error

signtool sign /debug /sm /d "Snowflake CLI" /t http://timestamp.digicert.com /a %CLI_MSI% || goto :error
signtool verify /v /pa %CLI_MSI% || goto :error

echo "[INFO] uploading artifacts"
cmd /c aws s3 cp %CLI_MSI% %RELEASE_URL%/%CLI_MSI% || goto :error

REM FINISH SCRIPT EXECUTION HERE
GOTO :EOF

:error
echo Failed with error code #%errorlevel%.
exit /b %errorlevel%
