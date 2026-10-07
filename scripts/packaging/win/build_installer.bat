@echo on

set PATH=C:\Program Files\7-Zip;C:\Users\jenkins\AppData\Local\Programs\Python\Python38;C:\Users\jenkins\AppData\Local\Programs\Python\Python38\Scripts;C:\Program Files (x86)\WiX Toolset v3.11\bin;%PATH%

python.exe --version
python.exe -c "import platform as p; print(f'{p.system()=}, {p.architecture()=}')"

python.exe -m pip install click==8.2.1 hatch==1.15.1 virtualenv==20.39.1
REM WiX/MSI and the unsigned zip use the 4-integer Windows version (3.29.0.dev0 -> 3.29.0.0).
REM The managed tarball/fragment must use hatch version so Assemble-Managed can merge with Linux/Mac.
FOR /F "delims=" %%I IN ('hatch run packaging:win-build-version') DO SET CLI_VERSION_WIN=%%I
FOR /F "delims=" %%I IN ('hatch version') DO SET CLI_VERSION=%%I
FOR /F "delims=" %%I IN ('git rev-parse %svnRevision%') DO SET REVISION=%%I
FOR /F "delims=" %%I IN ('echo %releaseType%') DO SET RELEASE_TYPE=%%I

echo CLI_VERSION = `%CLI_VERSION%`
echo CLI_VERSION_WIN = `%CLI_VERSION_WIN%`
echo REVISION = `%REVISION%`
echo RELEASE_TYPE = %RELEASE_TYPE%`

set CLI_ZIP=snowflake-cli-%CLI_VERSION_WIN%.zip
set CLI_MSI=snowflake-cli-%CLI_VERSION_WIN%-x86_64.msi
set STAGE_URL=s3://sfc-eng-jenkins/repository/snowflake-cli/staging/%RELEASE_TYPE%/windows_x86_64/%REVISION%
set RELEASE_URL=s3://sfc-eng-jenkins/repository/snowflake-cli/%RELEASE_TYPE%/windows_x86_64/%REVISION%

echo "[INFO] downloading artifacts"
cmd /c aws s3 cp %STAGE_URL%/%CLI_ZIP% . || goto :error

echo "[INFO] building installer"
7z x %CLI_ZIP% || goto :error

set SM_HOST=https://clientauth.one.digicert.com
set SM_CLIENT_CERT_FILE=%WORKSPACE%\Certificate_pkcs12.p12
smctl healthcheck || goto :error
smctl windows certsync || goto :error

smctl sign --keypair-alias %digicert_key_name% --input dist\snow\snow.exe || goto :error
signtool verify /v /pa dist\snow\snow.exe || goto :error

if exist dist\snowflake-managed\snow.exe (
  smctl sign --keypair-alias %digicert_key_name% --input dist\snowflake-managed\snow.exe || goto :error
  signtool verify /v /pa dist\snowflake-managed\snow.exe || goto :error
  python.exe scripts\packaging\build_isolated_binary_with_hatch.py --pack-tarball dist\snowflake-managed\snow.exe --version %CLI_VERSION% --os-name windows --arch amd64 || goto :error
)

candle.exe ^
  -arch x64 ^
  -dSnowflakeCLIVersion=%CLI_VERSION_WIN% ^
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

smctl sign --keypair-alias %digicert_key_name% --input %CLI_MSI% || goto :error
signtool verify /v /pa %CLI_MSI% || goto :error

echo "[INFO] uploading artifacts"
cmd /c aws s3 cp %CLI_MSI% %RELEASE_URL%/%CLI_MSI% || goto :error
if exist dist\snowflake-cli-%CLI_VERSION%-windows-amd64.tar.gz (
  cmd /c aws s3 cp dist\snowflake-cli-%CLI_VERSION%-windows-amd64.tar.gz %RELEASE_URL%/snowflake-cli-%CLI_VERSION%-windows-amd64.tar.gz || goto :error
  cmd /c aws s3 cp dist\manifest-windows-amd64.json %RELEASE_URL%/manifest-windows-amd64.json || goto :error
)

REM FINISH SCRIPT EXECUTION HERE
GOTO :EOF

:error
echo Failed with error code #%errorlevel%.
exit /b %errorlevel%
