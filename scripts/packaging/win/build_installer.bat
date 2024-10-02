@echo on

set PATH=C:\Program Files\7-Zip;C:\Users\jenkins\AppData\Local\Programs\Python\Python38;C:\Users\jenkins\AppData\Local\Programs\Python\Python38\Scripts;C:\Program Files (x86)\WiX Toolset v3.11\bin;%PATH%

python.exe --version
python.exe -c "import platform as p; print(f'{p.system()=}, {p.architecture()=}')"

python.exe -m pip install hatch
FOR /F "delims=" %%I IN ('hatch run packaging:win-build-version') DO SET CLI_VERSION=%%I
FOR /F "delims=" %%I IN ('git rev-parse %svnRevision%') DO SET REVISION=%%I
@echo off
echo CLI_VERSION = %CLI_VERSION%
echo REVISION = %REVISION%`
@echo on

REM DEBUG
set REVISION=56041f1f1e5f229265dd28385d87a4e345038efc
set RELEASE_TYPE=dev

set CLI_ZIP=snowflake-cli-%CLI_VERSION%.zip
set CLI_MSI=snowflake-cli-%CLI_VERSION%-x86_64.msi
set STAGE_URL=s3://sfc-eng-jenkins/repository/snowflake-cli/staging/%RELEASE_TYPE%/windows_x86_64/%REVISION%

cmd /c aws s3 cp %STAGE_URL%/%CLI_ZIP% .

7z x %CLI_ZIP% &&

signtool sign /debug /sm /t http://timestamp.digicert.com /a dist\snow\snow.exe

heat.exe dir dist\snow\_internal ^
   -gg ^
   -cg SnowflakeCLIInternalFiles ^
   -dr TESTFILEPRODUCTDIR ^
   -var var.SnowflakeCLIInternalFiles ^
   -sfrag ^
   -o _internal.wxs

candle.exe ^
  -arch x64 ^
  -dSnowflakeCLIVersion=%CLI_VERSION% ^
  -dSnowflakeCLIInternalFiles=dist\\snow\\_internal ^
  scripts\packaging\win\snowflake_cli.wxs ^
  scripts\packaging\win\snowflake_cli_exitdlg.wxs ^
  _internal.wxs

light.exe ^
  -ext WixUIExtension ^
  -ext WixUtilExtension ^
  -cultures:en-us ^
  -loc scripts\packaging\win\snowflake_cli_en-us.wxl ^
  -out %CLI_MSI% ^
  snowflake_cli.wixobj ^
  snowflake_cli_exitdlg.wixobj ^
  _internal.wixobj

signtool sign /debug /sm /t http://timestamp.digicert.com /a %CLI_MSI%

aws s3 cp %CLI_MSI% %STAGE_URL%/%CLI_MSI%
