@echo on

dir C:\Users\jenkins\AppData\Local\Programs\Python

set PATH=C:\Program Files\7-Zip;C:\Users\jenkins\AppData\Local\Programs\Python\Python38;C:\Users\jenkins\AppData\Local\Programs\Python\Python38\Scripts;C:\Program Files (x86)\WiX Toolset v3.11\bin;%PATH%

REM      |FOR /F "delims=" %%I IN ('hatch run packaging:win-build-version') DO SET CLI_VERSION=%%I
REM      |FOR /F "delims=" %%I IN ('git rev-parse %svnRevision%') DO SET REVISION=%%I
REM      |set STAGE_URL=s3://sfc-eng-jenkins/repository/snowflake-cli/staging/%releaseType%/windows_x86_64/%REVISION%/

python.exe --version
python.exe -c "import platform as p; print(f'{p.system()=}, {p.architecture()=}')"

REM replace with one from environment
set RELEASE_TYPE=dev

REM DEBUG:
echo %CLI_VERSION%
set CLI_VERSION=3.0.0.2
set STAGE_URL=s3://sfc-eng-jenkins/repository/snowflake-cli/staging/dev/windows_x86_64/56041f1f1e5f229265dd28385d87a4e345038efc/snowflake-cli-3.0.0.2.zip

aws s3 cp %STAGE_URL% . && ^
7z x snowflake-cli-%CLI_VERSION%.zip && ^
dir && ^
signtool sign /debug /sm /t http://timestamp.digitcert.com /a dist\snow\snow.exe && ^
heat.exe dir dist\snow\_internal ^
   -gg ^
   -cg SnowflakeCLIInternalFiles ^
   -dr TESTFILEPRODUCTDIR ^
   -var var.SnowflakeCLIInternalFiles ^
   -sfrag ^
   -o _internal.wxs && ^
candle.exe ^
  -arch x64 ^
  -dSnowflakeCLIVersion=%CLI_VERSION% ^
  -dSnowflakeCLIInternalFiles=dist\\snow\\_internal ^
  scripts\packaging\win\snowflake_cli.wxs ^
  scripts\packaging\win\snowflake_cli_exitdlg.wxs ^
  _internal.wxs && ^
light.exe ^
  -ext WixUIExtension ^
  -ext WixUtilExtension ^
  -cultures:en-us ^
  -loc scripts\packaging\win\snowflake_cli_en-us.wxl ^
  -out snowflake-cli-%CLI_VERSION%-x86_64.msi ^
  snowflake_cli.wixobj ^
  snowflake_cli_exitdlg.wixobj ^
  _internal.wixobj && ^
signtool sign /debug /sm /t http://timestamp.digitcert.com /a snowflake-cli-%CLI_VERSION%-x86_64.msi
