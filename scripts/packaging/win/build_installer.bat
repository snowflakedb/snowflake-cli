@echo on

REM replace with one from environment
set RELEASE_TYPE=dev

REM DEBUG:
REM aws s3 ls %STAGE_URL% --recursive

set CLI_VERSION=3.0.0

aws s3 cp s3://sfc-eng-jenkins/repository/snowflake-cli/staging/dev/windows_x86_64/56041f1f1e5f229265dd28385d87a4e345038efc/snowflake-cli-3.0.0.2.zip .
tar -xf snowflake-cli-3.0.0.2.zip

dir dist\snow /s

signtool sign /debug /sm /t http://timestamp.digitcert.com /a dist\snow\snow.exe
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
  -out snowflake-cli-%CLI_VERSION%-x86_64.msi ^
  snowflake_cli.wixobj ^
  snowflake_cli_exitdlg.wixobj ^
  _internal.wixobj

exist snowflake-cli-%CLI_VERSION%-x86_64.msi
