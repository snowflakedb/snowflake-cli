@echo on

REM replace with one from environment
set RELEASE_TYPE=dev

REM DEBUG:
REM aws s3 ls %STAGE_URL% --recursive
aws s3 ls s3://sfc-eng-jenkins/repository/snowflake-cli/staging/dev/windows_x86_64/56041f1f1e5f229265dd28385d87a4e345038efc/snowflake-cli-3.0.0.2.zip --recursive

tar -xf snowflake-cli-3.0.0.2.zip
dir

heat.exe /?
REM candle.exe /?
REM light.exe /?


REM heat.exe dir dist\\snow\\_internal ^
REM   -gg ^
REM   -cg SnowflakeCLIInternalFiles ^
REM   -dr TESTFILEPRODUCTDIR ^
REM   -var var.SnowflakeCLIInternalFiles ^
REM   -sfrag ^
REM   -o _internal.wxs
REM
REM candle.exe ^
REM   -arch x64 ^
REM   -dSnowflakeCLIVersion=%CLI_VERSION% ^
REM   -dSnowflakeCLIInternalFiles=dist\\snow\\_internal ^
REM   scripts\packaging\win\snowflake_cli.wxs ^
REM   scripts\packaging\win\snowflake_cli_exitdlg.wxs ^
REM   _internal.wxs
REM
REM light.exe ^
REM   -ext WixUIExtension ^
REM   -ext WixUtilExtension ^
REM   -cultures:en-us ^
REM   -loc scripts\packaging\win\snowflake_cli_en-us.wxl ^
REM   -out snowflake-cli-%CLI_VERSION%-x86_64.msi ^
REM   snowflake_cli.wixobj ^
REM   snowflake_cli_exitdlg.wixobj ^
REM   _internal.wixobj
