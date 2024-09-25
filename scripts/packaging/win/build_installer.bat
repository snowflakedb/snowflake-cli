@echo on

REM replace with one from environment
set RELEASE_TYPE=dev

aws s3 ls %STAGE_URL% --recursive

heat.exe /?
candle.exe /?
light.exe /?


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
