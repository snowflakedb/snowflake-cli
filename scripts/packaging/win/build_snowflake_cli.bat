@echo off
set PATH=C:\Program Files\Python310\;c:\Program Files (x86)\Windows Kits\8.1\bin\x86\;%PATH%

python.exe --version
python.exe -c "import platform as p; print(f'{p.system()=}, {p.architecture()=}')"

python.exe -m pip install --upgrade pip uv hatch

@echo off
FOR /F "delims=" %%I IN ('hatch run packaging:win-build-version') DO SET CLI_VERSION=%%I
echo %CLI_VERSION%

set ENTRYPOINT=src\\snowflake\\cli\\_app\\__main__.py

RMDIR /S /Q dist
RMDIR /S /Q build
DEL /Q *.wixobj

@echo on
python.exe -m hatch -e packaging run ^
  pyinstaller ^
  --target-arch=64bit ^
  --name snow ^
  --onedir ^
  --clean ^
  --noconfirm ^
  --console ^
  --icon=scripts\packaging\win\snowflake_msi.ico ^
  %ENTRYPOINT%

tar -a -c -f snowflake-cli-%CLI_VERSION%.zip dist\snow


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
