@echo off
echo %PATH%
echo "Updating PATH"
set PATH=C:\Program Files\Python310\;c:\Program Files (x86)\Windows Kits\8.1\bin\x86\;%PATH%
echo %PATH%

@echo on
python.exe --version
python.exe -m pip install --upgrade pip uv hatch

@echo off
FOR /F "delims=" %%I IN ('python.exe scripts\packaging\win\build_version.py') DO SET CLI_VERSION=%%I
echo %CLI_VERSION%

set CONTENTSDIR="snowflake-cli-%CLI_VERSION%"
echo %CONTENTSDIR%
set ENTRYPOINT=src\\snowflake\\cli\\_app\\__main__.py
exit
@echo on
python.exe -m hatch -e packaging run pyinstaller --name snow --onedir --clean --noconfirm --noconsole --contents-directory=%CONTENTSDIR% %ENTRYPOINT%

cd dist\snow
dir .
signtool sign /debug /sm /t http://timestamp.digicert.com /a snow.exe


cd ..\..
candle ^
    -dSnowflakeCLIVersion=%CLI_VERSION% ^
    scripts/packaging/win/snowflake_cli.wxs ^
    scripts/packaging/win/snowflake_cli_exitdlg.wxs
light snowflake_cli.wixobj ^
    snowflake_cli_exitdlg.wixobj ^
    -cultures:en-us ^
    -loc snowsql_en-us.wxl ^
    -ext WixUIExtension ^
    -ext WixUtilExtension ^
    -o dist\snowflake-cli-windows_x86_64.msi
