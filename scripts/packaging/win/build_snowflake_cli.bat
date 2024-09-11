@echo off
echo %PATH%
echo "Updating PATH"
set PATH=C:\Program Files\Python310\;c:\Program Files (x86)\Windows Kits\8.1\bin\x86\;%PATH%
echo %PATH%

@echo on
python.exe --version
python.exe -m pip install --upgrade pip uv hatch

@echo off
FOR /F "delims=" %%I IN ('hatch run packaging:win-build-version') DO SET CLI_VERSION=%%I
echo %CLI_VERSION%

set CONTENTSDIR="snowflake-cli-%CLI_VERSION%"
echo %CONTENTSDIR%
set ENTRYPOINT=src\\snowflake\\cli\\_app\\__main__.py

REM exit


@echo on
python.exe -m hatch -e packaging run ^
  pyinstaller ^
  --name snow ^
  --onedir ^
  --clean ^
  --noconfirm ^
  --console ^
  --icon=scripts\packaging\win\snowflake_msi.ico ^
  --contents-directory=%CONTENTSDIR% ^
  %ENTRYPOINT%

tar -a -c -f snow.zip dist\snow

cd dist\snow
dir /r .
signtool sign /debug /sm /t http://timestamp.digicert.com /a snow.exe

REM Build MSI-installer
cd ..\..
dir /r .
REM Generate wxs file for Wix
python.exe -m hatch -e packaging run ^
  python scripts\packaging\win\wxs_builder.py

candle ^
    -dSnowflakeCLIVersion=%CLI_VERSION% ^
    scripts\packaging\win\snowflake_cli.wxs ^
    scripts\packaging\win\snowflake_cli_exitdlg.wxs

dir /r .

light snowflake_cli.wixobj ^
    snowflake_cli_exitdlg.wixobj ^
    -cultures:en-us ^
    -loc scripts\packaging\win\snowflake_cli_en-us.wxl ^
    -ext WixUIExtension ^
    -ext WixUtilExtension ^
    -o dist\snowflake-cli-%CLI_VERSION%-windows_x86_64.msi

dir /r .
