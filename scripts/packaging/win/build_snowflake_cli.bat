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


dotnet tool install --global wix
wix --version
where wix
wix extension add -g WixToolset.Util.wixext
wix extension add -g WixToolset.UI.wixext

cd dist\snow
dir /r .
signtool sign /debug /sm /t http://timestamp.digicert.com /a snow.exe

REM Build MSI-installer
cd ..\..

wix build -d SnowflakeCLIVersion=3.0.0.2 ^
  -o snowflake-cli-3.0.0.dev0.2-windows_x86_64.msi ^
  -ext WixToolset.UI.wixext ^
  scripts\packaging\win\snowflake_cli.wxs

dir /r .
