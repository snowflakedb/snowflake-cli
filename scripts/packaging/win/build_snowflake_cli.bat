@echo on
echo %PATH%
"C:\Program Files\Python310\python.exe" --version
"C:\Program Files\Python310\python.exe" -m pip install --upgrade pip uv hatch

set PATH = C:\Program Files\Python310\;%PATH%
echo %PATH%
python.exe --version
python.exe -m pip install --upgrade pip uv hatch

FOR /F "delims=" %%I IN ('python.exe -m hatch version') DO CLI_VERSION=%%I
echo %CLI_VERSION%

set CONTENTSDIR="snowflake-cli-%CLI_VERSION%"
set ENTRYPOINT=src\\snowflake\\cli\\_app\\__main__.py
python.exe -m hatch -e packaging run pyinstaller --name snow --onedir --clean --noconfirm --noconsole --contents-directory=%CONTENTSDIR% %ENTRYPOINT%

cd dist\snow
dir .
REM signtool sign /debug /sm /t http://timestamp.digicert.com /a snow.exe
wmic product get name, version
where /r c:\ "signtool*"
DIR "C:\Program Files (x86)\Windows Kits\10\bin\"


REM candle /?
REM light /?
