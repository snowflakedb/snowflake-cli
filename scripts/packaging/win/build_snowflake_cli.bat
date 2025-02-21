@echo off
set PATH=C:\Program Files\Python310\;c:\Program Files (x86)\Windows Kits\8.1\bin\x86\;%PATH%

python.exe --version
python.exe -c "import platform as p; print(f'{p.system()=}, {p.architecture()=}')"

python.exe -m pip install --upgrade pip uv hatch

curl https://static.rust-lang.org/rustup.sh > rustup-init.exe
rustup-init.exe \y
del rustup-init.exe

@echo off
FOR /F "delims=" %%I IN ('hatch run packaging:win-build-version') DO SET CLI_VERSION=%%I
echo %CLI_VERSION%

set ENTRYPOINT=src\\snowflake\\cli\\_app\\__main__.py

RMDIR /S /Q dist
RMDIR /S /Q build
DEL /Q *.wixobj

@echo on
python.exe -m hatch -e packaging run build-binaries-pyapp
dir dist
dir dist\binary

tar -a -c -f snowflake-cli-%CLI_VERSION%.zip dist\snow
