@echo off
set PATH=C:\Program Files\Python310\;c:\Program Files (x86)\Windows Kits\8.1\bin\x86\;%PATH%

python.exe --version
python.exe -c "import platform as p; print(f'{p.system()=}, {p.architecture()=}')"

python.exe -m pip install --upgrade pip uv hatch

curl -o rustup-init.exe https://win.rustup.rs/
rustup-init.exe -y
del rustup-init.exe
set PATH=%PATH%;%USERPROFILE%\.cargo\bin\

@echo off
FOR /F "delims=" %%I IN ('hatch run packaging:win-build-version') DO SET CLI_VERSION_WIN=%%I
echo %CLI_VERSION_WIN%
FOR /F "delims=" %%I IN ('hatch version') DO SET CLI_VERSION=%%I
echo %CLI_VERSION%

RMDIR /S /Q dist
DEL /Q *.wixobj

@echo on
python.exe -m hatch -e packaging run build-isolated-binary
dir dist\binary

mkdir dist\snow
move dist\binary\snow-%CLI_VERSION%.exe dist\snow\snow.exe
.\dist\snow\snow.exe --help

tar -a -c -f snowflake-cli-%CLI_VERSION_WIN%.zip dist\snow
