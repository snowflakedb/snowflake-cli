@echo on
echo %PATH%
where /r C:\Program Files python.exe
"C:\Program Files\Python310\python.exe" --version
"C:\Program Files\Python310\python.exe" -m pip install --upgrade pip uv hatch
