@echo on

echo "[INFO] testing installation"

set CLI_VERSION=3.0.0.2
set REVISION=56041f1f1e5f229265dd28385d87a4e345038efc
set RELEASE_TYPE=dev
set CLI_MSI=snowflake-cli-%CLI_VERSION%-x86_64.msi
set STAGE_URL=s3://sfc-eng-jenkins/repository/snowflake-cli/staging/%RELEASE_TYPE%/windows_x86_64/%REVISION%

cmd /c aws s3 cp %STAGE_URL%/%CLI_MSI% .

snow.exe -h

msiexec /h

msiexec /i /n %CLI_MSI%

snow.exe -h

REM msiexec /uninstall /n %CLI_MSI%

snow.exe -h
