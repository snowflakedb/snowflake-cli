#Requires -Version 5.1
# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

<#
.SYNOPSIS
    Installs the snowflake-managed distribution of Snowflake CLI on Windows.

.DESCRIPTION
    Downloads and installs the latest stable snowflake-managed Snowflake CLI.
    The binary is installed to %LOCALAPPDATA%\snowflake-cli\<version>\snow.exe.
    Shims live in %LOCALAPPDATA%\snowflake-cli\bin\ (snow.cmd and a POSIX snow
    for Git Bash). Releng copies this file to
    https://sfc-repo.snowflakecomputing.com/snowflake-cli/install.ps1

    Override install root with SNOWFLAKE_CLI_MANAGED_HOME.
    Override repo base with SNOWFLAKE_CLI_MANAGED_REPO.
    NON_INTERACTIVE / SKIP_PATH_PROMPT skip the PATH prompt (CI).

.EXAMPLE
    irm https://sfc-repo.snowflakecomputing.com/snowflake-cli/install.ps1 | iex
#>

$ErrorActionPreference = "Stop"

$BinaryName = "snow.exe"
$DefaultRepoBase = "https://sfc-repo.snowflakecomputing.com/snowflake-cli"
$PointerName = "stable_version.txt"
$ManifestName = "manifest.json"
$ManifestSigName = "manifest.json.sig"
# RSA-4096 n/e matching managed_manifest.pub.pem (PowerShell 5.1 has no ImportFromPem).
$ManifestModulusB64 = "mecfO6u7BbYw16AHXhDyMOGlJop5LZ08eZrsVswXlps+19CtIEJQQJZPjTKDAqDay0+TpNfj0ErberXW4pRlWTHV1IsRrMBZQwj+Rx7wfcIBCnctMvXVinCTQxDCG6QVEIvyntSi/shju7ktWeONGT4deCy7ZnTnE2abtK8sre+sbNAy5UbPiiAWULcoXmmpI/upQhGWDYsugMGdUQAmaA3u/QzoH/PwH8pClZWUJfbid05U51rUV6WzScb6PZP0PK9JYcSVTHZscmGCYkY3LrlCNZ+zMy1JCnp2B0cWzwaHoHFBmAACUMJLHz1NtQyRbHcLekKSgr5CIMMNIZmVrDYYTrGxnqfph6RMCVBd1RuzcXSVZhxTI2o93r0GR/7J+tr1GmY003dfuuWlHsBa+zKA8NyJDCnq7atW96AnnH78VQ+PuwMxEzmkgbi50lmAtGZuBpRtQn5ySWnaVKG1S2qBnPgiPWLJ7r7cViK0ZL5Mk8z0hkLXBDlop9MVea9LV9x1evFDkgoC49Gn5g4ZyqwTODl1tnE7/i47SKfqZl6lTnhbYLlUYYGt1pFmOp8w98ycCNynSFwAKKPr48FJuUCg4ucODyWnSw5d7xVnaUbqiHCj7QgPGWR5ssyFnlYHJD8os319nd/0xgvopeeXnPViy3zvEUsQyDblLYH2jQM="
$ManifestExponentB64 = "AQAB"

function Get-LongPath {
    param([string]$Path)
    return (Get-Item -LiteralPath $Path).FullName
}

function Get-InstallRoot {
    if ($env:SNOWFLAKE_CLI_MANAGED_HOME) {
        return $env:SNOWFLAKE_CLI_MANAGED_HOME.TrimEnd('\', '/')
    }
    $localAppData = Get-LongPath $env:LOCALAPPDATA
    return Join-Path $localAppData "snowflake-cli"
}

function Get-RepoBase {
    if ($env:SNOWFLAKE_CLI_MANAGED_REPO) {
        return $env:SNOWFLAKE_CLI_MANAGED_REPO.TrimEnd('/')
    }
    return $DefaultRepoBase
}

$InstallBaseDir = Get-InstallRoot
$RepoBase = Get-RepoBase

function Write-Success {
    param([string]$Message)
    Write-Host "[OK] $Message" -ForegroundColor Green
}

function Write-ErrorMsg {
    param([string]$Message)
    Write-Host "[ERROR] $Message" -ForegroundColor Red
}

function Write-Info {
    param([string]$Message)
    Write-Host $Message -ForegroundColor Cyan
}

function Get-StableVersion {
    $versionUrl = "$RepoBase/$PointerName"
    try {
        $response = Invoke-WebRequest -Uri $versionUrl -UseBasicParsing
        return $response.Content.Trim()
    }
    catch {
        Write-ErrorMsg "Failed to fetch stable version from $versionUrl"
        Write-ErrorMsg $_.Exception.Message
        exit 1
    }
}

# Must match src/snowflake/cli/_plugins/upgrade/layout.py validate_version.
function Assert-ManagedVersion {
    param([string]$Version)
    if (
        $Version -notmatch '^[A-Za-z0-9][A-Za-z0-9._-]*$' -or
        $Version -in @('bin', '.current', '.previous')
    ) {
        Write-ErrorMsg "Invalid snowflake-managed version '$Version'. Use a version directory name such as 3.12.0."
        exit 1
    }
}

# Package names from the manifest are used in URLs. Reject path separators so a
# compromised name cannot walk out of the version directory (CWE-22).
function Assert-PackageName {
    param([string]$Name)
    if (
        [string]::IsNullOrWhiteSpace($Name) -or
        $Name -eq '.' -or
        $Name -eq '..' -or
        $Name.Contains('/') -or
        $Name.Contains('\')
    ) {
        Write-ErrorMsg "Invalid package name in manifest: $Name"
        exit 1
    }
}

function Get-UrlEncodedVersion {
    param([string]$Version)
    return $Version -replace '\+', '%2B'
}

function Get-RepoVersionDir {
    param([string]$Version)
    $encodedVersion = Get-UrlEncodedVersion $Version
    return "$RepoBase/$encodedVersion/"
}

function Get-PlatformInfo {
    $os = "windows"
    if ([Environment]::Is64BitOperatingSystem) {
        if ($env:PROCESSOR_ARCHITECTURE -eq "ARM64") {
            $arch = "arm64"
        } else {
            $arch = "amd64"
        }
    } else {
        Write-ErrorMsg "32-bit Windows is not supported"
        exit 1
    }
    return @{ OS = $os; Arch = $arch }
}

function Test-ManifestSignature {
    param([string]$ManifestPath, [string]$SigPath)
    $data = [System.IO.File]::ReadAllBytes((Get-Item -LiteralPath $ManifestPath).FullName)
    $sig = [System.IO.File]::ReadAllBytes((Get-Item -LiteralPath $SigPath).FullName)
    $rsa = New-Object System.Security.Cryptography.RSACryptoServiceProvider
    $params = New-Object System.Security.Cryptography.RSAParameters
    $params.Modulus = [Convert]::FromBase64String($script:ManifestModulusB64)
    $params.Exponent = [Convert]::FromBase64String($script:ManifestExponentB64)
    $rsa.ImportParameters($params)
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $hash = $sha.ComputeHash($data)
        $oid = [System.Security.Cryptography.CryptoConfig]::MapNameToOID("SHA256")
        return $rsa.VerifyHash($hash, $oid, $sig)
    }
    finally {
        $sha.Dispose()
        $rsa.PersistKeyInCsp = $false
        $rsa.Clear()
    }
}

function Get-Manifest {
    param([string]$Version, [string]$TempDir)
    $base = Get-RepoVersionDir $Version
    $manifestUrl = $base + $ManifestName
    $sigUrl = $base + $ManifestSigName
    $manifestPath = Join-Path $TempDir "manifest.json"
    $sigPath = Join-Path $TempDir "manifest.json.sig"
    try {
        Invoke-WebRequest -Uri $manifestUrl -OutFile $manifestPath -UseBasicParsing
    }
    catch {
        Write-ErrorMsg "Failed to download manifest from $manifestUrl"
        Write-ErrorMsg $_.Exception.Message
        exit 1
    }
    try {
        Invoke-WebRequest -Uri $sigUrl -OutFile $sigPath -UseBasicParsing
    }
    catch {
        Write-ErrorMsg "Missing signature for manifest.json. Refusing to install unsigned snowflake-managed package."
        Write-ErrorMsg $_.Exception.Message
        exit 1
    }
    if (-not (Test-ManifestSignature $manifestPath $sigPath)) {
        Write-ErrorMsg "Invalid signature on manifest.json. Refusing to install."
        exit 1
    }
    return (Get-Content -Raw -LiteralPath $manifestPath) | ConvertFrom-Json
}

function Get-PackageInfo {
    param($Manifest, [string]$OS, [string]$Arch)

    $packages = $Manifest.packages
    if (-not $packages.$OS) {
        return $null
    }
    if (-not $packages.$OS.$Arch) {
        return $null
    }

    return @{
        TarName = $packages.$OS.$Arch.name
        Checksum = $packages.$OS.$Arch.checksum
    }
}

function Test-Checksum {
    param([string]$FilePath, [string]$ExpectedChecksum)

    $hash = Get-FileHash -Path $FilePath -Algorithm SHA256
    $actualChecksum = $hash.Hash.ToLower()
    $expectedLower = $ExpectedChecksum.ToLower()
    if ($expectedLower.StartsWith("sha256:")) {
        $expectedLower = $expectedLower.Substring(7)
    }

    if ($actualChecksum -ne $expectedLower) {
        Write-ErrorMsg "Checksum mismatch: expected $expectedLower, got $actualChecksum"
        return $false
    }
    return $true
}

function Test-NonInteractive {
    return [string]::IsNullOrEmpty($env:NON_INTERACTIVE) -eq $false
}

function Test-SkipPathPrompt {
    return [string]::IsNullOrEmpty($env:SKIP_PATH_PROMPT) -eq $false
}

function Add-ToPath {
    param([string]$Directory)

    $currentPath = [Environment]::GetEnvironmentVariable("Path", "User")
    if ($null -eq $currentPath) {
        $currentPath = ""
    }

    $paths = $currentPath -split ';'
    if ($paths -contains $Directory) {
        Write-Success "$Directory is already in PATH"
        return $true
    }

    if (Test-SkipPathPrompt) {
        Write-Host "Skipping PATH configuration (SKIP_PATH_PROMPT is set)"
        Write-Host "This session: add $Directory to PATH to use snow."
        return $false
    }

    $autoAdd = Test-NonInteractive
    if (-not $autoAdd) {
        Write-Host ""
        Write-Host "$Directory needs to be added to your PATH to use snowflake-managed Snowflake CLI." -ForegroundColor Yellow
        $response = Read-Host "Would you like to add it now? [Y/n]"
        $autoAdd = ($response -eq '') -or ($response -match '^[Yy]')
    }

    if ($autoAdd) {
        $newPath = $Directory + ";" + $currentPath
        [Environment]::SetEnvironmentVariable("Path", $newPath, "User")
        $env:Path = $Directory + ";" + $env:Path
        Write-Success "Added $Directory to PATH"
        Write-Host ""
        Write-Info "PATH has been updated. You may need to restart your terminal for changes to take effect."
        return $true
    }

    Write-Host ""
    Write-Host "To use Snowflake CLI, add this directory to your PATH manually:" -ForegroundColor Yellow
    Write-Host "    $Directory" -ForegroundColor Green
    return $false
}

function New-ManagedShims {
    param([string]$VersionDir)

    $binDir = Join-Path $InstallBaseDir "bin"
    if (-not (Test-Path $binDir)) {
        New-Item -ItemType Directory -Path $binDir -Force | Out-Null
    }

    $binary = Join-Path $VersionDir $BinaryName
    $cmdPath = Join-Path $binDir "snow.cmd"
    $cmdContent = "@echo off`r`n`"$binary`" %*`r`n"
    Set-Content -Path $cmdPath -Value $cmdContent -NoNewline -Encoding ASCII

    $posixPath = Join-Path $binDir "snow"
    $posixTarget = ($binary -replace '\\', '/')
    $posixContent = "#!/bin/sh`nexec `"$posixTarget`" `"`$@`"`n"
    Set-Content -Path $posixPath -Value $posixContent -NoNewline -Encoding ASCII
    return $binDir
}

function Write-Pointer {
    param([string]$Name, [string]$Value)
    $path = Join-Path $InstallBaseDir $Name
    Set-Content -Path $path -Value "$Value`n" -NoNewline -Encoding ASCII
}

function Read-Pointer {
    param([string]$Name)
    $path = Join-Path $InstallBaseDir $Name
    if (Test-Path -LiteralPath $path) {
        return (Get-Content -LiteralPath $path -Raw).Trim()
    }
    return $null
}

function Remove-DirectoryBestEffort {
    param([string]$Directory)

    if ([string]::IsNullOrWhiteSpace($Directory)) {
        return
    }

    try {
        if (Test-Path -LiteralPath $Directory) {
            Remove-Item -LiteralPath $Directory -Recurse -Force -ErrorAction Stop
            return
        }
    } catch {
        try {
            cmd /d /c "rd /s /q `"$Directory`"" 2>$null
        } catch {
            # Best-effort cleanup.
        }
    }
}

function Get-PayloadDirectory {
    param([string]$ExtractDir)

    $direct = Join-Path $ExtractDir $BinaryName
    if (Test-Path -LiteralPath $direct) {
        return $ExtractDir
    }

    $dirs = @(Get-ChildItem -Path $ExtractDir -Directory)
    if ($dirs.Count -eq 1) {
        $nested = Join-Path $dirs[0].FullName $BinaryName
        if (Test-Path -LiteralPath $nested) {
            return $dirs[0].FullName
        }
    }

    Write-ErrorMsg "Tarball does not contain $BinaryName at the root or in a single top-level directory."
    exit 1
}

function Invoke-SnowflakeCliInstall {
    Write-Host ""
    Write-Host "Installing snowflake-managed Snowflake CLI for Windows..." -ForegroundColor Cyan
    Write-Host ""

    $platform = Get-PlatformInfo
    Write-Host "Platform: $($platform.OS)-$($platform.Arch)"

    Write-Host "Fetching latest version..."
    $version = Get-StableVersion
    if ([string]::IsNullOrWhiteSpace($version)) {
        Write-ErrorMsg "Failed to determine the latest snowflake-managed version of Snowflake CLI"
        exit 1
    }
    Assert-ManagedVersion $version
    Write-Success "Latest version: $version"

    $randomNum = Get-Random
    $tempDir = Join-Path (Get-LongPath $env:TEMP) "snowflake-cli-install-$randomNum"
    New-Item -ItemType Directory -Path $tempDir -Force | Out-Null

    try {
        Write-Host "Downloading manifest..."
        $manifest = Get-Manifest $version $tempDir

        $packageInfo = Get-PackageInfo $manifest $platform.OS $platform.Arch
        if (-not $packageInfo) {
            Write-ErrorMsg "Snowflake CLI is not available for your platform: $($platform.OS)-$($platform.Arch)"
            exit 1
        }

        $tarName = $packageInfo.TarName
        $expectedChecksum = $packageInfo.Checksum
        Assert-PackageName $tarName
        Write-Success "Found package: $tarName"

        $baseUrl = Get-RepoVersionDir $version
        $encodedTarName = Get-UrlEncodedVersion $tarName
        $tarUrl = $baseUrl + $encodedTarName
        $tarPath = Join-Path $tempDir "package.tar.gz"

        Write-Host "Downloading Snowflake CLI from $tarUrl ..."
        $ProgressPreference = 'SilentlyContinue'
        Invoke-WebRequest -Uri $tarUrl -OutFile $tarPath -UseBasicParsing
        $ProgressPreference = 'Continue'
        Write-Success "Downloaded successfully"

        Write-Host "Verifying checksum..."
        if (-not (Test-Checksum $tarPath $expectedChecksum)) {
            Write-ErrorMsg "Checksum verification failed. Refusing to install."
            exit 1
        }
        Write-Success "Checksum verified"

        Write-Host "Extracting..."
        $extractDir = Join-Path $tempDir "extracted"
        New-Item -ItemType Directory -Path $extractDir -Force | Out-Null

        Push-Location $extractDir
        $ErrorActionPreference = "Continue"
        $tarOutput = & tar -xzf $tarPath 2>&1
        $ErrorActionPreference = "Stop"
        $extractedCount = (Get-ChildItem -Path $extractDir -Recurse -File -ErrorAction SilentlyContinue).Count
        if ($extractedCount -eq 0) {
            Write-ErrorMsg "Extraction failed - no files extracted"
            Write-Host $tarOutput
            Pop-Location
            exit 1
        }
        Pop-Location

        $payload = Get-PayloadDirectory $extractDir
        $binaryPath = Join-Path $payload $BinaryName
        if (-not (Test-Path $binaryPath)) {
            Write-ErrorMsg "Binary '$BinaryName' not found in extracted package"
            exit 1
        }
        Write-Success "Extracted successfully"

        $installDir = Join-Path $InstallBaseDir $version
        Remove-DirectoryBestEffort $installDir
        New-Item -ItemType Directory -Path $InstallBaseDir -Force | Out-Null
        New-Item -ItemType Directory -Path $installDir -Force | Out-Null
        Copy-Item -Path (Join-Path $payload "*") -Destination $installDir -Recurse -Force

        $previous = Read-Pointer ".current"
        if ($previous -and ($previous -ne $version)) {
            Write-Pointer ".previous" $previous
        }
        Write-Pointer ".current" $version

        $binDir = New-ManagedShims $installDir
        Write-Success "Installed Snowflake CLI v$version"

        Add-ToPath $binDir

        $installedBinary = Join-Path $installDir $BinaryName
        $cmdShim = Join-Path $binDir "snow.cmd"
        if (Test-Path $cmdShim) {
            $smoke = & $cmdShim --version 2>&1
            if ($LASTEXITCODE -ne 0 -and "$smoke" -match "GLIBC|cannot open shared object|Exec format error") {
                Write-ErrorMsg "Binary is not compatible with this system:"
                Write-Host $smoke
                exit 1
            }
        }

        Write-Host ""
        Write-Host "Installation complete!" -ForegroundColor Green
        Write-Host ""
        Write-Info "You can now use Snowflake CLI via 'snow'."
        Write-Host "Installed binary: $installedBinary" -ForegroundColor Gray
        Write-Host "Shim directory: $binDir" -ForegroundColor Gray
        Write-Host ""
        Write-Host "If you're using an IDE, restart it to apply the updated PATH." -ForegroundColor Yellow
        Write-Host ""
    }
    finally {
        Remove-DirectoryBestEffort $tempDir
    }
}

Invoke-SnowflakeCliInstall
