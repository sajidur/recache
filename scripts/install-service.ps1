#Requires -RunAsAdministrator
<#
    Publishes recache and installs it as a Windows Service.
    Run this from an elevated PowerShell prompt:

        .\scripts\install-service.ps1

    Re-run it any time to redeploy (it stops, republishes, and
    restarts the existing service if one is already installed).
#>
param(
    [string]$ServiceName = "Recache",
    [string]$DisplayName = "Recache Cache Service",
    [string]$Configuration = "Release"
)

$ErrorActionPreference = "Stop"

$repoRoot   = Split-Path -Parent $PSScriptRoot
$project    = Join-Path $repoRoot "recache\recache.csproj"
$publishDir = Join-Path $repoRoot "recache\publish"
$exePath    = Join-Path $publishDir "recache.exe"

Write-Host "Publishing $project ($Configuration) to $publishDir ..."
dotnet publish $project -c $Configuration -o $publishDir
if ($LASTEXITCODE -ne 0) {
    throw "dotnet publish failed with exit code $LASTEXITCODE"
}

if (-not (Test-Path $exePath)) {
    throw "Publish succeeded but '$exePath' was not found."
}

$existing = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue

if ($existing) {
    Write-Host "Service '$ServiceName' already exists - stopping it before redeploying."
    if ($existing.Status -ne "Stopped") {
        Stop-Service -Name $ServiceName -Force
        $existing.WaitForStatus("Stopped", (New-TimeSpan -Seconds 15))
    }
}
else {
    # sc.exe requires the binPath value to already contain literal
    # quotes around any path with spaces.
    $quotedExePath = '"' + $exePath + '"'

    Write-Host "Creating service '$ServiceName' -> $exePath"
    & sc.exe create $ServiceName binPath= $quotedExePath start= auto DisplayName= $DisplayName | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "sc.exe create failed with exit code $LASTEXITCODE"
    }

    & sc.exe description $ServiceName "In-memory Redis-compatible cache server (recache)." | Out-Null

    # Auto-restart on crash: after 5s, 5s, then 5s, resetting the
    # failure count after a day of stable uptime.
    & sc.exe failure $ServiceName reset= 86400 actions= restart/5000/restart/5000/restart/5000 | Out-Null
}

Write-Host "Starting service '$ServiceName' ..."
Start-Service -Name $ServiceName

$status = (Get-Service -Name $ServiceName).Status
Write-Host "Service '$ServiceName' is now: $status"
Write-Host "Config file: $publishDir\appsettings.yaml"
Write-Host ""
Write-Host "To uninstall: .\scripts\uninstall-service.ps1"
