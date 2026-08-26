#Requires -RunAsAdministrator
<#
    Stops and removes the recache Windows Service.
    Run this from an elevated PowerShell prompt:

        .\scripts\uninstall-service.ps1
#>
param(
    [string]$ServiceName = "Recache"
)

$ErrorActionPreference = "Stop"

$existing = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue

if (-not $existing) {
    Write-Host "Service '$ServiceName' is not installed - nothing to do."
    exit 0
}

if ($existing.Status -ne "Stopped") {
    Write-Host "Stopping service '$ServiceName' ..."
    Stop-Service -Name $ServiceName -Force
    $existing.WaitForStatus("Stopped", (New-TimeSpan -Seconds 15))
}

Write-Host "Removing service '$ServiceName' ..."
& sc.exe delete $ServiceName | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "sc.exe delete failed with exit code $LASTEXITCODE"
}

Write-Host "Service '$ServiceName' removed."
