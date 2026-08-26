<#
    Registers and starts the Recache Windows Service. This is meant
    to be invoked by the installer (already running elevated) right
    after it copies files into place - run it manually only if you
    know what you're doing.
#>
param(
    [string]$ServiceName = "Recache",
    [string]$DisplayName = "Recache Cache Service"
)

$ErrorActionPreference = "Stop"

$exePath = Join-Path $PSScriptRoot "recache.exe"

if (-not (Test-Path $exePath)) {
    Write-Error "recache.exe not found next to this script at '$exePath'."
    exit 1
}

$existing = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue

if ($existing) {
    if ($existing.Status -ne "Stopped") {
        Stop-Service -Name $ServiceName -Force
        $existing.WaitForStatus("Stopped", (New-TimeSpan -Seconds 15))
    }

    & sc.exe delete $ServiceName | Out-Null
    Start-Sleep -Seconds 1
}

# sc.exe requires the binPath value to already contain literal
# quotes around any path with spaces (e.g. "C:\Program Files\...").
$quotedExePath = '"' + $exePath + '"'

& sc.exe create $ServiceName binPath= $quotedExePath start= auto DisplayName= $DisplayName | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw "sc.exe create failed with exit code $LASTEXITCODE"
}

& sc.exe description $ServiceName "In-memory Redis-compatible cache server (recache)." | Out-Null

# Auto-restart on crash: after 5s, 5s, then 5s, resetting the
# failure count after a day of stable uptime.
& sc.exe failure $ServiceName reset= 86400 actions= restart/5000/restart/5000/restart/5000 | Out-Null

Start-Service -Name $ServiceName
