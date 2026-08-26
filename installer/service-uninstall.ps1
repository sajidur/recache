<#
    Stops and removes the Recache Windows Service. This is meant to
    be invoked by the uninstaller (already running elevated) before
    it deletes the installed files.
#>
param(
    [string]$ServiceName = "Recache"
)

$ErrorActionPreference = "SilentlyContinue"

$existing = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue

if ($existing) {
    if ($existing.Status -ne "Stopped") {
        Stop-Service -Name $ServiceName -Force
        $existing.WaitForStatus("Stopped", (New-TimeSpan -Seconds 15))
    }

    & sc.exe delete $ServiceName | Out-Null
}
