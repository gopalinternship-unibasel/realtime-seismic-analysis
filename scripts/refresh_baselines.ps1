<#
.SYNOPSIS
  Download the last HISTORY_DAYS (default 90) days of USGS earthquakes from the FDSN event
  service and recompute data/baseline/*.csv on the Spark cluster. Restart the streaming job
  afterwards so it loads the new baselines.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File .\scripts\refresh_baselines.ps1
#>
$ErrorActionPreference = 'Continue'
Set-Location (Split-Path -Parent $PSScriptRoot)

& docker compose --profile batch run --rm baselines
if ($LASTEXITCODE -ne 0) { throw "Baseline job failed (exit $LASTEXITCODE)" }
Get-Content data\baseline\baseline_metadata.json
Write-Host 'Restart the streaming job to use the new baselines:  docker compose --profile app restart streaming-job'
