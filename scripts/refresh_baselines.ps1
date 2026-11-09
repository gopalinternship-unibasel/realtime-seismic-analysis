<#
.SYNOPSIS
  Download the last HISTORY_DAYS (default 90) days of USGS earthquakes from the FDSN event
  service and recompute data/baseline/*.csv on the Spark cluster. If the streaming job is
  running, it is restarted afterwards so it loads the new baselines.

  The pipeline may keep running: the streaming job holds at most 4 of the cluster's 6 cores,
  and the batch job uses the other 2. If the cluster has no free cores (for example another
  Spark application holds them), the job stops with an error after 3 minutes instead of waiting.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File .\scripts\refresh_baselines.ps1
  powershell -ExecutionPolicy Bypass -File .\scripts\refresh_baselines.ps1 -NoRestart
#>
param(
    # Do not restart a running streaming job afterwards (it keeps the old baselines until its next start).
    [switch]$NoRestart
)

$ErrorActionPreference = 'Continue'
Set-Location (Split-Path -Parent $PSScriptRoot)

$started = Get-Date
& docker compose --profile batch run --rm baselines
if ($LASTEXITCODE -ne 0) { throw "Baseline job failed (exit $LASTEXITCODE)" }
Get-Content data\baseline\baseline_metadata.json
Write-Host ("Baselines recomputed in {0:N0} s" -f ((Get-Date) - $started).TotalSeconds)

$running = @(& docker compose --profile app ps --status running -q streaming-job 2>$null | Where-Object { $_ })
if ($running.Count -eq 0) {
    Write-Host 'The streaming job is not running; it loads the new baselines on its next start.'
} elseif ($NoRestart) {
    Write-Host 'Restart the streaming job to use the new baselines:  docker compose --profile app restart streaming-job'
} else {
    Write-Host 'Restarting the streaming job so it loads the new baselines (it resumes from its checkpoint) ...'
    & docker compose --profile app restart streaming-job
    if ($LASTEXITCODE -ne 0) { throw "Restarting the streaming job failed (exit $LASTEXITCODE)" }
}
