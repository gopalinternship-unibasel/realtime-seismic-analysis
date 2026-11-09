<#
.SYNOPSIS
  Start the seismic pipeline (Kafka, Spark master + 2 workers, streaming job, PostgreSQL,
  dashboard and the chosen producers) and wait until the dashboard answers.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File .\scripts\start.ps1              # live USGS feed + replay demo
  powershell -ExecutionPolicy Bypass -File .\scripts\start.ps1 -Mode replay # replay demo only
  powershell -ExecutionPolicy Bypass -File .\scripts\start.ps1 -Reset       # wipe this project's data first
#>
param(
    [ValidateSet('both', 'live', 'replay')]
    [string]$Mode = 'both',
    # Delete this project's volumes (Kafka topic, PostgreSQL, Spark checkpoints, producer state) first.
    [switch]$Reset,
    [switch]$NoBuild,
    [int]$TimeoutSeconds = 900
)

$ErrorActionPreference = 'Continue'
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

function Invoke-Compose {
    param([string[]]$ComposeArgs)
    & docker compose @ComposeArgs
    if ($LASTEXITCODE -ne 0) { throw "docker compose $($ComposeArgs -join ' ') failed (exit $LASTEXITCODE)" }
}

& docker info --format '{{.ServerVersion}}' *> $null
if ($LASTEXITCODE -ne 0) {
    throw 'Docker is not running. Start Docker Desktop, wait until it says "Engine running", then run this script again.'
}

if (-not (Test-Path (Join-Path $root '.env'))) {
    Copy-Item (Join-Path $root '.env.example') (Join-Path $root '.env')
    Write-Host 'Created .env from .env.example'
}

$allProfiles = @('--profile', 'app', '--profile', 'replay', '--profile', 'live')
if ($Reset) {
    Write-Host 'Removing this project''s containers and volumes (cvfix-seismic_*) ...'
    Invoke-Compose ($allProfiles + @('down', '-v', '--remove-orphans'))
}

$profiles = @('--profile', 'app')
if ($Mode -eq 'both' -or $Mode -eq 'replay') { $profiles += @('--profile', 'replay') }
if ($Mode -eq 'both' -or $Mode -eq 'live') { $profiles += @('--profile', 'live') }

$up = $profiles + @('up', '-d')
if (-not $NoBuild) { $up += '--build' }
Write-Host "Starting: docker compose $($up -join ' ')"
Invoke-Compose $up

Write-Host 'Waiting for the dashboard on http://localhost:8050 ...'
$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
$dashboardUp = $false
while ((Get-Date) -lt $deadline) {
    try {
        $r = Invoke-WebRequest -Uri 'http://localhost:8050/' -UseBasicParsing -TimeoutSec 5
        if ($r.StatusCode -eq 200) { $dashboardUp = $true; break }
    } catch { }
    Start-Sleep -Seconds 3
}
if (-not $dashboardUp) { throw "The dashboard did not answer within $TimeoutSeconds s. Check: docker compose --profile app logs dashboard" }

Write-Host 'Waiting for the Spark streaming job to finish its first micro-batch ...'
Write-Host '(the first start downloads the Kafka and PostgreSQL connectors, which takes a few minutes)'
$batches = @()
$crashed = $false
while ((Get-Date) -lt $deadline) {
    # Only the log lines of the job's current run: a restarted container keeps its older logs,
    # so read them from the container's latest start (it changes if the job restarts itself).
    $jobId = (& docker compose --profile app ps -q streaming-job 2>$null | Select-Object -First 1)
    $startedAt = if ($jobId) { (& docker inspect -f '{{.State.StartedAt}}' $jobId 2>$null) } else { $null }
    if ($startedAt) {
        $log = @(& docker compose --profile app logs --no-log-prefix --since $startedAt streaming-job 2>$null)
        $batches = @($log | Where-Object { $_ -match '^Batch \d+:' })
        if ($batches.Count -gt 0) { break }
        if (@($log | Select-Object -Last 40) -match 'Traceback') { $crashed = $true; break }
    }
    Start-Sleep -Seconds 5
}
if ($batches.Count -gt 0) {
    $processed = @($batches | Where-Object { $_ -match 'received=' })
    if ($processed.Count -gt 0) {
        $processed | Select-Object -Last 3 | ForEach-Object { Write-Host "  $_" }
    } else {
        Write-Host "  $($batches[-1])  (no new events since the last run; the stored results stay on the dashboard)"
    }
} elseif ($crashed) {
    Write-Warning 'The streaming job logged an error. See: docker compose --profile app logs streaming-job'
} else {
    Write-Warning 'No micro-batch yet. Follow the job with: docker compose --profile app logs -f streaming-job'
}

Write-Host ''
Write-Host 'Seismic pipeline is running:'
Write-Host '  Dashboard          http://localhost:8050'
Write-Host '  Spark master UI    http://localhost:8080'
Write-Host '  Spark worker UIs   http://localhost:8081  http://localhost:8082'
Write-Host '  Kafka (host)       localhost:9092   topic seismic-events'
Write-Host '  PostgreSQL (host)  localhost:5432   database/user/password from .env (default seismic/seismic/seismic)'
Write-Host 'Check the pipeline:  powershell -ExecutionPolicy Bypass -File .\scripts\check.ps1'
Write-Host 'Stop everything:     powershell -ExecutionPolicy Bypass -File .\scripts\stop.ps1'
