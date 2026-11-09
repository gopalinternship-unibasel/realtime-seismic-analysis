<#
.SYNOPSIS
  Stop every container of this project. Data (Kafka topic, PostgreSQL, Spark checkpoints)
  is kept, so the next start.ps1 resumes where the pipeline stopped.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File .\scripts\stop.ps1          # stop, keep data
  powershell -ExecutionPolicy Bypass -File .\scripts\stop.ps1 -Reset   # stop and delete this project's volumes
#>
param([switch]$Reset)

$ErrorActionPreference = 'Continue'
Set-Location (Split-Path -Parent $PSScriptRoot)

$allProfiles = @('--profile', 'app', '--profile', 'replay', '--profile', 'live', '--profile', 'batch', '--profile', 'test')
if ($Reset) {
    & docker compose @allProfiles down -v --remove-orphans
} else {
    & docker compose @allProfiles stop
}
if ($LASTEXITCODE -ne 0) { throw "docker compose failed (exit $LASTEXITCODE)" }
& docker compose @allProfiles ps -a --format 'table {{.Service}}\t{{.Status}}'
