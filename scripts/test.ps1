<#
.SYNOPSIS
  Run the automated tests inside the project's Docker images (no local Python or Spark needed).
  Pipeline tests use a local SparkSession in the Spark image; dashboard tests use the dashboard image.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File .\scripts\test.ps1
#>
$ErrorActionPreference = 'Continue'
Set-Location (Split-Path -Parent $PSScriptRoot)

& docker compose --profile test build tests dashboard-tests
if ($LASTEXITCODE -ne 0) { throw 'Building the test images failed' }

& docker compose --profile test run --rm --no-deps dashboard-tests
$dashboard = $LASTEXITCODE
& docker compose --profile test run --rm --no-deps tests
$pipeline = $LASTEXITCODE

Write-Host ''
Write-Host ("dashboard tests: {0}   pipeline tests: {1}" -f ($(if ($dashboard -eq 0) { 'passed' } else { 'FAILED' })), ($(if ($pipeline -eq 0) { 'passed' } else { 'FAILED' })))
if ($dashboard -ne 0 -or $pipeline -ne 0) { exit 1 }
