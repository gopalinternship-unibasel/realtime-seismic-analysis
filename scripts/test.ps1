<#
.SYNOPSIS
  Run the automated tests inside the project's Docker images (no local Python or Spark needed).
  Pipeline tests use a local SparkSession in the Spark image and a throwaway PostgreSQL
  (test-postgres, data in memory) for the forecasting tests; dashboard tests use the dashboard image.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File .\scripts\test.ps1
#>
$ErrorActionPreference = 'Continue'
Set-Location (Split-Path -Parent $PSScriptRoot)

& docker compose --profile test build tests dashboard-tests
if ($LASTEXITCODE -ne 0) { throw 'Building the test images failed' }

& docker compose --profile test up -d --wait test-postgres
if ($LASTEXITCODE -ne 0) { throw 'Starting the test database failed' }

& docker compose --profile test run --rm --no-deps dashboard-tests
$dashboard = $LASTEXITCODE
& docker compose --profile test run --rm --no-deps -e TEST_POSTGRES_HOST=test-postgres -e REQUIRE_TEST_DB=1 tests
$pipeline = $LASTEXITCODE

# The test database keeps nothing (tmpfs); remove its container.
& docker compose --profile test rm -s -f test-postgres *> $null

Write-Host ''
Write-Host ("dashboard tests: {0}   pipeline tests: {1}" -f ($(if ($dashboard -eq 0) { 'passed' } else { 'FAILED' })), ($(if ($pipeline -eq 0) { 'passed' } else { 'FAILED' })))
if ($dashboard -ne 0 -or $pipeline -ne 0) { exit 1 }
