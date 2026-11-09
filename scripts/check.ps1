<#
.SYNOPSIS
  Show that every stage of the pipeline works: containers, Kafka topic and messages,
  Spark cluster and streaming application, PostgreSQL tables per data source, dashboard.

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File .\scripts\check.ps1
#>
$ErrorActionPreference = 'Continue'
Set-Location (Split-Path -Parent $PSScriptRoot)
$profiles = @('--profile', 'app', '--profile', 'replay', '--profile', 'live')

$envFile = @{}
if (Test-Path .env) {
    Get-Content .env | Where-Object { $_ -match '^\s*([A-Z_]+)\s*=\s*(.*)$' } | ForEach-Object {
        $null = $_ -match '^\s*([A-Z_]+)\s*=\s*(.*)$'; $envFile[$Matches[1]] = $Matches[2].Trim()
    }
}
$dbUser = if ($envFile['POSTGRES_USER']) { $envFile['POSTGRES_USER'] } else { 'seismic' }
$dbName = if ($envFile['POSTGRES_DB']) { $envFile['POSTGRES_DB'] } else { 'seismic' }
$topic = if ($envFile['KAFKA_TOPIC']) { $envFile['KAFKA_TOPIC'] } else { 'seismic-events' }

Write-Host '== Containers'
& docker compose @profiles ps -a --format 'table {{.Service}}\t{{.Status}}'

Write-Host "`n== Kafka topic '$topic' (partition:offset = messages per partition)"
& docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:9092 --topic $topic

Write-Host "`n== Two raw Kafka messages"
& docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic $topic --from-beginning --max-messages 2 --timeout-ms 20000 2>$null

Write-Host "`n== Spark cluster (http://localhost:8080)"
try {
    $master = Invoke-RestMethod -Uri 'http://localhost:8080/json/' -TimeoutSec 10
    Write-Host ("  status={0}  alive workers={1}  cores={2}" -f $master.status, $master.aliveworkers, $master.cores)
    foreach ($app in $master.activeapps) { Write-Host ("  running application: {0} ({1}, {2} cores)" -f $app.name, $app.id, $app.cores) }
} catch { Write-Warning "Spark master UI not reachable: $_" }

Write-Host "`n== Latest streaming micro-batches (""empty"": nothing new in Kafka, or only duplicates)"
$batchLines = @((& docker compose --profile app logs --no-log-prefix streaming-job 2>$null) | Select-String -Pattern '^Batch \d+:')
$batchLines | Select-Object -Last 3 | ForEach-Object { Write-Host "  $($_.Line)" }
$withData = @($batchLines | Where-Object { $_.Line -match 'received=' })
if ($withData.Count -gt 0 -and $withData[-1].Line -ne $batchLines[-1].Line) { Write-Host "  last batch with new events: $($withData[-1].Line)" }
if ($batchLines.Count -eq 0) { Write-Host '  none yet (the first micro-batch takes a few minutes after a fresh start)' }

Write-Host "`n== PostgreSQL rows per data source"
$sql = @"
SELECT 'seismic_events' AS table_name, source, COUNT(*) AS rows FROM seismic_events GROUP BY source
UNION ALL SELECT 'anomaly_windows', source, COUNT(*) FROM anomaly_windows GROUP BY source
UNION ALL SELECT 'active_regions', source, COUNT(*) FROM active_regions GROUP BY source
UNION ALL SELECT 'bvalue_windows', source, COUNT(*) FROM bvalue_windows GROUP BY source
UNION ALL SELECT 'mainshocks', source, COUNT(*) FROM mainshocks GROUP BY source
UNION ALL SELECT 'aftershock_forecasts', m.source, COUNT(*) FROM aftershock_forecasts f JOIN mainshocks m ON m.event_id = f.mainshock_id GROUP BY m.source
ORDER BY 1, 2;
"@
& docker compose exec -T postgres psql -U $dbUser -d $dbName -c $sql

Write-Host "`n== Omori-Utsu forecasts per data source (latest forecast run; stale = active mainshock not refitted in it)"
$sql = @"
SELECT r.source, r.clock AS run_clock, r.batch_id, r.mainshocks, r.forecasts,
       (SELECT COUNT(*) FROM mainshocks m
        JOIN LATERAL (SELECT forecast_origin_hours FROM aftershock_forecasts f WHERE f.mainshock_id = m.event_id
                      ORDER BY f.fitted_at DESC, f.id DESC LIMIT 1) f ON TRUE
        WHERE m.source = r.source AND m.active AND m.larger_event_id IS NULL
          AND m.event_time + make_interval(secs => f.forecast_origin_hours * 3600) < r.clock - interval '1 second') AS stale
FROM forecast_runs r ORDER BY 1;
"@
& docker compose exec -T postgres psql -U $dbUser -d $dbName -c $sql

Write-Host "`n== Dashboard"
try {
    $r = Invoke-WebRequest -Uri 'http://localhost:8050/' -UseBasicParsing -TimeoutSec 10
    Write-Host "  http://localhost:8050 -> HTTP $($r.StatusCode)"
} catch { Write-Warning "Dashboard not reachable: $_" }
