Windows: RUNNING.md describes scripts\start.ps1, check.ps1, stop.ps1 and test.ps1, which wrap
the commands below. In Git Bash, prefix commands that contain /opt/kafka/... with
MSYS_NO_PATHCONV=1.

1. SETUP AND CLEANUP COMMANDS
-----------------------------

Copy-Item .env.example .env
Creates the .env file from the example configuration file.

docker compose --profile app --profile replay --profile live down -v --remove-orphans
Stops all containers and removes this project's volumes (Kafka, PostgreSQL, checkpoints) to start cleanly.

(Spark checkpoints live in the spark-checkpoints Docker volume, so "down -v" resets them
together with Kafka and PostgreSQL. Old data\checkpoints folders must never be reused.)

docker compose ps
Shows which project containers are currently running.

docker compose logs
Shows logs from all running services.


2. MAIN DEMO START COMMANDS
---------------------------

docker compose --profile app --profile replay up --build
Runs the full demo with Kafka, Spark, PostgreSQL, dashboard, and replay producer. 

docker compose --profile app --profile live up --build
Runs the pipeline using live USGS earthquake data instead of replay data.

docker compose down
Stops all running containers but keeps volumes.

docker compose down -v --remove-orphans
Stops containers and removes stored database/checkpoint volumes for a full reset.


3. USEFUL BROWSER LINKS
-----------------------

http://localhost:8050
Opens the seismic dashboard.

http://localhost:8080
Opens the Spark Master UI.

http://localhost:8081
Opens Spark Worker 1 UI.

http://localhost:8082
Opens Spark Worker 2 UI.


4. KAFKA COMMANDS
-----------------

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
Shows all Kafka topics, including seismic-events.

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic seismic-events
Shows topic details such as partitions and replication factor.

docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic seismic-events --from-beginning --max-messages 5
Displays the first five raw earthquake event messages from Kafka.

docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic seismic-events --from-beginning
Continuously prints Kafka messages as they are consumed.


5. SPARK COMMANDS
-----------------

docker compose --profile app logs streaming-job
Shows logs from the Spark Structured Streaming application.

docker compose --profile app logs -f streaming-job
Continuously follows Spark logs while the streaming job runs.

docker compose logs spark-master
Shows logs from the Spark master container.

docker compose logs spark-worker-1
Shows logs from Spark worker 1.

docker compose logs spark-worker-2
Shows logs from Spark worker 2.


6. PRODUCER COMMANDS
--------------------

docker compose --profile app --profile replay logs replay-producer
Shows whether the replay producer is sending synthetic earthquake events.

docker compose --profile app --profile replay logs -f replay-producer
Continuously shows replay producer activity.

docker compose --profile app --profile live logs live-producer
Shows whether the live USGS producer is fetching earthquake events.


7. POSTGRESQL COMMANDS
----------------------

docker compose exec postgres psql -U seismic -d seismic
Opens an interactive PostgreSQL shell inside the database container.

docker compose exec postgres psql -U seismic -d seismic -c "SELECT COUNT(*) FROM seismic_events;"
Shows how many earthquake events Spark has stored in the database.

docker compose exec postgres psql -U seismic -d seismic -c "SELECT event_id, event_time, magnitude, place FROM seismic_events ORDER BY event_time DESC LIMIT 10;"
Displays the ten most recent processed earthquake events.

docker compose exec postgres psql -U seismic -d seismic -c "SELECT cell_id, event_count, z_score FROM anomaly_windows ORDER BY created_at DESC LIMIT 10;"
Shows recent anomalous spatial grid cells and their z-scores.

docker compose exec postgres psql -U seismic -d seismic -c "SELECT region_id, b_value, b_sigma, is_alert FROM bvalue_windows ORDER BY created_at DESC LIMIT 10;"
Shows recent Gutenberg-Richter b-value estimates.

docker compose exec postgres psql -U seismic -d seismic -c "SELECT mainshock_id, p_value, next_six_hour_expected FROM aftershock_forecasts ORDER BY fitted_at DESC LIMIT 10;"
Shows Omori-Utsu aftershock forecast results.

docker compose exec postgres psql -U seismic -d seismic -c "\\dt"
Lists all PostgreSQL tables created by the project.


8. DASHBOARD COMMANDS
---------------------

docker compose --profile app logs dashboard
Shows whether the Dash dashboard started correctly.

docker compose --profile app logs -f dashboard
Continuously shows dashboard logs.


9. HEALTH-CHECK COMMANDS
------------------------

docker compose ps
Shows whether each service is running or exited.

docker compose ps -a
Shows all containers, including stopped or failed ones.

docker compose --profile app restart dashboard
Restarts only the dashboard service.

docker compose --profile app restart streaming-job
Restarts only the Spark streaming job.


10. BEST COMMANDS TO SHOW DURING THE WORKSHOP DEMO
--------------------------------------------------

1. Start the full replay demo:

docker compose --profile app --profile replay up --build

Description:
Starts Kafka, Spark, PostgreSQL, the dashboard, and the replay producer.

2. Check running containers:

docker compose ps

Description:
Shows all running distributed services.

3. List Kafka topics:

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

Description:
Proves that Kafka has created the seismic-events topic.

4. Show raw Kafka messages:

docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic seismic-events --from-beginning --max-messages 5

Description:
Shows raw earthquake event messages flowing through Kafka.

5. Follow Spark streaming logs:

docker compose --profile app logs -f streaming-job

Description:
Shows Spark processing streaming micro-batches.

6. Count stored events:

docker compose exec postgres psql -U seismic -d seismic -c "SELECT COUNT(*) FROM seismic_events;"

Description:
Shows that processed events are stored in PostgreSQL.

7. Show anomaly detection output:

docker compose exec postgres psql -U seismic -d seismic -c "SELECT cell_id, event_count, z_score FROM anomaly_windows ORDER BY created_at DESC LIMIT 10;"

Description:
Shows anomaly detection results.

8. Show b-value analysis output:

docker compose exec postgres psql -U seismic -d seismic -c "SELECT region_id, b_value, b_sigma, is_alert FROM bvalue_windows ORDER BY created_at DESC LIMIT 10;"

Description:
Shows Gutenberg-Richter b-value analysis results.

9. Show aftershock forecast output:

docker compose exec postgres psql -U seismic -d seismic -c "SELECT mainshock_id, p_value, next_six_hour_expected FROM aftershock_forecasts ORDER BY fitted_at DESC LIMIT 10;"

Description:
Shows Omori-Utsu aftershock forecast results.

10. Open dashboard:

http://localhost:8050

Description:
Shows the final dashboard visualization.


11. FOR WORKSHOP
-----------------------------------

1. Run replay mode.
2. Show docker compose ps.
3. Show Kafka topic list.
4. Show raw Kafka messages.
5. Show Spark Master UI at http://localhost:8080.
6. Show Spark streaming logs.
7. Query PostgreSQL event count.
8. Query anomaly, b-value, and forecast tables.
9. Open the dashboard at http://localhost:8050.
10. Explain replay mode versus live mode.


12. IMPORTANT DEMO NOTE
-----------------------

Use replay mode for the workshop:

docker compose --profile app --profile replay up --build

Replay mode is recommended because it gives deterministic results and makes anomalies, b-values, and aftershock forecasts easier to show during a short presentation.

Live mode can be shown as an additional feature:

docker compose --profile app --profile live up --build

However, live mode depends on the real USGS feed, so the dashboard may look empty if there are not enough recent earthquake events.
"""
