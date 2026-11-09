import os
from dataclasses import dataclass


@dataclass(frozen=True)
class PostgresConfig:
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    db: str = os.getenv("POSTGRES_DB", "seismic")
    user: str = os.getenv("POSTGRES_USER", "seismic")
    password: str = os.getenv("POSTGRES_PASSWORD", "seismic")

    @property
    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.db} "
            f"user={self.user} password={self.password}"
        )


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic: str = os.getenv("KAFKA_TOPIC", "seismic-events")
    starting_offsets: str = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")


@dataclass(frozen=True)
class StreamConfig:
    checkpoint_dir: str = os.getenv("CHECKPOINT_DIR", "/opt/workspace/data/checkpoints/seismic_stream")
    baseline_csv: str = os.getenv("BASELINE_CSV", "/opt/workspace/data/baseline/cell_baselines.csv")
    z_threshold: float = float(os.getenv("Z_THRESHOLD", "2.0"))
    min_events_for_anomaly: int = int(os.getenv("MIN_EVENTS_FOR_ANOMALY", "3"))
    max_kmeans_clusters: int = int(os.getenv("MAX_KMEANS_CLUSTERS", "5"))
