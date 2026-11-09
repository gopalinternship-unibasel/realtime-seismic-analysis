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
            f"user={self.user} password={self.password} options='-c TimeZone=UTC'"
        )

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.db}"


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic: str = os.getenv("KAFKA_TOPIC", "seismic-events")
    starting_offsets: str = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")


@dataclass(frozen=True)
class StreamConfig:
    checkpoint_dir: str = os.getenv("CHECKPOINT_DIR", "/opt/workspace/checkpoints/seismic_stream")
    baseline_csv: str = os.getenv("BASELINE_CSV", "/opt/workspace/data/baseline/cell_baselines.csv")
    region_bvalue_csv: str = os.getenv("REGION_BVALUE_CSV", "/opt/workspace/data/baseline/region_bvalues.csv")
    z_threshold: float = float(os.getenv("Z_THRESHOLD", "2.0"))
    min_events_for_anomaly: int = int(os.getenv("MIN_EVENTS_FOR_ANOMALY", "3"))
    max_kmeans_clusters: int = int(os.getenv("MAX_KMEANS_CLUSTERS", "5"))
    # Anomalous cells whose windows ended within this many hours of the latest event form the active set.
    active_region_hours: int = int(os.getenv("ACTIVE_REGION_HOURS", "24"))
    # K-Means grows k until every anomalous cell is within this distance of its region centroid.
    region_radius_km: float = float(os.getenv("REGION_RADIUS_KM", "300"))
    # A 24-hour b-value needs at least this many magnitudes at or above Mc.
    bvalue_min_events: int = int(os.getenv("BVALUE_MIN_EVENTS", "20"))
    mainshock_magnitude: float = float(os.getenv("MAINSHOCK_MAGNITUDE", "5.0"))
    mainshock_active_days: int = int(os.getenv("MAINSHOCK_ACTIVE_DAYS", "30"))
