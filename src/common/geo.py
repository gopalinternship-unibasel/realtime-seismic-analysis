import math
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def millis_to_iso(ms: Optional[int]) -> Optional[str]:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat()


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def grid_cell(lon: Optional[float], lat: Optional[float]) -> tuple[Optional[int], Optional[int], Optional[str]]:
    if lon is None or lat is None:
        return None, None, None
    grid_lon = math.floor(lon)
    grid_lat = math.floor(lat)
    return grid_lon, grid_lat, f"{grid_lon}:{grid_lat}"


def coarse_region(lon: Optional[float], lat: Optional[float], width: int = 10) -> Optional[str]:
    if lon is None or lat is None:
        return None
    rg_lon = int(math.floor(lon / width) * width)
    rg_lat = int(math.floor(lat / width) * width)
    return f"R{rg_lon:+04d}_{rg_lat:+03d}"


def haversine_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    radius = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return 2 * radius * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def lonlat_to_unit_vector(lon: float, lat: float) -> tuple[float, float, float]:
    """Map a lon/lat point to a 3-D unit vector so clustering ignores the dateline seam."""
    lam, phi = math.radians(lon), math.radians(lat)
    return math.cos(phi) * math.cos(lam), math.cos(phi) * math.sin(lam), math.sin(phi)


def unit_vector_to_lonlat(x: float, y: float, z: float) -> tuple[float, float]:
    lon = math.degrees(math.atan2(y, x))
    lat = math.degrees(math.atan2(z, math.sqrt(x * x + y * y)))
    return lon, lat


def is_valid_coordinate(lon: Optional[float], lat: Optional[float]) -> bool:
    return lon is not None and lat is not None and -180.0 <= lon <= 180.0 and -90.0 <= lat <= 90.0


def flatten_usgs_feature(feature: Dict[str, Any], source: str = "usgs-live") -> Optional[Dict[str, Any]]:
    props = feature.get("properties") or {}
    geom = feature.get("geometry") or {}
    coords = geom.get("coordinates") or []
    if len(coords) < 2:
        return None

    lon = safe_float(coords[0])
    lat = safe_float(coords[1])
    depth = safe_float(coords[2]) if len(coords) > 2 else None
    event_id = feature.get("id") or props.get("code")
    event_time = millis_to_iso(props.get("time"))
    if not event_id or not event_time or not is_valid_coordinate(lon, lat):
        return None

    grid_lon, grid_lat, cell_id = grid_cell(lon, lat)
    return {
        "event_id": str(event_id),
        "event_time": event_time,
        "ingestion_time": datetime.now(tz=timezone.utc).isoformat(),
        "magnitude": safe_float(props.get("mag")),
        "depth_km": depth,
        "longitude": lon,
        "latitude": lat,
        "place": props.get("place"),
        "event_type": props.get("type"),
        "status": props.get("status"),
        "tsunami": safe_int(props.get("tsunami")),
        "significance": safe_int(props.get("sig")),
        "station_count": safe_int(props.get("nst")),
        "rms": safe_float(props.get("rms")),
        "azimuthal_gap": safe_float(props.get("gap")),
        "felt_count": safe_int(props.get("felt")),
        "url": props.get("url"),
        "grid_lon": grid_lon,
        "grid_lat": grid_lat,
        "cell_id": cell_id,
        "region_id": coarse_region(lon, lat),
        "source": source,
    }
