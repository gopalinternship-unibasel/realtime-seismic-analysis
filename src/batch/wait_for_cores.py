"""Wait until the Spark standalone cluster has free cores for a batch application, or fail clearly.

A Spark application that asks a full cluster for executors does not fail: it logs
"Initial job has not accepted any resources" every 15 seconds and waits forever. That is how
refresh_baselines.ps1 used to hang while the streaming job held every core. The compose file now
leaves cores free for the baselines job (spark.cores.max); this check turns any remaining
shortage (an older compose file, another application on the cluster) into an error message.

Usage: python src/batch/wait_for_cores.py [cores] (default SPARK_BATCH_CORES or 2)
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.request

MASTER_UI = os.getenv("SPARK_MASTER_UI", "http://spark-master:8080/json/")
TIMEOUT_SECONDS = float(os.getenv("SPARK_CORES_TIMEOUT_SECONDS", "180"))


def read_master(url: str = MASTER_UI) -> dict:
    with urllib.request.urlopen(url, timeout=10) as response:
        return json.load(response)


def free_cores(status: dict) -> int:
    """Free cores on the alive workers (the master's own total also counts dead workers)."""
    return sum(
        int(w.get("cores", 0)) - int(w.get("coresused", 0))
        for w in status.get("workers", [])
        if str(w.get("state", "")).upper() == "ALIVE"
    )


def describe(status: dict) -> str:
    apps = ", ".join(f"{a.get('name')} ({a.get('cores')} cores, {a.get('state')})" for a in status.get("activeapps", []))
    return (
        f"alive workers={status.get('aliveworkers')}, cores={status.get('cores')}, "
        f"used={status.get('coresused')}; running applications: {apps or 'none'}"
    )


def main(argv: list[str]) -> int:
    needed = int(argv[1]) if len(argv) > 1 else int(os.getenv("SPARK_BATCH_CORES", "2"))
    deadline = time.time() + TIMEOUT_SECONDS
    last = None
    while True:
        try:
            last = read_master()
            if free_cores(last) >= needed:
                print(f"Spark cluster has {free_cores(last)} free cores ({describe(last)})", flush=True)
                return 0
        except OSError as exc:  # master not up yet
            last = {"error": str(exc)}
        if time.time() >= deadline:
            detail = last.get("error") if "error" in last else describe(last)
            print(
                f"ERROR: the Spark cluster has had fewer than {needed} free cores for {TIMEOUT_SECONDS:.0f} s "
                f"({detail}). A batch job would wait forever. Stop the application holding the cores "
                "(for example: docker compose --profile app stop streaming-job) and run the job again.",
                file=sys.stderr,
                flush=True,
            )
            return 2
        time.sleep(5)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
