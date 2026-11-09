from src.batch import wait_for_cores

# Shape of http://spark-master:8080/json/ while the streaming job runs (2 workers x 3 cores, 4 used).
STATUS = {
    "aliveworkers": 2,
    "cores": 6,
    "coresused": 4,
    "workers": [
        {"id": "w1", "cores": 3, "coresused": 2, "state": "ALIVE"},
        {"id": "w2", "cores": 3, "coresused": 2, "state": "ALIVE"},
        {"id": "w0", "cores": 3, "coresused": 0, "state": "DEAD"},
    ],
    "activeapps": [{"name": "RealTimeSeismicActivityAnalysis", "cores": 4, "state": "RUNNING"}],
}


def test_free_cores_counts_alive_workers_only():
    assert wait_for_cores.free_cores(STATUS) == 2
    full = {**STATUS, "workers": [{**w, "coresused": w["cores"]} for w in STATUS["workers"][:2]]}
    assert wait_for_cores.free_cores(full) == 0
    assert "RealTimeSeismicActivityAnalysis (4 cores, RUNNING)" in wait_for_cores.describe(STATUS)


def test_fails_instead_of_waiting_forever_when_the_cluster_is_full(monkeypatch, capsys):
    full = {**STATUS, "workers": [{**w, "coresused": w["cores"]} for w in STATUS["workers"][:2]]}
    monkeypatch.setattr(wait_for_cores, "read_master", lambda url=None: full)
    monkeypatch.setattr(wait_for_cores, "TIMEOUT_SECONDS", 0.0)
    monkeypatch.setattr(wait_for_cores.time, "sleep", lambda s: None)
    assert wait_for_cores.main(["wait_for_cores.py", "2"]) == 2
    assert "fewer than 2 free cores" in capsys.readouterr().err
    monkeypatch.setattr(wait_for_cores, "read_master", lambda url=None: STATUS)
    assert wait_for_cores.main(["wait_for_cores.py", "2"]) == 0
