import toml

from benchmark import run_benchmark


def test_local_cluster(tmp_path):
    config = {
        "log_level": "INFO",
        "output_dir": str(tmp_path / "results"),
        "job_scheduler": "local",
        "queue": "batch",
        "format": "latlon",
        "walltime": 3600,
        "maxcore_per_node": 2,
        "cluster": {
            "job_scheduler": "local",
            "n_workers": 2,
            "memory_limit": "1gb",
            "threads_per_worker": 1,
        },
        "data": {
            "chunk_size": "50MB",
            "chunking_scheme": "auto",
        },
        "operation": "temporal_mean",
    }
    config_file = tmp_path / "local.toml"
    config_file.write_text(toml.dumps(config))
    run_benchmark(config_file)
