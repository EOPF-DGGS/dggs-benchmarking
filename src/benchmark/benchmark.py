from typing import Any
import logging
from pathlib import Path
import toml
import datetime

from dask.utils import format_bytes

from .cluster import create_client
from .timer import DiagnosticTimer
from .operations import OPERATIONS
from .data import create_dataset

logger = logging.getLogger(__name__)


def validate_config(config: dict[str, Any]) -> None:
    """Validate the configuration dictionary."""
    # Potentially return a Pydantic model or a TypedDict
    pass


def run_benchmark(config_file: Path) -> None:
    config = toml.loads(config_file.read_text())
    validate_config(config)

    # TODO: Add some file handlers to that logs go to a file
    logging.basicConfig(
        level=config.get("log_level", "INFO"),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    main_output_dir = Path(config.get("output_dir", "results"))
    now = datetime.datetime.now()
    output_dir = main_output_dir / config["job_scheduler"] / now.isoformat()
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "config.toml").write_text(toml.dumps(config))

    client = create_client(**config["cluster"])
    timer = DiagnosticTimer()
    ds = create_dataset(
        **config["data"],
    )
    dataset_size = format_bytes(ds.nbytes)
    logger.info(ds)
    logger.info(f"Dataset total size: {dataset_size}")
    with timer.time(**config):
        res = OPERATIONS[config["operation"]](ds).persist()

    client.cancel(ds)

    timing_df = timer.dataframe()
    timing_df.to_csv(output_dir / "timings.csv", index=False)
    res.to_zarr(
        output_dir / "results.zarr",
    )
    logger.info(f"Results saved to {output_dir / 'results.zarr'}")
