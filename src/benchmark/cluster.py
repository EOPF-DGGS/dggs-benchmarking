import logging
from typing import Literal
import time
from distributed import Client, LocalCluster
from dask_jobqueue import PBSCluster, SLURMCluster


job_schedulers = {
    "local": LocalCluster,
    "pbs": PBSCluster,
    "slurm": SLURMCluster,
}

logger = logging.getLogger(__name__)


def cluster_wait(client, n_workers):
    """Delay process until all workers in the cluster are available."""
    start = time.time()
    wait_thresh = 600
    worker_thresh = n_workers * 0.95

    while len(client.cluster.scheduler.workers) < n_workers:
        time.sleep(2)
        elapsed = time.time() - start
        # If we are getting close to timeout but cluster is mostly available,
        # just break out
        if elapsed > wait_thresh and len(client.cluster.scheduler.workers) >= worker_thresh:
            break


def create_client(
    job_scheduler: Literal["local", "slurm", "pbs"],
    n_workers: int,
    memory_limit: str,
    threads_per_worker: int,
    processes: bool = True,
    host: str = "localhost",
    **kwargs,
) -> Client:
    logger.info("Creating a dask client")
    logger.info(f"Job Scheduler: {job_scheduler}")
    logger.info(f"Memory size for each node: {memory_limit}")
    logger.info(f"Number of workers for each node: {n_workers}")
    logger.info(f"Number of threads for each worker: {threads_per_worker}")
    logger.info(f"Processes: {processes}")
    logger.info(f"Host: {host}")

    cluster = job_schedulers[job_scheduler](
        n_workers=n_workers,
        memory_limit=memory_limit,
        threads_per_worker=threads_per_worker,
        processes=processes,
        host=host,
    )
    client = Client(cluster)
    logger.info(f"Dask cluster dashboard_link: {client.cluster.dashboard_link}")
    # Potentially add a scaling to the cluster
    # client.cluster.scale(n_workers)
    return client
