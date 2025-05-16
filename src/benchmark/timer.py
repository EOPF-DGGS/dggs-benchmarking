from contextlib import contextmanager
import time

import pandas as pd


class DiagnosticTimer:
    def __init__(self):
        self.diagnostics = []

    @contextmanager
    def time(self, **kwargs):
        tic = time.perf_counter()
        yield
        toc = time.perf_counter()
        kwargs["runtime"] = toc - tic
        self.diagnostics.append(kwargs)

    def dataframe(self):
        return pd.DataFrame(self.diagnostics)


def cluster_wait(client, n_workers: int):
    """Delay process until all workers in the cluster are available."""
    start = time.perf_counter()
    wait_thresh = 600
    worker_thresh = n_workers * 0.95

    while len(client.cluster.scheduler.workers) < n_workers:
        time.sleep(2)
        elapsed = time.perf_counter() - start
        # If we are getting close to timeout but cluster is mostly available,
        # just break out
        if elapsed > wait_thresh and len(client.cluster.scheduler.workers) >= worker_thresh:
            break
