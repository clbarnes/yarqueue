import signal
import sys
import time
from itertools import zip_longest
from typing import Dict, Optional

from redis import Redis

try:
    from tqdm import tqdm
except ImportError:
    raise ImportError(
        "tqdm not importable; CLI watcher not available. pip install tqdm"
    )

try:
    import click
except ImportError:
    raise ImportError(
        "click not importable; CLI watcher not available. pip install click"
    )

from .. import __version__
from .common import (
    QueueWatcher,
    signal_handler,
    DEFAULT_INTERVAL,
    DEFAULT_RHOST,
    DEFAULT_RPORT,
    DEFAULT_DB,
)


class TqdmQueueWatcher:
    def __init__(self, watcher: QueueWatcher, total=None):
        self.watcher = watcher
        len_now = len(self.watcher)
        self.total = total or len_now
        self.previous_done = self.total - len_now

        self.pbar = tqdm(desc=watcher.name, total=self.total)
        self.pbar.update(self.previous_done)

    def n_done(self):
        return self.total - len(self.watcher)

    def update(self):
        done = self.n_done()
        diff = done - self.previous_done
        self.previous_done = done
        self.pbar.update(diff)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.pbar.close()


class MultiTqdm:
    def __init__(self, redis, names_totals: Dict[str, Optional[int]]):
        self.tqdm_watcher = [
            TqdmQueueWatcher(QueueWatcher(name, redis), total)
            for name, total in names_totals.items()
        ]

    def __iter__(self):
        yield from self.tqdm_watcher

    def update(self):
        for tw in self:
            tw.update()

    def close(self):
        for tw in self:
            tw.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def main(redis, names_totals, interval=1):
    signal.signal(signal.SIGINT, signal_handler)
    print("Press Ctrl+C to exit", file=sys.stderr)
    with MultiTqdm(redis, names_totals) as mt:
        while True:
            mt.update()
            time.sleep(interval)


@click.command(
    help="Watch the progress of a number of redis-backed queues, on the command line."
)
@click.version_option(version=__version__)
@click.help_option()
@click.option(
    "--name",
    "-n",
    multiple=True,
    help="Name of redis lists to watch (accepts multiple)",
)
@click.option(
    "--total",
    "-t",
    multiple=True,
    type=int,
    help="Total items added to the queue (accepts multiple, same order as --name)",
)
@click.option(
    "--interval",
    "-i",
    default=DEFAULT_INTERVAL,
    type=float,
    help="Polling interval (seconds)",
    show_default=True,
)
@click.option(
    "--host",
    default=DEFAULT_RHOST,
    help="Hostname for the Redis instance",
    show_default=True,
)
@click.option(
    "--port",
    default=DEFAULT_RPORT,
    type=int,
    help="Port for the Redis instance",
    show_default=True,
)
@click.option(
    "--db",
    default=DEFAULT_DB,
    type=int,
    help="DB ID for the Redis instance",
    show_default=True,
)
@click.option("--password", help="Password for the Redis instance", show_default=True)
def yarqwatch(name, total, interval, host, port, db, password):
    redis = Redis(host, port, db, password)
    names_totals = dict(zip_longest(name, total))
    main(redis, names_totals, interval)


if __name__ == "__main__":
    if not click:
        raise ImportError("click is not installed; yarqwatch unavailable")
    if not tqdm:
        raise ImportError("tqdm is not installed; yarqwatch unavailable")
    yarqwatch()
