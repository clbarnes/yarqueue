import sys

from redis import Redis

from ..queue import JoinableQueue

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 8080
DEFAULT_RHOST = "localhost"
DEFAULT_DB = 0
DEFAULT_RPORT = 6379
DEFAULT_INTERVAL = 1

DEFAULT_REDIS = Redis(DEFAULT_RHOST, DEFAULT_RPORT, db=DEFAULT_DB)


class QueueWatcher:
    def __init__(self, name, redis: Redis = DEFAULT_REDIS):
        self.name = name
        self._queue = JoinableQueue(name=name, redis=redis)

    def __len__(self):
        return sum(self.items_tasks())

    def items_tasks(self):
        return len(self._queue), self._queue.n_tasks()


def signal_handler(sig, frame):
    print("Detected interrupt, exiting", file=sys.stderr)
    sys.exit(0)
