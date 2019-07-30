import sys

from redis import Redis

from ..base_queue import BaseJoinableQueue
from ..queue import JoinableQueue, Queue

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
        if redis.exists(name + "__counter"):
            self._queue = JoinableQueue(name=name, redis=redis)
        else:
            self._queue = Queue(name=name, redis=redis)

    def __len__(self):
        return sum(self.queued_inprogress())

    def queued_inprogress(self):
        if isinstance(self._queue, BaseJoinableQueue):
            in_prog = self._queue.n_in_progress()
        else:
            in_prog = 0
        return len(self._queue), in_prog


def signal_handler(sig, frame):
    print("Detected interrupt, exiting", file=sys.stderr)
    sys.exit(0)
