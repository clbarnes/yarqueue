from functools import wraps
from typing import Optional, Iterator
from queue import Empty, Full
from datetime import datetime, timedelta
import time

from redis import Redis

from yarqueue.constants import DEFAULT_SERIALIZER, POLL_INTERVAL, Side
from .base_queue import BaseQueue, BaseJoinableQueue
from yarqueue.serializer import BaseSerializer


def _redislite():
    try:
        import redislite
    except ImportError:
        raise ValueError

    return redislite.Redis()


class Queue(BaseQueue):
    _put_side = Side.RIGHT
    _get_side = Side.LEFT

    def __init__(
        self,
        name,
        maxsize=0,
        redis: Optional[Redis] = None,
        serializer: Optional[BaseSerializer] = DEFAULT_SERIALIZER,
    ):
        super().__init__(maxsize)
        self.name = name
        self._redis = self._ensure_redis(redis)
        self._serializer = serializer

    def _ensure_redis(self, redis: Optional[Redis]):
        if redis:
            return redis

        try:
            import redislite

            return redislite.Redis()
        except ImportError:
            raise ValueError(
                "Redis instance not given and redislite not importable. Run\n"
                "pip install redislite"
            )

    def qsize(self) -> int:
        return len(self)

    def __len__(self):
        return self._redis.llen(self.name)

    def put(self, obj, block=True, timeout=None) -> None:
        self._put(self._put_side, obj, block, timeout)

    def _put(self, side: Side, obj, block=True, timeout=None) -> None:
        if self._serializer:
            obj = self._serializer.dumps(obj)

        maxsize = self.maxsize or float("inf")

        push_method = getattr(self._redis, side + "push")

        if not block:
            if self.full():
                raise Full()
            else:
                push_method(self.name, obj)
                return

        started = datetime.utcnow()
        while len(self) >= maxsize:
            time.sleep(POLL_INTERVAL)
            if timeout is not None:
                elapsed: timedelta = datetime.utcnow() - started
                if elapsed.total_seconds() >= timeout:
                    raise Full()

        push_method(self.name, obj)

    def get(self, block=True, timeout=None) -> object:
        return self._get(self._get_side, block, timeout)

    def _get(self, side: Side, block=True, timeout=None) -> object:
        if block:
            timeout = timeout or 0
            msg = getattr(self._redis, f"b{side}pop")(self.name, timeout=timeout)
        else:
            msg = getattr(self._redis, side + "pop")(self.name)

        if msg is None:
            raise Empty()
        else:
            item = msg[1]

        if self._serializer:
            return self._serializer.loads(item)

        return item

    def take(self, max_items=None, block=True, timeout=None) -> Iterator:
        max_items = max_items or float("inf")
        count = 0
        while count < max_items:
            yield self.get(block, timeout)


FifoQueue = Queue


class JoinableQueue(Queue, BaseJoinableQueue):
    @wraps(Queue.__init__)
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._counter_name = self.name + "__counter"
        self._redis.set(self._counter_name, 0)

    @wraps(Queue._put)
    def _put(self, *args, **kwargs):
        self._redis.incr(self._counter_name)
        super()._put(*args, **kwargs)

    def n_tasks(self) -> int:
        return int(self._redis.get(self._counter_name) or 0)

    def task_done(self) -> None:
        self._redis.decr(self._counter_name)


JoinableFifoQueue = JoinableQueue


class LifoQueue(Queue):
    _get_side = Side.RIGHT


class JoinableLifoQueue(JoinableQueue):
    _get_side = Side.RIGHT


class DeQueue(Queue):
    def put_left(self, obj, block=True, timeout=None) -> None:
        self._put(Side.LEFT, obj, block, timeout)

    def put_right(self, obj, block=True, timeout=None) -> None:
        self._put(Side.RIGHT, obj, block, timeout)

    def get_left(self, block=True, timeout=None) -> object:
        return self._get(Side.LEFT, block, timeout)

    def get_right(self, block=True, timeout=None) -> object:
        return self._get(Side.RIGHT, block, timeout)


class JoinableDeQueue(JoinableQueue, DeQueue):
    pass
