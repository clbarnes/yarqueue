from functools import wraps
from typing import Optional, Iterator, Iterable
from queue import Empty, Full
from datetime import datetime
import time

from redis import Redis

from yarqueue.constants import DEFAULT_SERIALIZER, POLL_INTERVAL, Side
from .base_queue import BaseQueue, BaseJoinableQueue
from yarqueue.serializer import BaseSerializer


def _ensure_redis(redis: Optional[Redis]):
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
        self._redis = _ensure_redis(redis)
        self._serializer = serializer

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
                elapsed = datetime.utcnow() - started
                if elapsed.total_seconds() >= timeout:
                    raise Full()

        push_method(self.name, obj)

    def get(self, block=True, timeout=None) -> object:
        return self._get(self._get_side, block, timeout)

    def _get(self, side: Side, block=True, timeout=None) -> object:
        if block:
            timeout = timeout or 0
            msg = getattr(self._redis, "b{}pop".format(side))(self.name, timeout=timeout)
            if msg is not None:
                msg = msg[1]
        else:
            msg = getattr(self._redis, side + "pop")(self.name)

        if msg is None:
            raise Empty()

        if self._serializer:
            msg = self._serializer.loads(msg)

        return msg

    def take(self, n_items, block=True, timeout=None) -> Iterator:
        """Yield items from the queue.

        :param n_items: how many items to get (can be ``float("inf")``)
        :param block: as used in ``get``
        :param timeout: timeout as used in ``get``, per item fetched
        """
        count = 0
        while count < n_items:
            yield self.get(block, timeout)
            count += 1

    def _put_many(self, side: Side, objs: Iterable, block=True, timeout=None):
        if self._serializer:
            objs = [self._serializer.dumps(obj) for obj in objs]

        maxsize = self.maxsize or float("inf")

        push_method = getattr(self._redis, side + "push")

        if not block:
            if len(objs) > maxsize - len(self):
                raise Full()
            else:
                push_method(self.name, *objs)
                return

        started = datetime.utcnow()
        while len(objs) > maxsize - len(self):
            time.sleep(POLL_INTERVAL)
            if timeout is not None:
                elapsed = datetime.utcnow() - started
                if elapsed.total_seconds() >= timeout:
                    raise Full()

        push_method(self.name, *objs)

    def put_many(self, objs: Iterable, block=True, timeout=None):
        """Put multiple items on the queue at once.

        May be faster than individual calls.
        However, if the queue has a maxsize, no items will be put on until all of them
        can be.

        :param objs: iterable of objects to add. Must be finite.
        :param block: as used in ``put``
        :param timeout: how long to wait, in seconds, for *all* items to be added
            together
        """
        self._put_many(self._put_side, objs, block, timeout)

    def clear(self):
        self._redis.delete(self.name)

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.clear()


FifoQueue = Queue


class JoinableQueue(Queue, BaseJoinableQueue):
    @wraps(Queue.__init__)
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._counter_name = self.name + "__counter"
        self._redis.set(self._counter_name, 0)

    @wraps(Queue._put)
    def _put(self, *args, **kwargs):
        # possible race condition
        super()._put(*args, **kwargs)
        self._redis.incr(self._counter_name)

    @wraps(Queue._put_many)
    def _put_many(self, side, objs, *args, **kwargs):
        try:
            length = len(objs)
        except TypeError:
            objs = list(objs)
            length = len(objs)

        # possible race condition
        super()._put_many(side, objs, *args, **kwargs)
        self._redis.incr(self._counter_name, length)

    def n_tasks(self) -> int:
        return int(self._redis.get(self._counter_name) or 0)

    def task_done(self) -> None:
        self._redis.decr(self._counter_name)

    def clear(self):
        self._redis.delete(self._counter_name)
        super().clear()

    def __exit__(self, type_, value, traceback):
        self.join()
        return super().__exit__(type_, value, traceback)


JoinableFifoQueue = JoinableQueue


class LifoQueue(Queue):
    _get_side = Side.RIGHT


class JoinableLifoQueue(JoinableQueue):
    _get_side = Side.RIGHT


class DeQueue(Queue):
    def put_left(self, obj, block=True, timeout=None) -> None:
        self._put(Side.LEFT, obj, block, timeout)

    def put_many_left(self, objs: Iterable, block=True, timeout=None):
        self._put_many(Side.LEFT, objs, block, timeout)

    def put_right(self, obj, block=True, timeout=None) -> None:
        self._put(Side.RIGHT, obj, block, timeout)

    def put_many_right(self, objs: Iterable, block=True, timeout=None):
        self._put_many(Side.RIGHT, objs, block, timeout)

    def get_left(self, block=True, timeout=None) -> object:
        return self._get(Side.LEFT, block, timeout)

    def get_right(self, block=True, timeout=None) -> object:
        return self._get(Side.RIGHT, block, timeout)


class JoinableDeQueue(JoinableQueue, DeQueue):
    pass
