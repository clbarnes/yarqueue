import uuid
from functools import wraps
from typing import Optional, Iterator, Iterable
from queue import Empty, Full
from datetime import datetime
import time

from redis import Redis

from .constants import DEFAULT_SERIALIZER, POLL_INTERVAL, Side
from .base_queue import BaseQueue, BaseJoinableQueue
from .serializer import BaseSerializer


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
    """Redis-backed first-in, first-out queue compatible with multiprocessing.Queue.

    Additionally, contains a ``put_many()`` method for adding several items to the redis
    list at once; a ``get_many()`` method which yields a given number of items as they
    become available, and a ``clear()`` method to empty and delete the underlying list.
    """

    _put_side = Side.RIGHT
    _get_side = Side.LEFT

    def __init__(
        self,
        maxsize=0,
        name: Optional[str] = None,
        redis: Optional[Redis] = None,
        serializer: Optional[BaseSerializer] = DEFAULT_SERIALIZER,
    ):
        """
        :param maxsize: optional maximum number of items to allow in the queue
        :param name: name to use for the underlying redis list. Not mangled. If empty,
            will generate a unique identifier (UUID4).
        :param redis: redis client instance. If ``None`` (default),
            will attempt to start a redislite instance. If a redislite instance is
            created, other processes may not be able to connect to it easily.
        :param serializer: anything which can ``dumps`` an item to bytes, and ``loads``
            it back again. By default, uses a wrapper around ``pickle`` which uses the
            highest pickle protocol available, possibly with the backported ``pickle5``
            library.
        """
        super().__init__(maxsize)
        self.name = name or str(uuid.uuid4())
        self._redis = _ensure_redis(redis)
        self._serializer = serializer

    def qsize(self) -> int:
        return len(self)

    def __len__(self) -> int:
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
            msg = getattr(self._redis, "b{}pop".format(side))(
                self.name, timeout=timeout
            )
            if msg is not None:
                msg = msg[1]
        else:
            msg = getattr(self._redis, side + "pop")(self.name)

        if msg is None:
            raise Empty()

        if self._serializer:
            msg = self._serializer.loads(msg)

        return msg

    def get_many(self, n_items: int, block=True, timeout=None) -> Iterator:
        """Yield items from the queue.

        :param n_items: how many items to get (can be ``float("inf")``)
        :param block: as used in ``get``
        :param timeout: timeout as used in ``get``, per item fetched
        """
        yield from self._get_many(self._get_side, n_items, block, timeout)

    def _get_many(self, side: Side, n_items: int, block, timeout) -> Iterator:
        count = 0
        while count < n_items:
            yield self._get(side, block, timeout)
            count += 1

    def _put_many(self, side: Side, objs: Iterable, block, timeout):
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
        """Empty and delete the underlying Redis list"""
        self._redis.delete(self.name)

    def __iter__(self) -> Iterator:
        while True:
            try:
                yield self.get_nowait()
            except Empty:
                return

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.clear()


FifoQueue = Queue


class JoinableQueue(Queue, BaseJoinableQueue):
    """Redis-backed first-in, first-out queue compatible with ``multiprocessing.JoinableQueue``.

    Additionally, contains an ``n_tasks()`` method exposing the number of items put onto
    the queue without ``task_done()`` being called for them, and an ``n_in_progress()``
    method to count how many have been fetched from the queue with ``task_done()`` being
    called.
    """

    @wraps(Queue.__init__)
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._counter_name = self.name + "__counter"
        if not self._redis.exists(self._counter_name):
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
        super().clear()
        self._redis.delete(self._counter_name)

    def __exit__(self, type_, value, traceback):
        self.join()
        return super().__exit__(type_, value, traceback)


JoinableFifoQueue = JoinableQueue


class LifoQueue(Queue):
    """Redis-backed last-in, first-out queue otherwise compatible with ``multiprocessing.Queue``.

    Contains all of the additional methods from ``yarqueue.Queue``.
    """

    _get_side = Side.RIGHT


class JoinableLifoQueue(JoinableQueue):
    """Redis-backed last-in, first-out queue otherwise compatible with ``multiprocessing.JoinableQueue``.

    Contains all of the additional methods from ``yarqueue.JoinableQueue``.
    """

    _get_side = Side.RIGHT


class DeQueue(Queue):
    """Redis-backed double-ended queue otherwise compatible with ``multiprocessing.Queue``.

    Contains all of the additional methods from ``yarqueue.Queue``.

    ``put()``, ``put_many()``, ``get()``, and ``get_many()`` behave has they do in the
    parent (Fifo)Queue; i.e. putting on the right, and getting from the left.
    Additionally, each has an explicit ``_left`` and ``_right`` variant.
    """

    def put_left(self, obj, block=True, timeout=None) -> None:
        """Put on the left (start) of the list."""
        self._put(Side.LEFT, obj, block, timeout)

    def put_many_left(self, objs: Iterable, block=True, timeout=None):
        """Put many elements on the left (start) of the list"""
        self._put_many(Side.LEFT, objs, block, timeout)

    def put_right(self, obj, block=True, timeout=None) -> None:
        """Put on the right (end) of the list."""
        self._put(Side.RIGHT, obj, block, timeout)

    def put_many_right(self, objs: Iterable, block=True, timeout=None):
        """Put many elements on the right (end) of the list """
        self._put_many(Side.RIGHT, objs, block, timeout)

    def get_left(self, block=True, timeout=None) -> object:
        """Get from the left (start) of the list."""
        return self._get(Side.LEFT, block, timeout)

    def get_right(self, block=True, timeout=None) -> object:
        """Get from the right (end) of the list."""
        return self._get(Side.RIGHT, block, timeout)

    def get_many_left(self, n_items: int, block=True, timeout=None) -> Iterator:
        """Yield elements from the left (start) of the list."""
        yield from self._get_many(Side.LEFT, n_items, block, timeout)

    def get_many_right(self, n_items: int, block=True, timeout=None) -> Iterator:
        """Yield elements from the right (end) of the list."""
        yield from self._get_many(Side.RIGHT, n_items, block, timeout)


class JoinableDeQueue(JoinableQueue, DeQueue):
    """Redis-backed double-ended queue otherwise compatible with ``multiprocessing.JoinableQueue``.
    """

    pass
