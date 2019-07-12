import time
from abc import ABC, abstractmethod
from typing import Optional
from datetime import datetime

from .constants import POLL_INTERVAL


class QueueTimeoutError(Exception):
    pass


class BaseQueue(ABC):
    def __init__(self, maxsize=0):
        self.maxsize = maxsize

    @abstractmethod
    def qsize(self) -> int:
        """Return the size of the queue."""
        pass

    def empty(self) -> bool:
        """Return whether the queue is empty"""
        return self.qsize() == 0

    def full(self) -> bool:
        """Return whether the queue is full (only if ``maxsize`` was set)"""
        return self.maxsize and self.qsize() >= self.maxsize

    @abstractmethod
    def put(self, obj, block=True, timeout: Optional[float] = None) -> None:
        """Put obj into the queue.

        If the optional argument block is True (the default) and timeout is
        ``None`` (the default), block if necessary until a free slot is
        available.
        If ``timeout`` is a positive number, it blocks at most ``timeout``
        seconds and raises the ``queue.Full`` exception if no free slot was
        available within that time.
        Otherwise (``block`` is ``False``), put an item on the queue if a free
        slot is immediately available, else raise the ``queue.Full`` exception
         (``timeout`` is ignored in that case).

        :param obj: object to add to the queue
        :param block: whether to wait until item can be added
        :param timeout: if blocking, how long to wait (``None`` if infinite)
        """
        pass

    def put_nowait(self, obj) -> None:
        """Equivalent to ``put(obj, False)``.

        :param obj: object to add to the queue
        """
        self.put(obj, False)

    @abstractmethod
    def get(self, block=True, timeout: Optional[float] = None) -> object:
        """Remove and return an item from the queue.

        If optional args ``block`` is ``True`` (the default) and ``timeout`` is
        ``None`` (the default), block if necessary until an item is available.
        If timeout is a positive number, it blocks at most timeout seconds and
        raises the queue.Empty exception if no item was available within that
        time. Otherwise (block is False), return an item if one is immediately
        available, else raise the queue.Empty exception (timeout is ignored in
        that case).

        :param block: whether to wait until an item is available
        :param timeout: if blocking, how long to wait (``None`` if infinite)
        :return: item returned from the queue
        """
        pass

    def get_nowait(self) -> object:
        """Equivalent to ``get(False)``"""
        return self.get(False)


class BaseJoinableQueue(BaseQueue):
    @abstractmethod
    def n_tasks(self) -> int:
        """How many items have been put onto the list without a respective ``task_done()`` call"""
        pass

    def n_in_progress(self) -> int:
        """How many items have been popped from the queue without ``task_done()`` being called for them"""
        return self.n_tasks() - len(self)

    @abstractmethod
    def task_done(self) -> None:
        """Indicate that a formerly enqueued task is complete.

        Used by queue consumers.
        For each ``get()`` used to fetch a task, a subsequent call to task_done() tells
        the queue that the processing on the task is complete.

        If a ``join()`` is currently blocking, it will resume when all items have been
        processed (meaning that a ``task_done()`` call was received for every item that
        had been ``put()`` into the queue).

        Raises a ``ValueError`` if called more times than there were items placed in the
        queue.
        """
        pass

    def wait(self, timeout: float):
        """Like ``join``, but with a timeout.

        If the timeout is reached, a QueueTimeoutError is raised.

        :param timeout: timeout in seconds
        """
        timeout = timeout or float("inf")
        started = datetime.utcnow()
        while self.n_tasks() > 0:
            time.sleep(POLL_INTERVAL)
            elapsed = datetime.utcnow() - started
            if elapsed.total_seconds() > timeout:
                raise QueueTimeoutError("Joining queue timed out")

    def join(self) -> None:
        """Block until all items in the queue have been gotten and processed.

        The count of unfinished tasks goes up whenever an item is added to the queue.
        The count goes down whenever a consumer calls ``task_done()`` to indicate that
        the item was retrieved and all work on it is complete.
        When the count of unfinished tasks drops to zero, ``join()`` unblocks.
        """
        while self.n_tasks() > 0:
            time.sleep(POLL_INTERVAL)
