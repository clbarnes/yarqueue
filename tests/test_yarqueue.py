#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `yarqueue` package."""
import time
from concurrent.futures.thread import ThreadPoolExecutor
from queue import Full, Empty
from datetime import datetime

import pytest

from yarqueue import JoinableQueue
from yarqueue.base_queue import QueueTimeoutError
from yarqueue.compat import pickle

from .conftest import qname


class CustomClass:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


@pytest.mark.parametrize(
    "value",
    [1, "string", None, [1, 2], {3: 4}, CustomClass(1, "potato", spade="fringe")],
)
def test_basic(queue, value):
    queue.put(value)
    out = queue.get(value)
    assert out == value


def test_highest_protocol(queue):
    assert queue._serializer.protocol == pickle.HIGHEST_PROTOCOL


def test_clear(queue):
    queue.put(1)
    queue.put(2)
    queue.put(3)
    assert queue.qsize() == 3
    queue.clear()
    assert queue.qsize() == 0
    assert queue.empty()


def test_take(queue):
    items = {1, 2, 3}
    for item in items:
        queue.put(item)

    out = set(queue.get_many(len(items)))
    assert out == items


def test_empty(queue):
    with pytest.raises(Empty):
        queue.get_nowait()


def test_full(queue):
    queue.maxsize = 2
    queue.put_nowait(1)
    queue.put_nowait(2)
    with pytest.raises(Full):
        queue.put_nowait(3)


def test_many(queue):
    vals = {1, 2, 3}
    queue.put_many(vals)
    assert set(queue.get_many(3)) == vals


def test_iter(queue):
    vals = {1, 2, 3}
    queue.put_many(vals)
    assert set(queue) == vals


def test_fifo_order(fifo):
    fifo.put(1)
    fifo.put(2)
    fifo.put(3)

    assert list(fifo.get_many(3)) == [1, 2, 3]


def test_lifo_order(lifo):
    lifo.put(1)
    lifo.put(2)
    lifo.put(3)

    assert list(lifo.get_many(3)) == [3, 2, 1]


def test_de(de):
    de.put_left(1)
    de.put_right(2)
    de.put_left(3)
    de.put_right(4)

    assert de.get_left() == 3
    assert de.get_right() == 4
    assert de.get_left() == 1
    assert de.get_right() == 2


def test_joinable_tasks(joinable):
    assert joinable.n_tasks() == 0
    joinable.put(1)
    joinable.put(2)
    assert joinable.n_tasks() == 2
    assert joinable.n_in_progress() == 0
    joinable.get()
    assert joinable.n_in_progress() == 1
    assert joinable.n_tasks() == 2
    joinable.task_done()
    assert joinable.n_tasks() == 1
    joinable.get()
    joinable.task_done()
    assert joinable.n_tasks() == 0


def test_wait(joinable):
    joinable.put(1)

    with pytest.raises(QueueTimeoutError):
        joinable.wait(0.1)


def test_join(request):
    name = qname(JoinableQueue.__name__, request.node.name)
    q = JoinableQueue(0, name)
    q.put(1)

    def fn():
        time.sleep(2)
        got = q.get()
        q.task_done()
        return got

    with ThreadPoolExecutor(max_workers=1) as exe:
        start = datetime.utcnow()
        fut = exe.submit(fn)
        q.join()
        elapsed = datetime.utcnow() - start
        assert fut.result() == 1

    assert elapsed.total_seconds() > 2
