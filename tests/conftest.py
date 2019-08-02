from contextlib import contextmanager
import random
import string

from redislite import Redis
import pytest

from yarqueue import (
    FifoQueue,
    LifoQueue,
    DeQueue,
    JoinableFifoQueue,
    JoinableLifoQueue,
    JoinableDeQueue,
)

rand = random.Random(1991)
chars = string.ascii_letters + string.digits


def qname(class_name, test_name):
    return ":".join((class_name, test_name, "".join(rand.choices(chars, k=5))))


@pytest.fixture
def redis():
    rd = Redis()
    keys = set(rd.keys())
    yield rd
    to_del = [k for k in rd.keys() if k not in keys]
    if to_del:
        rd.delete(*to_del)


@contextmanager
def setup_teardown_cls(redis, cls, name_prefix):
    q = cls(0, qname(cls.__name__, name_prefix), redis)
    yield q
    q.clear()


@pytest.fixture(
    params=[
        FifoQueue,
        LifoQueue,
        DeQueue,
        JoinableFifoQueue,
        JoinableLifoQueue,
        JoinableDeQueue,
    ]
)
def queue(redis, request):
    with setup_teardown_cls(redis, request.param, request.node.name) as q:
        yield q


@pytest.fixture(params=[JoinableFifoQueue, JoinableLifoQueue, JoinableDeQueue])
def joinable(redis, request):
    with setup_teardown_cls(redis, request.param, request.node.name) as q:
        yield q


@pytest.fixture(params=[FifoQueue, JoinableFifoQueue])
def fifo(redis, request):
    with setup_teardown_cls(redis, request.param, request.node.name) as q:
        yield q


@pytest.fixture(params=[LifoQueue, JoinableLifoQueue])
def lifo(redis, request):
    with setup_teardown_cls(redis, request.param, request.node.name) as q:
        yield q


@pytest.fixture(params=[DeQueue, JoinableDeQueue])
def de(redis, request):
    with setup_teardown_cls(redis, request.param, request.node.name) as q:
        yield q
