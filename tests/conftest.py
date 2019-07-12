from contextlib import contextmanager
import random
import string

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


@contextmanager
def setup_teardown_cls(cls, name_prefix):
    q = cls(0, qname(cls.__name__, name_prefix))
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
def queue(request):
    with setup_teardown_cls(request.param, request.node.name) as q:
        yield q


@pytest.fixture(params=[JoinableFifoQueue, JoinableLifoQueue, JoinableDeQueue])
def joinable(request):
    with setup_teardown_cls(request.param, request.node.name) as q:
        yield q


@pytest.fixture(params=[FifoQueue, JoinableFifoQueue])
def fifo(request):
    with setup_teardown_cls(request.param, request.node.name) as q:
        yield q


@pytest.fixture(params=[LifoQueue, JoinableLifoQueue])
def lifo(request):
    with setup_teardown_cls(request.param, request.node.name) as q:
        yield q


@pytest.fixture(params=[DeQueue, JoinableDeQueue])
def de(request):
    with setup_teardown_cls(request.param, request.node.name) as q:
        yield q
