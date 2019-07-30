# -*- coding: utf-8 -*-

"""Top-level package for Yet Another Redis Queue."""

from .queue import (
    Queue,
    FifoQueue,
    LifoQueue,
    DeQueue,
    JoinableQueue,
    JoinableFifoQueue,
    JoinableLifoQueue,
    JoinableDeQueue,
)
from .base_queue import QueueTimeoutError

from .serializer import Pickle, Json

__author__ = """Chris L. Barnes"""
__email__ = "barnesc@janelia.hhmi.org"
__version__ = "0.3.0"

__all__ = [
    "Queue",
    "FifoQueue",
    "LifoQueue",
    "DeQueue",
    "JoinableQueue",
    "JoinableFifoQueue",
    "JoinableLifoQueue",
    "JoinableDeQueue",
    "QueueTimeoutError",
    "Pickle",
    "Json",
]
