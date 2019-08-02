=====
Usage
=====

Queues
------

The basic usage is intentionally much like the standard `multiprocessing.Queue`_.
However, in the background this basic usage is spinning up a `redislite`_ instance.
Like a standard `multiprocessing.Queue`_, objects are (by default) serialized with `pickle`_.
However, the highest, rather than default, protocol is used (and it will attempt to use the backported `pickle5`_).

.. code-block:: python

    import time
    import yarqueue

    queue = yarqueue.Queue()

    queue.put(1)
    queue.put("two")
    queue.put({3: "four"})
    queue.put(["five", 6, 7.0])

    assert queue.get() == 1
    assert queue.get() == "two"


    def parallel_fn(wait):
        print("waiting for {} seconds".format(wait))
        time.sleep(wait)

        return queue.get()

    import multiprocessing as mp
    with mp.Pool() as p:
        third, fourth = p.map(parallel_fn, range(2))

``.put()`` and ``.get()`` methods accept ``block`` and ``timeout`` arguments like their counterparts;
they also have the expected ``.*_nowait()`` variants.
``.qsize()``, ``.full()``, and ``.empty()`` also work as expected.

There are some additional convenience methods:

.. code-block:: python

    # a more pythonic queue length interface
    assert len(queue) == queue.qsize()

    # add several elements in one go
    queue.put_many(["one", "one", "two", "two"])

    # return a generator of the first n elements
    for item in queue.get_many(2):
        assert item == "one"

    assert list(queue.get_many(2)) == ["two", "two"]

    # empty the queue
    queue.clear()

    # pythonic iteration: yields with .get_nowait() until the queue is empty
    for item in queue:
        print(item)

    # clears queue on leaving the context manager
    with queue:
        queue.put_many([1, 2, 3])
        assert len(queue) == 3

    assert len(queue) == 0

Like ``multiprocessing``, there is a ``JoinableQueue``, which keeps track of how many tasks have been added and processed.
With ``.join()``, it will wait until all of them have registered ``.task_done()``

.. code-block:: python

    jqueue = yarqueue.JoinableQueue()
    jqueue.put_many([1, 2, 3])
    one = jqueue.get()
    assert one == 1
    jqueue.task_done()

    def another_parallel_fn(wait):
        item = jqueue.get()
        print("waiting for {} seconds".format(wait))
        time.sleep(wait)
        jqueue.task_done()
        return item

    with mp.Pool() as p:
        results = p.map_async(another_parallel_fn, range(2))
        jqueue.join()

``yarqueue`` has some improvements here as well, which allow you to track workers' progress through the queue.
Joinable queues increment a counter whenever an item is added, and decrement it when ``.task_done()`` is called.
``.qsize()`` counts how many items are currently in the queue.
``.n_tasks()`` returns the current counter value.
``.n_in_progress()`` returns the number of items which have been removed from the queue, but are not done yet.

.. code-block:: python

    jqueue.clear()

    jqueue.put_many([1, 2, 3])
    assert len(jqueue) == 3
    assert jqueue.n_tasks() == 3
    assert jqueue.n_in_progress() == 0

    item = jqueue.get()
    assert len(jqueue) == 2
    assert jqueue.n_tasks() == 3
    assert jqueue.n_in_progress() == 1

    jqueue.task_done()
    assert len(jqueue) == 2
    assert jqueue.n_tasks() == 2
    assert jqueue.n_in_progress() == 0

    # .wait() does the same as .join(), but with a timeout in seconds
    import pytest
    with pytest.raises(yarqueue.QueueTimeoutError):
        jqueue.wait(1.5)

    # the context manager calls .join() on exit
    with jqueue:
        jqueue.get()
        jqueue.task_done()
        jqueue.get()
        jqueue.task_done()

As well as the default first-in, first-out queue, there is a last-in, first-out queue (stack), like the `threading.LifoQueue`_: ``yarqueue.LifoQueue``.
There is also a double-ended queue (``yarqueue.DeQueue``) which behaves like the standard Queue,
but has additional ``.*_left()`` (start) and ``.*_right()`` (end) variants for each ``.put*()`` and ``.get*()`` method.

.. code-block:: python

    lifo = yarqueue.LifoQueue()
    lifo.put(1)
    lifo.put(2)
    assert lifo.get() == 2
    assert lifo.get() == 1

    de = yarqueue.DeQueue()
    de.put(1)
    de.put_left(2)
    de.put_right(3)
    assert list(de.get_many_left(3)) == [2, 1, 3]

    de.put_left(1)
    de.put_left(2)
    de.put_many_left(3, 4)  # order as would be expected for repeated calls
    assert list(de.get_many_left(4)) == [4, 3, 2, 1]

These come in joinable varieties too: ``yarqueue.JoinableLifoQueue`` and ``yarqueue.JoinableDeQueue``.

The power of redis
------------------

The real value of using redis-backed queues is sharing them across different python instances.
To do this, you need to give the queues a name and a connection to the same redis server:

.. code-block:: python

    from redis import Redis

    redis_config = {"host": "localhost", "port": 6379, "db": 0}

    redis1 = Redis(**redis_config)
    queue1 = yarqueue.Queue(name="my_queue", redis=redis1)

    redis2 = Redis(**redis_config)
    queue2 = yarqueue.Queue(name="my_queue", redis=redis2)

    queue1.put(1)
    assert queue2.get() == 1

Be aware that different python environments may have different ``pickle`` protocols available:
it may be better to explicitly set your serializer on queue instantiation (see below).

These names are not mangled: redis can be used to synchronise programs running in different languages!
If you're using it this way, you should replace the default serializer, as most languages do not use ``pickle``.
Note that other languages and queue implementations will not respect the task-counting semantics of ``yarqueue.JoinableQueue``.

Serializers
-----------

In ``yarqueue``, a serializer is anything which can turn an object into ``bytes`` with a ``.dumps(obj)`` method,
and then ``bytes`` back into a python object with ``.loads(bytes_object)`` method.

The default serializer is a wrapper around ``pickle``, and uses the highest available pickle protocol.
Explicitly set the protocol version (useful for sharing a redis list between python environments) like this:

.. code-block:: python

    from yarqueue import Pickle

    pickle3_q = yarqueue.Queue(serializer=Pickle(3))

Feel free to create your own serializers (useful for sharing a redis list between programming languages).
here is the implementation of the included ``json`` serializer:

.. code-block:: python

    from copy import deepcopy
    from yarqueue.serializer import BaseSerializer

    class Json(BaseSerializer):
        def __init__(self, dumps_kwargs=None, loads_kwargs=None):
            self.dumps_kwargs = deepcopy(dumps_kwargs) or dict()
            self.loads_kwargs = deepcopy(loads_kwargs) or dict()

        def dumps(self, obj) -> bytes:
            return json.dumps(obj, **self.dumps_kwargs).encode()

        def loads(self, bytes_object: bytes) -> object:
            return json.loads(bytes_object, **self.loads_kwargs)

If you explicitly set the ``serializer`` argument to ``None``, values will not be serialized and deserialized.
This might be useful if you're only working with primitives ``redis`` understands.

Watchers
--------

Command line
~~~~~~~~~~~~

Requires click_ and tqdm_.

::

    Usage: yarqwatch [OPTIONS]

      Watch the progress of a number of redis-backed queues, on the command
      line.

    Options:
      --version             Show the version and exit.
      --help                Show this message and exit.
      -n, --name TEXT       Name of redis lists to watch (accepts multiple)
      -t, --total INTEGER   Total items added to the queue (accepts multiple, same
                            order as --name)
      -i, --interval FLOAT  Polling interval (seconds)  [default: 1]
      --host TEXT           Hostname for the Redis instance  [default: localhost]
      --port INTEGER        Port for the Redis instance  [default: 6379]
      --db INTEGER          DB ID for the Redis instance  [default: 0]
      --password TEXT       Password for the Redis instance

For example, to create a progress bar for
the ``potato`` queue, which had 10 jobs, and
the ``spade`` queue, which had 20 jobs,
on the redis instance at ``myserver:1234``,
polling every 5 seconds::

    yarqwatch -n potato -t 10 -n spade -t 20 --host myserver --port 1234 --interval 5

If totals are not explicitly given, the number of enqueued (and for Joinable queues, in-progress) items at calling time is used.

HTTP
~~~~

Requires click_ and flask_.

::

    Usage: yarqserve [OPTIONS]

      Watch the progress of a number of redis-backed queues, over HTTP.

    Options:
      --version            Show the version and exit.
      --help               Show this message and exit.
      -n, --name TEXT      Name of redis lists to watch (accepts multiple)
      -t, --total INTEGER  Total items added to the queue (accepts multiple, same
                           order as --name
      --host TEXT          Hostname at which to run server  [default: localhost]
      --port INTEGER       Port on which to run server  [default: 8080]
      --rhost TEXT         Hostname for the Redis instance  [default: localhost]
      --rport INTEGER      Port for the Redis instance  [default: 6379]
      --db INTEGER         DB ID for the Redis instance  [default: 0]
      --password INTEGER   Password for the Redis instance

For example, to serve a webpage and REST endpoint with a progress bar for
the ``potato`` queue, which started with 10 jobs, and
the ``spade`` queue, which started with 20 jobs,
on the redis instance at ``myserver:1234``,
at the host ``localhost:8888``::

    yarqserve -n potato -t 10 -n spade -t 20 --host localhost --port 8888 --rhost myserver --rport 1234

If totals are not explicitly given, the number of enqueued (and for Joinable queues, in-progress) items at server startup time is used.

Point your browser to http://localhost:8888 to see the webpage,
or ``curl http://localhost:8888/json`` to get the progress data in JSON form.
The returned object's keys are the queue names, and the values are objects with ``queued``, ``inProgress``, and ``total`` counts.

.. _multiprocessing.Queue: https://docs.python.org/3/library/multiprocessing.html#multiprocessing.Queue
.. _redislite: https://github.com/yahoo/redislite
.. _pickle: https://docs.python.org/3/library/pickle.html
.. _pickle5: https://pypi.org/project/pickle5/
.. _threading.LifoQueue: https://docs.python.org/3/library/queue.html#queue.LifoQueue
.. _click: https://click.palletsprojects.com
.. _flask: https://flask.palletsprojects.com
.. _tqdm: https://github.com/tqdm/tqdm
