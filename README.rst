=======================
Yet Another Redis Queue
=======================

.. image:: https://img.shields.io/pypi/v/yarqueue.svg
        :target: https://pypi.python.org/pypi/yarqueue

.. image:: https://img.shields.io/travis/clbarnes/yarqueue.svg
        :target: https://travis-ci.org/clbarnes/yarqueue

.. image:: https://readthedocs.org/projects/yarqueue/badge/?version=latest
        :target: https://yarqueue.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

Yet Another python queue backed by redis; but modern and compliant

* Free software: MIT license
* Documentation: https://yarqueue.readthedocs.io.

Heavily inspired by hotqueue_.

Features
--------

* Compatible with the API of ``multiprocessing.Queue``
* LIFO, FIFO and Double-Ended variants

  - Each has a non-joinable and a joinable form (compatible with ``multiprocessing.JoinableQueue``)

* Additional convenience methods:

  - ``get_many()``, ``put_many()``, ``clear()`` for all queues
  - ``n_tasks()`` and ``n_in_progress()`` for joinable queues

* Can be used as a context manager

  - Deletes the queue on exit
  - For Joinable variants, first waits for all tasks to be done

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
.. _hotqueue: https://github.com/richardhenry/hotqueue
