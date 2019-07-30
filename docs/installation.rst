.. highlight:: shell

============
Installation
============


Stable release
--------------

To install Yet Another Redis Queue, run this command in your terminal:

.. code-block:: console

    pip install yarqueue

Yarqueue comes with a number of optional extras.

.. code-block:: console

    # for a self-contained python implementation of redis
    pip install yarqueue[redislite]

    # for the backported pickle protocol 5 in python 3.6 and 3.7
    pip install yarqueue[pickle5]

    # for the command line queue watching utility
    pip install yarqueue[cli]

    # for the HTTP queue watching utility
    pip install yarqueue[http]

    # for all of the above
    pip install yarqueue[all]


If you don't have `pip`_ installed, this `Python installation guide`_ can guide
you through the process.

.. _pip: https://pip.pypa.io
.. _Python installation guide: http://docs.python-guide.org/en/latest/starting/installation/


From sources
------------

The sources for Yet Another Redis Queue can be downloaded from the `Github repo`_.

You can either clone the public repository:

.. code-block:: console

    $ git clone git://github.com/clbarnes/yarqueue

Or download the `tarball`_:

.. code-block:: console

    $ curl  -OL https://github.com/clbarnes/yarqueue/tarball/master

Once you have a copy of the source, you can install it with:

.. code-block:: console

    $ python setup.py install


.. _Github repo: https://github.com/clbarnes/yarqueue
.. _tarball: https://github.com/clbarnes/yarqueue/tarball/master
