import json
from itertools import zip_longest
from pathlib import Path
from typing import Optional, Dict

from redis import Redis

from .. import __version__

try:
    import click
except ImportError:
    raise ImportError(
        "click not importable; HTTP watcher not available. pip install click"
    )

try:
    import flask
    from flask import Flask
    from werkzeug.serving import run_simple
except ImportError:
    raise ImportError(
        "flask not importable; HTTP watcher not available. pip install flask"
    )

from .common import QueueWatcher, DEFAULT_INTERVAL


TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"


class JsonQueueWatcher:
    def __init__(self, watcher: QueueWatcher, total=None):
        self.watcher = watcher
        self.total = total or len(self.watcher)

    def status(self):
        queued, in_progress = self.watcher.items_tasks()
        obj = {"queued": queued, "inProgress": in_progress}
        if self.total:
            obj["total"] = self.total
        return obj


class MultiJson:
    def __init__(self, redis, names_totals: Dict[str, Optional[int]]):
        self.json_watcher = [
            JsonQueueWatcher(QueueWatcher(name, redis), total)
            for name, total in names_totals.items()
        ]

    def __iter__(self):
        yield from self.json_watcher

    def status(self, names=None):
        statuses = {w.watcher.name: w.status() for w in self}
        if names:
            return {n: statuses.get(n) for n in names}
        else:
            return statuses


def main(host, port, redis, names_totals):
    watchers = MultiJson(redis, names_totals)
    app = Flask("yarqserve", template_folder=TEMPLATE_DIR)

    @app.route("/")
    def page():
        statuses = watchers.status()
        context = {
            "interval": DEFAULT_INTERVAL * 1000,
            "names": list(statuses),
            "statusStr": json.dumps(statuses),
        }
        return flask.render_template("index.html", **context)

    @app.route("/json")
    def data():
        return flask.jsonify(watchers.status())

    run_simple(host, port, app)


@click.command(
    help="Watch the progress of a number of redis-backed queues, over HTTP."
)
@click.version_option(version=__version__)
@click.help_option()
@click.option(
    "--name", "-n", multiple=True,
    help="Name of redis lists to watch (accepts multiple)",
)
@click.option(
    "--total", "-t", multiple=True, type=int,
    help="Total items added to the queue (accepts multiple, same order as --name"
)
# @click.option(
#     "--interval", "-i", default=1, type=float, help="Polling interval (seconds)",
#     show_default=True,
# )
@click.option(
    "--host", default="localhost", help="Hostname at which to run server",
    show_default=True
)
@click.option(
    "--port", default=8080, type=int, help="Port on which to run server",
    show_default=True
)
@click.option(
    "--rhost", default="localhost", help="Hostname for the Redis instance",
    show_default=True,
)
@click.option(
    "--rport", default=6379, type=int, help="Port for the Redis instance",
    show_default=True,
)
@click.option(
    "--db", default=0, type=int, help="DB ID for the Redis instance",
    show_default=True,
)
@click.option(
    "--password", type=int, help="Password for the Redis instance",
    show_default=True,
)
def yarqserve(name, total, host, port, rhost, rport, db, password):
    redis = Redis(rhost, rport, db, password)
    names_totals = dict(zip_longest(name, total))
    main(host, port, redis, names_totals)


if __name__ == "__main__":
    yarqserve()
