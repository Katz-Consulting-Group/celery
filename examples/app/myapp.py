"""myapp.py

Usage::

   (window1)$ python myapp.py worker -l info

   (window2)$ python
   >>> from myapp import add
   >>> add.delay(16, 16).get()
   32


You can also specify the app to use with the `celery` command,
using the `-A` / `--app` option::

    $ celery -A myapp worker -l info

With the `-A myproj` argument the program will search for an app
instance in the module ``myproj``.  You can also specify an explicit
name using the fully qualified form::

    $ celery -A myapp:app worker -l info

"""

import json
from celery import Celery, Task
from celery.signals import task_received

app = Celery(
    "myapp",
    broker="redis://",
    backend="redis://",
)


@app.task
def add(x, y):
    return x + y


@app.task
def identity(x):
    return x


@app.task(bind=True)
def replaced_task(self: Task):
    self.replace(add.s(1, 1) | identity.s())


@task_received.connect
def task_received_handler(sender=None, request=None, signal=None, **kwargs):
    print(f"In {signal.name} for: {repr(request)}")
    if hasattr(request, "stamped_headers") and request.stamped_headers:
        print(f"Found stamps: {request.stamped_headers}")
        print(json.dumps(request.stamps, indent=4, sort_keys=True))
    else:
        print("No stamps found")


if __name__ == "__main__":
    app.start()
