"""myapp.py

Usage::

   (window1)$ python myapp.py worker -l INFO

   (window2)$ python
   >>> from myapp import add
   >>> add.delay(16, 16).get()
   32


You can also specify the app to use with the `celery` command,
using the `-A` / `--app` option::

    $ celery -A myapp worker -l INFO

With the `-A myproj` argument the program will search for an app
instance in the module ``myproj``.  You can also specify an explicit
name using the fully qualified form::

    $ celery -A myapp:app worker -l INFO

"""

import json
import uuid

from celery import Celery, Task
from celery.canvas import Signature, StampingVisitor, chain, group, signature
from celery.signals import task_received

app = Celery(
    "myapp",
    broker="redis://",
    backend="redis://",
)


class MyStampingVisitor(StampingVisitor):
    def on_signature(self, sig, **headers) -> dict:
        return {'mystamp': 'I am a stamp!'}


class MonitoringTaskIdVisitor(StampingVisitor):
    def on_signature(self, sig, **headers) -> dict:
        return {'mtask_id': str(uuid.uuid4())}


class MyTask(Task):
    def on_replace(self, sig):
        sig.stamp(MonitoringTaskIdVisitor())
        return super().on_replace(sig)


@ app.task
def add(x, y):
    return x + y


@ app.task
def identity(x):
    return x


# replaced_sig = add.s(1, 1) | identity.s()
# replaced_sig = group(identity.s("group_task_1"), identity.s("group_task_2"))
# replaced_sig = chain(add.s(1, 1), identity.s())
# replaced_sig = chain(add.s(1, 1), group(add.s(4), add.s(2)))
# replaced_sig = chain(add.s(1, 1), group(add.s(4), add.s(2)), identity.s(), identity.s())

# replaced_sig = group(identity.s("group_task_1"), identity.s("group_task_2"))
# replaced_sig = add.s(1, 1) | identity.s()
replaced_sig = chain(add.s(1, 1)) | group(add.s(4), add.s(2))
# replaced_sig = chain(add.s(1, 1)) | group(add.s(4), add.s(2)) | chain(identity.s()) | chain(identity.s())


@ app.task(bind=True, base=MyTask)
def replaced_task(self: MyTask):
    self.replace(replaced_sig)


@ task_received.connect
def task_received_handler(
    sender=None,
    request=None,
    signal=None,
    **kwargs


):
    print(f'In {signal.name} for: {repr(request)}')
    if hasattr(request, 'stamped_headers') and request.stamped_headers:
        print(f'Found stamps: {request.stamped_headers}')
        print(json.dumps(request.stamps, indent=4, sort_keys=True))
    else:
        print('No stamps found')


def run_from_shell():
    sig: Signature = replaced_task.s()
    sig = signature('myapp.replaced_task', queue='q5')
    sig.stamp(visitor=MyStampingVisitor())
    sig.delay()
    # print(f'result: {res.get()}')


run_from_shell()
# if __name__ == "__main__":
#     app.start()
