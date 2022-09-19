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

from time import sleep

from celery import Celery

app = Celery(
    'myapp',
    broker='pyamqp://',
    backend='redis://',
)


@app.task
def add(x, y):
    sleep(10)
    return x + y


# from celery.canvas import StampingVisitor
# from uuid import uuid4
# from time import sleep
# target_monitoring_id = 1234
# class MonitoringIdStampingVisitor(StampingVisitor):
#     def on_signature(self, sig, **headers) -> dict:
#         return {'monitoring_id': target_monitoring_id, 'stamped_headers': ['monitoring_id']}

# sig = add.si(1, 1)
# sig.stamp(visitor=MonitoringIdStampingVisitor())
# r = sig.delay()
# sleep(1)
# r.revoke_by_stamped_header(header=[{'monitoring_id': target_monitoring_id}], terminate=True)
# r.get()

if __name__ == '__main__':
    app.start()
