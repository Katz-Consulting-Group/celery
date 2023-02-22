from celery import Celery

app = Celery(
    'myapp',
    broker='pyamqp://',
    backend='cache+pylibmc://',
)
