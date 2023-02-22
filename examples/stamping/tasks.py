from time import sleep

from config import app

from celery import Task
from celery.canvas import Signature, chain, group, maybe_signature, signature
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


def log_demo(running_task):
    request, name = running_task.request, running_task.name + running_task.request.argsrepr
    stamps, stamped_headers = None, None
    if hasattr(request, "stamps"):
        stamps = request.stamps or {}
        stamped_headers = request.stamped_headers or []

    if stamps and stamped_headers:
        logger.critical(f"Found {name}.stamps: {stamps}")
        logger.critical(f"Found {name}.stamped_headers: {stamped_headers}")
    else:
        logger.critical(f"Running {name} without stamps")

    links = request.callbacks or []
    for link in links:
        link = maybe_signature(link)
        logger.critical(f"Found {name}.link: {link}")
        stamped_headers = link.options.get("stamped_headers", [])
        stamps = {stamp: link.options[stamp] for stamp in stamped_headers}

        if stamps and stamped_headers:
            logger.critical(f"Found {name}.link stamps: {stamps}")
            logger.critical(f"Found {name}.link stamped_headers: {stamped_headers}")
        else:
            logger.critical(f"Running {name}.link without stamps")


@app.task(bind=True)
def blm_202(self):
    # current runs in c4

    # runs in c5
    s1 = blm_202_replace1.si().set(queue="celery5queue")
    raise self.replace(s1)


@app.task(bind=True)
def blm_202_replace1(self):
    # current runs in c5

    # runs in c5
    s1 = blm_202_replace2.si().set(queue="celery5queue")
    raise self.replace(s1)


@app.task(bind=True)
def blm_202_replace2(self):
    # current runs in c5

    # runs in c5
    s1 = group_chain_task.si().set(queue="celery5queue")
    raise self.replace(s1)


@app.task(bind=True)
def group_chain_task(self):
    def foo():
        # runs in c4
        s1 = noop.si("s1")
        s2 = noop.si("s2")

        return chain(s1, s2)

    raise self.replace(group([foo(), foo()]))


@app.task(bind=True, base=Task)
def replace_task_with(self: Task, other: Signature):
    log_demo(self)
    logger.warning(f"Replacing 'replace_task_with(celery4)' with: {other}")
    return self.replace(signature(other))


@app.task(bind=True)
def noop(self, x):
    log_demo(self)
    return x


@app.task(bind=True)
def identity_task(self, x):
    """Identity function"""
    log_demo(self)
    return x


@app.task
def waitfor(seconds: int) -> None:
    """Wait for "seconds" seconds, ticking every second."""
    print(f"Waiting for {seconds} seconds...")
    for i in range(seconds):
        sleep(1)
        print(f"{i+1} seconds passed")
