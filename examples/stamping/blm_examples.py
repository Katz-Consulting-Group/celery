from tasks import blm_202, noop, replace_task_with

from celery import chain, group


def blm202():
    # run in c4
    blm_202.si().apply_async()


def blm204():
    # replace_task_5 -> group( noop_4 * 2) -> X_4_5 means task runs in c5 and _4 runs in c4.
    noop_4a = noop.si("4a")
    noop_4b = noop.si("4b")
    replace_task_5 = group(noop_4a, noop_4b)
    canvas = replace_task_with.si(replace_task_5).set(queue="celery5queue")
    canvas.apply_async()


def blm205():
    # replace_task_5 ---> chain(task1_4, task2_4)
    noop_4a = noop.si("task1_4")
    noob_4b = noop.si("task2_4")
    replace_task_5 = chain(noop_4a, noob_4b)
    canvas = replace_task_with.si(replace_task_5).set(queue="celery5queue")
    canvas.apply_async()


def blm206():
    # chain(task1_4, group([task2_4 *2]), task3_4, task4_4)
    noop_4a = noop.si("task1_4")
    noop_4b = noop.si("task2_4")
    noop_4c = noop.si("task3_4")
    noop_4d = noop.si("task4_4")
    replace_task_5 = chain(noop_4a, group(noop_4b, noop_4c), noop_4d)
    canvas = replace_task_with.si(replace_task_5).set(queue="celery5queue")
    canvas.apply_async()
