import os
from datetime import UTC, datetime, timedelta

from celery import Celery
from celery.canvas import group

# from create_quorum_queues import my_quorum_queue


app = Celery("myapp", broker="amqp://guest@localhost:5672//")

# app.conf.worker_quorumq = os.environ.get("WORKER_QUORUMQ", "False").lower() == "true"

# if app.conf.worker_quorumq:
#     app.conf.task_queues = (my_quorum_queue,)

# A must for quorum queues safety or else messages may be lost
app.conf.broker_transport_options = {"confirm_publish": True}

# Reduce qos to 1*4=4
app.conf.worker_prefetch_multiplier = 1
app.conf.worker_concurrency = 4

# Reduce logs
app.conf.worker_heartbeat = None
app.conf.broker_heartbeat = 0


@app.task
def add(x, y):
    return x + y


@app.task
def identity(x):
    return x


def test():
    # if app.conf.worker_quorumq:
    #     queue = my_quorum_queue.name
    # else:
    #     queue = "celery"
    queue = "celery"

    while True:
        print("Celery Quorum Queue POC")
        print("=======================")
        print("1. Send a simple identity task")
        print("1.1 Send an ETA identity task")
        print("2. Send a group of add tasks")
        print("3. Inspect the active queues")
        print("4. Shutdown Celery worker")
        print("Q. Quit")
        print("Q! Exit")
        choice = input("Enter your choice (1-4 or Q): ")

        if choice == "1" or choice == "1.1":
            payload = f"Hello, {"Quorum" if app.conf.worker_quorumq else "Classic"} Queue!"
            eta = datetime.now(UTC) + timedelta(seconds=30)
            if choice == "1.1":
                result = identity.si(payload).apply_async(queue=queue, eta=eta)
            else:
                result = identity.si(payload).apply_async(queue=queue)
            print()
            print(f"Task sent with ID: {result.id}")
            print("Task type: identity")

            if choice == "1.1":
                print(f"ETA: {eta}")

            print(f"Payload: {payload}")

        elif choice == "2":
            tasks = [
                (1, 2),
                (3, 4),
                (5, 6),
            ]
            result = group(
                add.s(*tasks[0]),
                add.s(*tasks[1]),
                add.s(*tasks[2]),
            ).apply_async(queue=queue)
            print()
            print("Group of tasks sent.")
            print(f"Group result ID: {result.id}")
            for i, task_args in enumerate(tasks, 1):
                print(f"Task {i} type: add")
                print(f"Payload: {task_args}")

        elif choice == "3":
            active_queues = app.control.inspect().active_queues()
            print()
            print("Active queues:")
            for worker, queues in active_queues.items():
                print(f"Worker: {worker}")
                for queue in queues:
                    print(f"  - {queue['name']}")

        elif choice == "4":
            print("Shutting down Celery worker...")
            app.control.shutdown()

        elif choice.lower() == "q":
            print("Quitting test()")
            break

        elif choice.lower() == "q!":
            print("Exiting...")
            os.abort()

        else:
            print("Invalid choice. Please enter a number between 1 and 4 or Q to quit.")

        print("\n" + "#" * 80 + "\n")


if __name__ == "__main__":
    app.start()
