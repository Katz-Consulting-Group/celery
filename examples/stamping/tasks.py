from time import sleep

from config import app


@app.task
def identity(x):
    """Identity function"""
    return x


@app.task
def waitfor(seconds):
    """Wait for "seconds" seconds, ticking every second."""
    print("Waiting for {0} seconds...".format(seconds))
    for i in range(seconds):
        sleep(1)
        print("{0} seconds passed".format(i + 1))
