import os
import time


def fakesum(a, b, c):
    return a + b + c


def fakediv(a, b):
    return a / b

def wait_for_file(path, timeout=5.0, poll_interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if os.path.exists(path):
            return path
        time.sleep(poll_interval)
    raise RuntimeError('Timeout waiting for file {!r}'.format(path))
