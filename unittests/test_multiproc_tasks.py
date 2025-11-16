#!/usr/bin/env -S -- python3 -X tracemalloc

import os
import sys
import multiprocessing
import time
import unittest

testdir = os.path.dirname(os.path.realpath(__file__))
sourcedir = os.path.dirname(testdir)

if sourcedir not in sys.path:
    sys.path.insert(0, sourcedir)


from coldoc_tasks import multiproc_tasks


def _capture_arguments(*args, **kwargs):
    return args, kwargs


def _slow_add(a, b, delay=0):
    if delay:
        time.sleep(delay)
    return a + b


def _raise_boom():
    raise ValueError("boom")


try:
    multiprocessing.set_start_method("fork")
except RuntimeError:
    # Already set or unsupported; tests will skip elsewhere as needed
    pass


@unittest.skipIf(sys.platform == "win32",
                 "multiprocessing semantics differ on Windows")
class TestMultiprocTasks(unittest.TestCase):

    def test_multiproc_runs_command_in_subprocess(self):
        job = multiproc_tasks.fork_class()
        job.run(_slow_add, 2, 3)
        self.assertEqual(job.wait(), 5)

    def test_multiproc_passes_args_and_kwargs(self):
        job = multiproc_tasks.fork_class()
        job.run(_capture_arguments, 1, 2, answer=42)
        args, kwargs = job.wait()
        self.assertEqual(args, (1, 2))
        self.assertEqual(kwargs, {"answer": 42})

    def test_wait_returns_exception_when_not_raising(self):
        job = multiproc_tasks.fork_class()
        job.run(_raise_boom)
        exc = job.wait(raise_exception=False)
        self.assertIsInstance(exc, ValueError)
        self.assertIs(job.exception, exc)
        self.assertIsNotNone(job.traceback)

    def test_wait_raises_exception_by_default(self):
        job = multiproc_tasks.fork_class()
        job.run(_raise_boom)
        with self.assertRaises(ValueError):
            job.wait()

    def test_run_without_fork_executes_inline(self):
        job = multiproc_tasks.fork_class(use_fork=False)
        job.run(_capture_arguments, "a", foo="bar")
        args, kwargs = job.wait()
        self.assertEqual(args, ("a",))
        self.assertEqual(kwargs, {"foo": "bar"})


if __name__ == '__main__':
    unittest.main()
