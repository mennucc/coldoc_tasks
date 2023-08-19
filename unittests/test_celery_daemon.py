#!/usr/bin/env  -S -- python3  -X tracemalloc

"""
run this as
$ python3 unittests/test_cli.py
or
$ python3 -m unittests unittests/test_cli.py
or
$ pytest-3 unittests/test_cli.py 
"""

import os, sys, io, unittest, tempfile, shutil, time
import functools, tempfile, threading, multiprocessing, logging, signal
from os.path import join as osjoin

import logging
logger = logging.getLogger(__name__)

testdir = os.path.dirname(os.path.realpath(__file__))
sourcedir = os.path.dirname(testdir)



try:
    import celery
except ImportError:
    logger.error('Cannot import `celery`')
    celery = None

if __name__ == '__main__':
    if sourcedir not in sys.path:
        sys.path.insert(0, sourcedir)

import coldoc_tasks.task_utils, coldoc_tasks.celery_tasks

class TestDaemon(unittest.TestCase):
    #
    def test_daemon(self):
        cc = os.path.join(sourcedir, 'etc', 'celeryconfig.py')
        proc = coldoc_tasks.celery_tasks.tasks_daemon_autostart(cc)
        self.assertTrue( proc )
        ret = coldoc_tasks.celery_tasks.test(cc)
        self.assertTrue( ret == 0 )
        coldoc_tasks.celery_tasks.shutdown(cc)
        coldoc_tasks.task_utils.proc_join(proc)


if __name__ == '__main__':
    unittest.main()
