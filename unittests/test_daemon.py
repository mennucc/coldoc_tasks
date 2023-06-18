#!/usr/bin/env python3

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

import coldoc_tasks.simple_tasks, coldoc_tasks.coldoc_tasks, coldoc_tasks.celery_tasks

class TestDaemon(unittest.TestCase):
    #
    def test_daemon(self):
        t = tempfile.NamedTemporaryFile(prefix='info_', delete=False)
        info = t.name
        proc, info_ = coldoc_tasks.coldoc_tasks.tasks_daemon_autostart(infofile=info, logfile=True)
        self.assertTrue( proc )
        address, authkey = coldoc_tasks.coldoc_tasks.tasks_server_readinfo(info)[:2]
        coldoc_tasks.coldoc_tasks.shutdown(address, authkey)
        coldoc_tasks.task_utils.proc_join(proc)


if __name__ == '__main__':
    unittest.main()
