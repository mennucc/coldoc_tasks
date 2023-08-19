#!/usr/bin/env  -S --  python3 -X tracemalloc

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

if __name__ == '__main__':
    if sourcedir in sys.path:
        sys.path.insert(0, sourcedir)

try:
    import celery
except ImportError:
    logger.error('Cannot import `celery`')
    celery = None

try:
    import django
except ImportError:
    logger.error('Cannot import `django`')
    django = None

if __name__ == '__main__':
    if sourcedir not in sys.path:
        sys.path.insert(0, sourcedir)

import coldoc_tasks.simple_tasks, coldoc_tasks.coldoc_tasks, coldoc_tasks.celery_tasks


@unittest.skipIf(django is None, 'django is not installed')
class TestDjangoDaemon(unittest.TestCase):
    #
    @classmethod
    def setUpClass(cls):
        a = os.path.join(sourcedir, 'django_test')
        pp = os.environ.get('PYTHONPATH','').split(os.pathsep)
        if a not in pp:
            pp = [a] + pp
        os.environ['PYTHONPATH'] = os.pathsep.join(pp)
        if a not in sys.path :
            sys.path.insert(0, a)
        os.environ['DJANGO_SETTINGS_MODULE'] = 'django_test.settings'
        django.setup()
        from django.conf import settings
        cls.settings = settings
        cls.info = getattr(settings, 'COLDOC_TASKS_INFOFILE', None)
    #
    def test_daemon(self):
        proc, info_ = coldoc_tasks.coldoc_tasks.tasks_daemon_django_autostart(self.settings)
        self.assertTrue( proc )
        address, authkey = coldoc_tasks.coldoc_tasks.tasks_server_readinfo(self.info)[:2]
        coldoc_tasks.coldoc_tasks.shutdown(address, authkey)

if __name__ == '__main__':
    unittest.main()
