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

sys.path.insert(0, sourcedir)
#sys.path.append(osjoin(sourcedir,'coldoc_tasks'))

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

import coldoc_tasks.simple_tasks, coldoc_tasks.coldoc_tasks, coldoc_tasks.celery_tasks

class TestDaemon(unittest.TestCase):
    #
    def test_daemon(self):
        t = tempfile.NamedTemporaryFile()
        info = t.name
        proc = coldoc_tasks.coldoc_tasks.tasks_daemon_autostart(infofile=info)
        self.assertTrue( proc )
        address, authkey = coldoc_tasks.coldoc_tasks.tasks_server_readinfo(info)[:2]
        coldoc_tasks.coldoc_tasks.shutdown(address, authkey)


@unittest.skipIf(django is None, 'django is not installed')
class TestDjangoDaemon(unittest.TestCase):
    #
    @classmethod
    def setUpClass(cls):
        os.environ['PYTHONPATH'] = os.path.join(sourcedir, 'django_test')
        os.environ['DJANGO_SETTINGS_MODULE'] = 'django_test.settings'
        django.setup()
        from django.conf import settings
        cls.settings = settings
        cls.info = getattr(settings, 'COLDOC_TASKS_INFOFILE', None)
    #
    def test_daemon(self):
        proc = coldoc_tasks.coldoc_tasks.tasks_daemon_django_autostart(self.settings)
        self.assertTrue( proc )
        address, authkey = coldoc_tasks.coldoc_tasks.tasks_server_readinfo(self.info)[:2]
        coldoc_tasks.coldoc_tasks.shutdown(address, authkey)

if __name__ == '__main__':
    #
    unittest.main()
