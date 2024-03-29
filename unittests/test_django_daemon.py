#!/usr/bin/env  -S --  python3 -X tracemalloc

"""
run this as
$ python3 unittests/test_cli.py
or
$ python3 -m unittests unittests/test_cli.py
or
$ pytest-3 unittests/test_cli.py 
"""

import os, sys, io, unittest, tempfile, shutil, time, concurrent
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
    import lockfile
except ImportError:
    logger.error('Cannot import `lockfile`')
    lockfile = None

try:
    import django
except ImportError:
    logger.error('Cannot import `django`')
    django = None

if __name__ == '__main__':
    if sourcedir not in sys.path:
        sys.path.insert(0, sourcedir)

import coldoc_tasks.simple_tasks, coldoc_tasks.coldoc_tasks


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


    @unittest.skipIf(lockfile is None,
                     "must install the `lockfile` library for this test")
    def test_daemon_twice_lock(self):
        #settings = os.path.join(sourcedir, 'django_test',' settings.py')
        info = self.info
        # start
        def run1(l):
            log1 = tempfile.NamedTemporaryFile(prefix=l, delete=False)
            proc1, info1 = coldoc_tasks.coldoc_tasks.tasks_daemon_django_autostart(self.settings)
            self.assertEqual( info1, info )
            self.assertTrue( proc1 )
            address1, authkey1 = coldoc_tasks.coldoc_tasks.tasks_server_readinfo(info)[:2]
            ping1 = coldoc_tasks.coldoc_tasks.ping(address1, authkey1)
            self.assertTrue( ping1 )
            return log1,proc1,info1,address1,authkey1,ping1
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            f1 = executor.submit(run1, 'log1')
            f2 = executor.submit(run1, 'log2')
            r1 = f1.result()
            r2 = f2.result()
            #print('=== r1',r1)
            #print('=== r2',r2)
        log1,proc1,info1,address1,authkey1,ping1 = r1
        log2,proc2,info2,address2,authkey2,ping2 = r2
        
        self.assertEqual( info2, info )
        self.assertEqual( info1, info )
        
        proc1pid =  getattr(proc1,'pid',proc1)
        proc2pid =  getattr(proc2,'pid',proc2)
        self.assertEqual( proc1pid, proc2pid )
        #print( proc1pid, proc2pid )
        #print( proc1, proc2 )
        
        self.assertEqual(address1, address2)
        self.assertEqual(authkey1, authkey2)
        
        ## stop
        coldoc_tasks.coldoc_tasks.shutdown(address1, authkey1)
        if not isinstance(proc1,int):
            # avoid an useless warning by joining the subprocess
            coldoc_tasks.task_utils.proc_join(proc1)
        elif not isinstance(proc2,int):
            coldoc_tasks.task_utils.proc_join(proc2)
        else:
            coldoc_tasks.task_utils.proc_join(proc1pid)


if __name__ == '__main__':
    unittest.main()
