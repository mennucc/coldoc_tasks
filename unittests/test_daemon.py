#!/usr/bin/env -S -- python3 -X tracemalloc

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
import concurrent.futures


try:
    import lockfile
except ImportError:
    lockfile = None


import logging
logger = logging.getLogger(__name__)

testdir = os.path.dirname(os.path.realpath(__file__))
sourcedir = os.path.dirname(testdir)


if __name__ == '__main__':
    if sourcedir not in sys.path:
        sys.path.insert(0, sourcedir)

import coldoc_tasks.simple_tasks, coldoc_tasks.coldoc_tasks as CT

class TestDaemon(unittest.TestCase):
    #
    def test_daemon(self):
        t = tempfile.NamedTemporaryFile(prefix='info_', delete=False)
        info = t.name
        proc, info_ = CT.tasks_daemon_autostart(infofile=info, logfile=True)
        self.assertTrue( info_ == info )
        self.assertTrue( proc )
        address, authkey, info_pid = CT.tasks_server_readinfo(info)[:3]
        #
        def noprint(*k, **v):
            pass
        #
        err = CT.test(address, authkey, print_=noprint)
        self.assertTrue(err == 0)
        #
        CT.shutdown(address, authkey)
        coldoc_tasks.task_utils.proc_join(proc)
        t.close()
        #print(t.name)
        os.unlink(t.name)

    def test_daemon_twice(self):
        t = tempfile.NamedTemporaryFile(prefix='info_', delete=False)
        info = t.name
        # start once, and stop
        proc, info_ = CT.tasks_daemon_autostart(infofile=info, logfile=True)
        self.assertTrue( info_ == info )
        self.assertTrue( proc )
        address, authkey = CT.tasks_server_readinfo(info)[:2]
        ping = CT.ping(address, authkey)
        self.assertTrue( ping )
        CT.shutdown(address, authkey)
        coldoc_tasks.task_utils.proc_join(proc)
        # check that it is off
        ping = CT.ping(address, authkey, warn=False)
        self.assertFalse( ping )
        # start again and stop again
        proc, info_ = CT.tasks_daemon_autostart(infofile=info, logfile=True)
        self.assertTrue( info_ == info )
        self.assertTrue( proc )
        address2, authkey2 = CT.tasks_server_readinfo(info)[:2]
        self.assertTrue( address2 == address)
        self.assertTrue( authkey2 == authkey)
        ping = CT.ping(address, authkey)
        self.assertTrue( ping )
        CT.shutdown(address, authkey)
        coldoc_tasks.task_utils.proc_join(proc)
        t.close()
        #print(open(info).read())
        os.unlink(t.name)

    @unittest.skipIf(lockfile is None,
                     "must install the `lockfile` library for this test")
    def test_daemon_twice_lock(self):
        t = tempfile.NamedTemporaryFile(prefix='info_', delete=False)
        info = t.name
        # start
        def run1(l):
            log1 = tempfile.NamedTemporaryFile(prefix=l, delete=False)
            proc1, info1 = CT.tasks_daemon_autostart(infofile=info, logfile=log1.name)
            self.assertTrue( info1 == info )
            self.assertTrue( proc1 )
            address1, authkey1 = CT.tasks_server_readinfo(info)[:2]
            ping1 = CT.ping(address1, authkey1)
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
        
        self.assertEqual(address1, address2)
        self.assertEqual(authkey1, authkey2)
        
        ## stop
        CT.shutdown(address1, authkey1)
        if not isinstance(proc1,int):
            # avoid an useless warning by joining the subprocess
            coldoc_tasks.task_utils.proc_join(proc1)
        elif not isinstance(proc2,int):
            coldoc_tasks.task_utils.proc_join(proc2)
        else:
            coldoc_tasks.task_utils.proc_join(proc1pid)

        t.close()
        #print(open(info).read())
        os.unlink(t.name)
        
        if 0:
            print('v'*30)
            with open(log1.name) as f1:
                print(f1.read())
            print('^'*30)
            log1.close()
            
            print('v'*30)
            with open(log2.name) as f2:
                print(f2.read())
            print('^'*30)
            log2.close()


if __name__ == '__main__':
    unittest.main()
