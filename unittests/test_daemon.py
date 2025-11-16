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

import logging
logger = logging.getLogger(__name__)

testdir = os.path.dirname(os.path.realpath(__file__))
sourcedir = os.path.dirname(testdir)

if testdir not in sys.path:
    sys.path.insert(0, testdir)


if __name__ == '__main__':
    if sourcedir not in sys.path:
        sys.path.insert(0, sourcedir)

import coldoc_tasks.coldoc_tasks as CT
import coldoc_tasks.task_utils as TU
## currently unused
#import coldoc_tasks.simple_tasks
#import coldoc_tasks.wrap_lockfile as WL

from unittests.fakejobs import *

class TestDaemon(unittest.TestCase):
    #
    def test_daemon(self):
        t = tempfile.NamedTemporaryFile(prefix='info_', delete=False)
        info = t.name
        proc, info_ = CT.tasks_daemon_autostart(infofile=info, logfile=True)
        self.assertTrue( info_ == info )
        self.assertTrue( proc )
        address, authkey, info_pid = CT.tasks_server_readinfo(info)[:3]
        real_pid = CT.server_pid(address, authkey)
        self.assertEqual(info_pid, real_pid)
        status =  CT.status(address, authkey)
        self.assertIsInstance(status, dict)
        #
        def noprint(*k, **v):
            pass
        #
        err = CT.test(address, authkey, print_=noprint)
        #
        CT.shutdown(address, authkey)
        TU.proc_join(proc)
        t.close()
        #print(t.name)
        os.unlink(t.name)
        #
        self.assertTrue(err == 0)

    def test_daemon_twice(self):
        t = tempfile.NamedTemporaryFile(prefix='info_', delete=False)
        info = t.name
        # start once, and stop
        proc, info_ = CT.tasks_daemon_autostart(infofile=info, logfile=True)
        self.assertTrue( info_ == info )
        self.assertTrue( proc )
        address, authkey = CT.tasks_server_readinfo(info)[:2]
        ping1 = CT.ping(address, authkey)
        CT.shutdown(address, authkey)
        TU.proc_join(proc)
        # check that it is off
        ping2 = CT.ping(address, authkey, warn=False)
        # start again and stop again
        proc, info_ = CT.tasks_daemon_autostart(infofile=info, logfile=True)
        self.assertTrue( info_ == info )
        self.assertTrue( proc )
        address2, authkey2 = CT.tasks_server_readinfo(info)[:2]
        self.assertTrue( address2 == address)
        self.assertTrue( authkey2 == authkey)
        ping3 = CT.ping(address, authkey)
        CT.shutdown(address, authkey)
        TU.proc_join(proc)
        ping4 = CT.ping(address, authkey)
        # clean up
        t.close()
        os.unlink(t.name)
        #
        # asserts at the end
        self.assertTrue( ping1 )
        self.assertFalse( ping2 )
        self.assertTrue( ping3 )
        self.assertFalse( ping4 )


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
            TU.proc_join(proc1)
        elif not isinstance(proc2,int):
            TU.proc_join(proc2)
        else:
            TU.proc_join(proc1pid)

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
    def test_status_reports_waiting_task(self):
        # test using direct plumbing calls
        direct=True
        #
        with  tempfile.NamedTemporaryFile(prefix='info_status_', delete=False) as info_file:
            info = info_file.name
            proc, info_ = CT.tasks_daemon_autostart(infofile=info, logfile=True)
        self.assertEqual(info, info_)
        self.assertTrue(proc)
        address, authkey = CT.tasks_server_readinfo(info)[:2]
        tmpdir = tempfile.mkdtemp(prefix='status_tmp_')
        target = os.path.join(tmpdir, 'ready.txt')
        if direct:
            manager = CT.get_manager(address, authkey)
            proxy_id = manager.run_cmd__(wait_for_file, (target,), {'timeout': 10.0})
            task_id = proxy_id._getvalue()
            self.assertTrue(task_id)
        else:
            fork = CT.fork_class(address, authkey)
            fork.run(wait_for_file, target, timeout= 10.0)
            task_id = fork.task_id
        self.assertFalse(os.path.exists(target))
        time.sleep(0.1)
        status = CT.status(address, authkey)
        self.assertIn('processes', status)
        processes = status['processes']
        self.assertIn(task_id, processes)
        entry = processes[task_id]
        self.assertIn('socket', entry)
        # let the task finish by creating the file it is waiting for
        with open(target, 'w', encoding='utf-8') as handle:
            handle.write('go')
        if direct:
            result_proxy = manager.get_result_join__(task_id)
            result = result_proxy._getvalue()
            self.assertEqual(result[0], 0)
            self.assertEqual(result[1], target)
        else:
            fork.wait()
        #
        status = CT.status(address, authkey)
        processes = status['processes']
        self.assertEqual(processes,{})
        # 
        CT.shutdown(address, authkey)
        TU.proc_join(proc)
        info_file.close()
        os.unlink(info_file.name)
        shutil.rmtree(tmpdir)


if __name__ == '__main__':
    unittest.main()
