"""
run this as
$ python3 unittests/test_fork.py
or
$ python3 -m unittests unittests/test_fork.py
or
$ pytest-3 unittests/test_fork.py 
"""

import os, sys, io, unittest, tempfile, shutil, time
import functools, tempfile, threading, multiprocessing, logging
from os.path import join as osjoin

testdir = os.path.dirname(os.path.realpath(__file__))
sourcedir = os.path.dirname(testdir)

sys.path.insert(0, sourcedir)
#sys.path.append(osjoin(sourcedir,'coldoc_tasks'))

import coldoc_tasks.simple_tasks, coldoc_tasks.coldoc_tasks

from coldoc_tasks.task_utils import __fork_reentrat_test as fork_reentrat_test

def fakesum(a,b,c):
    return a + b + c

def fakediv(a,b):
    return a / b


class Base(object):

    def test_fork_wrong_order(self):
        subproc = self.fork_class()
        with self.assertRaises(Exception):
            subproc.wait()

    def test_fork(self):
        subproc = self.fork_class()
        subproc.run(fakesum, 1, 2, 3)
        r = subproc.wait()
        self.assertEqual(r , 6)

    def test_fork_raises(self):
        subproc = self.fork_class()
        subproc.run(fakediv, 1, 0)
        with self.assertRaises(ZeroDivisionError):
            r = subproc.wait()

    def test_nofork(self):
        subproc = self.fork_class(use_fork=False)
        subproc.run(fakesum, 1, 2, 3)
        r = subproc.wait()
        self.assertEqual(r , 6)

    def test_nofork_raises(self):
        subproc = self.fork_class(use_fork=False)
        subproc.run(fakediv, 1, 0)
        with self.assertRaises(ZeroDivisionError):
            r = subproc.wait()
            
    def test_reentrant(self):
        N = 2
        D = 4
        ff = list(range(N))
        for j in range(N):
            with self.subTest(j=j):
                ff[j] = self.fork_class()
                ff[j].run(fork_reentrat_test, self.fork_class, D,  sleep = (0.2 if j else -1))
        #print("== waiting")
        for j in range(1,N):
            with self.subTest(j=j):
                r = None
                r = ff[j].wait(timeout = 0.3)
                self.assertEqual(r , 'happy')
        with self.subTest(j=0):
            with self.assertRaises(ValueError):
                r = ff[0].wait(timeout = 0.3)


class TestForkSimple(Base,unittest.TestCase):
    fork_class =  coldoc_tasks.simple_tasks.fork_class

#class TestForkSimple_nofork(TestForkBase,unittest.TestCase):
#    fork_class =  functools.partial(coldoc_tasks.simple_tasks.fork_class, use_fork=False)

class TestForkNo(Base,unittest.TestCase):
    fork_class =  coldoc_tasks.simple_tasks.nofork_class


class TestForkColDoc(Base,unittest.TestCase):
    #
    @classmethod
    def setUpClass(cls):
        cls.tempdir = T = tempfile.mkdtemp()
        cls.address = osjoin(T,'socket')
        cls.authkey = os.urandom(9)
        cls.logger = logging.getLogger('coldoc_tasks')
        cls.infofile = osjoin(T,'info')
        d = os.path.dirname(os.path.dirname(__file__))
        if 1: 
            cls.proc = coldoc_tasks.coldoc_tasks.tasks_daemon_autostart(cls.infofile, cls.address, cls.authkey, 
                                                                    tempdir=cls.tempdir,
                                                                    pythonpath=(d),
                                                                    )
            if not cls.proc:
                cls.logger.critical("could not start server")
                raise Exception("could not start server")
        else:
            target = coldoc_tasks.coldoc_tasks.run_server
            cls.proc = multiprocessing.Process(target=target, args=(cls.address, cls.authkey,),
                                               kwargs={'tempdir':cls.tempdir})
            cls.proc.start()
            ok = coldoc_tasks.coldoc_tasks.server_wait(cls.address, cls.authkey, 2.0)
            if not ok:
                cls.logger.critical("could not start server")

        cls.fork_class =  functools.partial(coldoc_tasks.coldoc_tasks.fork_class,
                                             address=cls.address, authkey=cls.authkey)


    @classmethod
    def tearDownClass(cls):
        # remove 
        coldoc_tasks.coldoc_tasks.shutdown(cls.address, cls.authkey)
        coldoc_tasks.coldoc_tasks.tasks_server_join(cls.proc)


if __name__ == '__main__':
    #
    unittest.main()
