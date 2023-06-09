"""
run this as
$ python3 unittests/test_fork.py
or
$ python3 -m unittests unittests/test_fork.py
or
$ pytest-3 unittests/test_fork.py 
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

import coldoc_tasks.simple_tasks, coldoc_tasks.coldoc_tasks, coldoc_tasks.celery_tasks


from coldoc_tasks.task_utils import __fork_reentrat_test as fork_reentrat_test

from fakejobs import *

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
        #
        cls.tempdir = T = tempfile.mkdtemp()
        cls.address = osjoin(T,'socket')
        cls.authkey = os.urandom(9)
        cls.logger = logging.getLogger('coldoc_tasks')
        cls.infofile = osjoin(T,'info')
        #
        if 1: 
            logfile = tempfile.NamedTemporaryFile()
            cls.proc = coldoc_tasks.coldoc_tasks.tasks_daemon_autostart(cls.infofile, cls.address, cls.authkey, 
                                                                    tempdir=cls.tempdir,
                                                                    logfile=logfile.name,
                                                                    pythonpath=(sourcedir,testdir),
                                                                    )
            if not cls.proc:
                cls.logger.critical("could not start server! log follows \n" + ('v' * 70) +\
                                    open(logfile.name).read() + '\n' +  ('^' * 70))
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


    def test_direct_run_cmd(self):
        manager = coldoc_tasks.coldoc_tasks.get_manager(self.address, self.authkey)
        id_ = coldoc_tasks.coldoc_tasks.run_cmd(manager, str, (344,), {})
        coldoc_tasks.coldoc_tasks.wait(id_, manager)
        r = coldoc_tasks.coldoc_tasks.get_result(id_, manager)
        self.assertEqual(r , (0, '344') )


class TestForkCelery(Base,unittest.TestCase):
    #
    @unittest.skipIf(celery is None,
                     "must install the `celery` library for this test")
    @classmethod
    def setUpClass(cls):
        assert celery is not None
        cls.logger = logging.getLogger('celery_tasks')
        cls.logfile = tempfile.NamedTemporaryFile(delete=False)
        cls.celeryconfig = os.path.join(sourcedir,'etc','celeryconfig.py')
        cls.proc = coldoc_tasks.celery_tasks.tasks_daemon_autostart(cls.celeryconfig,
                                                                    logfile=cls.logfile.name,
                                                                    pythonpath=(sourcedir,testdir))
        if not cls.proc:
                cls.logger.critical("could not start server! log follows \n" + ('v' * 70) +\
                                    open(cls.logfile.name).read() + '\n' +  ('^' * 70))
                raise Exception("could not start Celery server")
        cls.fork_class =  functools.partial(coldoc_tasks.celery_tasks.fork_class,
                                            celeryconfig=cls.celeryconfig)

    @classmethod
    def tearDownClass(cls):
        # remove
        app = coldoc_tasks.celery_tasks.get_client(cls.celeryconfig)
        app.control.shutdown()
        if cls.proc is not True and hasattr(cls.proc,'pid'):
            os.kill(cls.proc.pid, signal.SIGTERM)
        os.unlink(cls.logfile.name)


if __name__ == '__main__':
    #
    unittest.main()
