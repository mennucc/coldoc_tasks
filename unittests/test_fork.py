#!/usr/bin/env -S -- python3 -X tracemalloc

"""
run this as
$ python3 unittests/test_fork.py
or
$ python3 -m unittests unittests/test_fork.py
or
$ pytest-3 unittests/test_fork.py 
"""

import os, sys, io, unittest, tempfile, shutil, time, traceback
import functools, tempfile, threading, multiprocessing, logging, signal
from os.path import join as osjoin

import logging
logger = logging.getLogger(__name__)

testdir = os.path.dirname(os.path.realpath(__file__))
sourcedir = os.path.dirname(testdir)

if __name__ == '__main__':
    if sourcedir not in sys.path:
        sys.path.insert(0, sourcedir)

try:
    import celery
except ImportError:
    logger.error('Cannot import `celery`')
    celery = None

import coldoc_tasks.simple_tasks, coldoc_tasks.coldoc_tasks, coldoc_tasks.celery_tasks


from coldoc_tasks.task_utils import format_exception, __fork_reentrat_test as fork_reentrat_test
from coldoc_tasks.exceptions import ColdocTasksTimeoutError

from unittests.fakejobs import *

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

    def test_fork_queue(self):
        subproc = self.fork_class(queue=True)
        subproc.run(fakesum, 1, 7, 3)
        r = subproc.wait()
        self.assertEqual(r , 11)
        self.assertTrue(subproc.exception is None)
        self.assertTrue(subproc.traceback is None)


    def test_fork_raises(self):
        subproc = self.fork_class()
        subproc.run(fakediv, 1, 0)
        ### no, this clears the traceback
        #with self.assertRaises(ZeroDivisionError) as cm:
        #    subproc.wait()
        #print('  CM   traceback',  traceback.format_exception(cm.exception))
        #
        try:
            r = subproc.wait()
        except ZeroDivisionError as exc:
            logger.debug('local  traceback %r', format_exception(exc))
        else:
            self.fail("ZeroDivisionError was not raised")
        logger.debug('remote traceback %r', subproc.traceback)
        self.assertIsInstance(subproc.exception, ZeroDivisionError)
        self.assertIsInstance(subproc.traceback, list)
 
    def test_fork_doesnt_raise(self):
        subproc = self.fork_class()
        subproc.run(fakediv, 1, 0)
        r = subproc.wait(raise_exception=False)
        self.assertIsInstance(subproc.exception, ZeroDivisionError)
        self.assertTrue(subproc.exception == r)
        self.assertIsInstance(subproc.traceback, list)

    def test_nofork(self):
        subproc = self.fork_class(use_fork=False)
        subproc.run(fakesum, 1, 2, 3)
        r = subproc.wait()
        self.assertEqual(r , 6)
        self.assertTrue(subproc.exception is None)
        self.assertTrue(subproc.traceback is None)

    def test_nofork_raises(self):
        subproc = self.fork_class(use_fork=False)
        subproc.run(fakediv, 1, 0)
        with self.assertRaises(ZeroDivisionError):
            r = subproc.wait()
        self.assertIsInstance(subproc.exception, ZeroDivisionError)
        self.assertIsInstance(subproc.traceback, list)

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
        cls.tempdir = T = tempfile.mkdtemp(prefix='test_fork_')
        logger.info('TestForkColDoc uses % r', T)
        cls.address = osjoin(T,'socket')
        cls.authkey = os.urandom(9)
        cls.logger = logging.getLogger('coldoc_tasks')
        cls.infofile = osjoin(T,'info')
        cls.logfile = tempfile.NamedTemporaryFile(prefix='coldoc_log_', delete=True, dir=T)
        #
        if 1: 
            cls.proc, info_ = coldoc_tasks.coldoc_tasks.tasks_daemon_autostart(cls.infofile,
                                                                               address=cls.address, authkey=cls.authkey,
                                                                    tempdir=cls.tempdir,
                                                                    logfile=cls.logfile.name,
                                                                    pythonpath=(sourcedir,testdir),
                                                                    )
            if not cls.proc:
                with open(cls.logfile.name) as logfile_fd:
                    logfile_content = logfile_fd.read()
                cls.logger.critical("could not start server! log follows \n" + ('v' * 70) +\
                                    logfile_content + '\n' +  ('^' * 70))
                raise Exception("could not start server")
        else:
            target = coldoc_tasks.coldoc_tasks.run_server
            cls.proc = multiprocessing.Process(target=target, args=(cls.address, cls.authkey,),
                                               kwargs={'tempdir':cls.tempdir})
            cls.proc.start()
            kwargs = coldoc_tasks.coldoc_tasks.server_wait(cls.address, cls.authkey, 2.0)
            ok = kwargs['return_code']
            if not ok:
                cls.logger.critical("could not start server")

    @property
    def fork_class(self, **v):
        return functools.partial(coldoc_tasks.coldoc_tasks.fork_class,
                                 address=self.address, authkey=self.authkey)

    @classmethod
    def tearDownClass(cls):
        # remove 
        coldoc_tasks.coldoc_tasks.shutdown(cls.address, cls.authkey)
        coldoc_tasks.task_utils.proc_join(cls.proc)
        cls.logfile.close()
        del cls.logfile
        shutil.rmtree(cls.tempdir)

    def test_timeout(self):
        f = self.fork_class()
        f.run(time.sleep, 3)
        with self.assertRaises(ColdocTasksTimeoutError):
            r = f.wait(timeout=1.)


    def test_direct_run_cmd(self):
        manager = coldoc_tasks.coldoc_tasks.get_manager(self.address, self.authkey)
        id_ = coldoc_tasks.coldoc_tasks.run_cmd(manager, str, (344,), {})
        coldoc_tasks.coldoc_tasks.wait(id_, manager)
        r = coldoc_tasks.coldoc_tasks.get_result(id_, manager)
        self.assertEqual(r , (0, '344', None) )


class TestForkCelery(Base,unittest.TestCase):
    #
    @unittest.skipIf(celery is None,
                     "must install the `celery` library for this test")
    @classmethod
    def setUpClass(cls):
        assert celery is not None
        cls.logger = logging.getLogger('celery_tasks')
        cls.logfile = tempfile.NamedTemporaryFile(prefix='celery_log_', delete=False)
        cls.celeryconfig = os.path.join(sourcedir,'etc','celeryconfig.py')
        cls.proc = coldoc_tasks.celery_tasks.tasks_daemon_autostart(cls.celeryconfig,
                                                                    logfile=cls.logfile.name,
                                                                    pythonpath=(sourcedir,testdir))
        if not cls.proc:
                with open(cls.logfile.name) as logfile_fd:
                    logfile_content = logfile_fd.read()
                cls.logger.critical("could not start server! log follows \n" + ('v' * 70) +\
                                    logfile_content + '\n' +  ('^' * 70))
                raise Exception("could not start Celery server")
            
    @property
    def fork_class(self, **v):
        return  functools.partial(coldoc_tasks.celery_tasks.fork_class,
                                  celeryconfig=self.celeryconfig)

    @classmethod
    def tearDownClass(cls):
        # remove
        coldoc_tasks.celery_tasks.shutdown(cls.celeryconfig)
        coldoc_tasks.task_utils.proc_join(cls.proc)
        cls.logfile.close()
        os.unlink(cls.logfile.name)

    def test_timeout(self):
        f = self.fork_class()
        f.run(time.sleep, 3)
        with self.assertRaises(ColdocTasksTimeoutError):
            r = f.wait(timeout=1.)


if __name__ == '__main__':
    #
    unittest.main()
