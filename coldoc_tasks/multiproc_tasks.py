########################################################################
# WARNING THIS CODE IS NOT YET USED, IT IS UNFINISHED AND POSSIBLY BUGGY
########################################################################

import sys, os, signal, tempfile, pickle, logging, functools, multiprocessing, traceback

import coldoc_tasks.simple_tasks, coldoc_tasks.task_utils
from .task_utils import format_exception

import logging
logger = logging.getLogger(__name__)

def _wrapper(cmd, k, v, t):
    try:
        ret = cmd(*k, **v)
        ret = (0,ret, None)
    except Exception as E:
        ret = (1,E, format_exception(E))
        #logger.exception('class_fork.run , insider forked pid %r, cmd %r , got exception as follows:', os.getpid(), cmd)
    with open(t,'wb') as f:
        pickle.dump(ret, f)

class fork_class(coldoc_tasks.simple_tasks.fork_class_base):
    "class that runs a job in a multiprocessing subprocess, and returns results or raises exception"
    fork_type = 'multiproc'
    def __init__(self, use_fork = True):
        super().__init__(use_fork = use_fork)
        self.tempfile_name = None
        self.__proc = None
        self.__my_pid    = os.getpid()
        self.__other_pid = None
        self.__ret = (2 , RuntimeError('Program bug'), None)
        self.__signal = None
        self.__pickle_exception = None
        # __del__ methods may be run after modules are gc
        self.os_unlink = os.unlink
    #
    @staticmethod
    def can_fork():
        return True
    #
    @property
    def subprocess_pid(self):
        return  self.__other_pid
    #
    @property
    def task_id(self):
        return str(self.__other_pid)
    #
    def kill(self):
        if self.__proc and not self.already_wait:
            self.__proc.kill()
    #
    def terminate(self):
        if self.__proc and not self.already_wait:
            self.__proc.terminate()
    #
    def run(self, cmd, *k, **v):
        assert self.already_run is False
        self.__cmd = cmd
        self.__k = k
        self.__v = v
        #
        if self.use_fork_:
          with tempfile.NamedTemporaryFile(prefix='forkret',delete=False) as _tempfile:
            self.tempfile_name = _tempfile.name
            with open(_tempfile.name,'wb') as f:
                pickle.dump((2,None, None), f)
            self.__proc = multiprocessing.Process(target=_wrapper, args=(cmd, k, v, _tempfile.name))
            self.__proc.start()
            self.__other_pid = self.__proc.pid
        else:
            try:
                self.__ret = (0, cmd(*k, **v), None)
            except Exception as e:
                self.__ret = (1, e, format_exception(e))
        self.already_run = True
    def wait(self, timeout=None, raise_exception=True):
        assert self.already_run is True
        if self.use_fork_ and not self.already_wait:
            self.__proc.join(timeout)
            s = self.__proc.exitcode
            if s is None:
                # timeout on join
                raise coldoc_tasks.task_utils.TimeoutError('For cmd %r job %r ', self.__cmd, self.__proc)
            self.already_wait = True
            a = []
            if s < 0:
                import signal
                self.__signal = sig = signal.Signals(-s)
                a.append('Got signal %r.' % (sig,))
                self.__ret = (2,  RuntimeError(' '.join(a)), None)
            else:
                try:
                    with open(self.tempfile_name,'rb') as f:
                        self.__ret = pickle.load(f)
                    self.os_unlink(self.tempfile_name)
                except Exception as E:
                    self.__pickle_exception = E
                    m = 'In pid %r cannot read exit status %r for pid %r: %s ' % \
                        (self.__my_pid, self.tempfile_name, self.__other_pid, E,)
                    a.append(m)
                    logger.warning(m)
            #
            self.__del()
            #
            if self.__ret[0] == 2:
                    a.append('Child did not terminate correctly (but no signal was detected), unknown exit status. ')
                    self.__ret = (2 , RuntimeError(' '.join(a)), None)
            #
            self.__del()
            #
            if len(a)>1:
                logger.error(' '.join(a))
        if self.__ret[0] :
            self.traceback_ = self.__ret[2]
            self.exception_ = self.__ret[1]
            if raise_exception:
                raise self.__ret[1]
        return self.__ret[1]
    ## this is not __del__  
    def __del(self):
        if self.tempfile_name is not None:
            try:
                self.os_unlink(self.tempfile_name)
                self.tempfile_name = None
            except FileNotFoundError:
                pass
