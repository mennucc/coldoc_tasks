import sys, os, signal, tempfile, pickle, logging, functools

logger = logging.getLogger(__name__)

from .task_utils import format_exception

############ class for forking

if (sys.platform != 'linux'):
    def waitstatus_to_exitcode(status):
        # FIXME  find a viable alternative
        return 0
elif sys.version_info >= (3,9):
    from os import waitstatus_to_exitcode
else:
    def waitstatus_to_exitcode(status):
        return os.WEXITSTATUS(status) if os.WIFEXITED(status) else \
               ( - os.WTERMSIG(status) if os.WIFSIGNALED(status) else None)

class fork_class_base(object):
    def __init__(self, use_fork = True):
        self.already_run = False
        self.already_wait = False
        self.use_fork_ = use_fork and self.can_fork()
        self.traceback_ = None
        self.exception_ = None
    #
    # must be defined in each class, as a static method
    #def can_fork(self):
    #        return self.can_fork_
    #
    @property
    def use_fork(self):
        return self.use_fork_
    @use_fork.setter
    def use_fork(self,v):
        assert self.already_run is False
        self.use_fork_ = v and self.can_fork()
    @property
    def traceback(self):
        "returns the exception (if any)"
        return self.traceback_
    @property
    def exception(self):
        "return the traceback, as a list of strings"
        return self.exception_

#####################################

class fork_class(fork_class_base):
    "class that runs a job in a forked subprocess, and returns results or raises exception"
    fork_type = 'simple'
    def __init__(self, use_fork = True, timeout=None, queue=None):
        super().__init__(use_fork = use_fork)
        self.tempfile_name = None
        self.__my_pid    = os.getpid()
        self.__other_pid = None
        self.__ret = (2 , RuntimeError('Program bug'), None)
        self.__signal = None
        self.__pickle_exception = None
        # __del__ methods may be run after modules are gc
        self.os_unlink = os.unlink
        #
        self.__timeout = timeout
    #
    @staticmethod
    def can_fork():
        # FIXME find a viable alternative
        return hasattr(os, 'fork')
    #
    @property
    def subprocess_pid(self):
        return  self.__other_pid
    #
    def kill(self, signal_ = signal.SIGTERM):
        if self.__other_pid and not self.already_wait:
            os.kill(self.__other_pid, signal_)
    #
    def terminate(self, signal_ = None):
        if signal_ is None:
            signal_  = getattr(signal, 'SIGKILL', signal.SIGTERM)
        if self.__other_pid and not self.already_wait:
            os.kill(self.__other_pid, signal_)
    #
    def run(self, cmd, *k, **v):
        assert self.already_run is False
        self.__cmd = cmd
        self.__k = k
        self.__v = v
        #
        if self.use_fork_:
            _tempfile = tempfile.NamedTemporaryFile(prefix='forkret',delete=False)
            self.tempfile_name = _tempfile.name
            with open(_tempfile.name,'wb') as f:
                pickle.dump((2,None), f)
            self.__other_pid = os.fork()
            if self.__other_pid == 0:
                self.__other_pid = os.getpid()
                logger.debug('class_fork.run , inside forked  newpid %r now starting %r , file %r .',
                             self.__other_pid, self.__cmd, self.tempfile_name)
                try:
                    ret = cmd(*k, **v)
                    ret = (0,ret, None)
                except Exception as e:
                    #logger.exception('class_fork.run , insider forked pid %r, cmd %r got exception as follows :', os.getpid(), self.__cmd)
                    ret = (1, e, format_exception(e))
                with open(self.tempfile_name,'wb') as f:
                    pickle.dump(ret, f)
                # avoid deleting the file
                # self.tempfile_name = None
                # (altough __del__ is not called)
                os._exit(0)
                # or maybe
                # sys.exit(0)
            else:
                logger.debug('class_fork.run , forked oldpid %r to newpid %r to start %r file %r .',
                               self.__my_pid, self.__other_pid, self.__cmd, self.tempfile_name)
        else:
            try:
                self.__ret = (0, cmd(*k, **v), None)
            except Exception as e:
                self.__ret = (1, e, format_exception(e))
        self.already_run = True
    def wait(self, timeout=None, raise_exception=True):
        timeout = self.__timeout if timeout is None else timeout
        assert self.already_run is True
        if self.use_fork_ and not self.already_wait:
            a = [('fork_class, cmd %r pid %r.' % (self.__cmd, self.__other_pid))]
            logger.debug('fork_class.wait,  my_pid %r waits for__other_pid %r, cmd %r , file %r .',
                         self.__my_pid, self.__other_pid, self.__cmd, self.tempfile_name)
            if timeout is not None:
                logger.warning('Timeout is not implemented in simple_tasks.fork_class')
            try:
                pid_, exitstatus_ = os.waitpid(self.__other_pid, 0)
                if pid_ != self.__other_pid:
                    logger.error('internal error lnkanla19')
                s = waitstatus_to_exitcode(exitstatus_)
            except ChildProcessError:
                logger.warning('Child %r has disappeared, unknown exit status', self.__other_pid)
                a.append('Child has disappeared, unknown exit status.')
                s = 0
            self.already_wait = True
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
                    self.__ret = (2 , None, None) # overwritten below
                if self.__ret[0] == 2:
                    a.append('Child did not terminate correctly (but no signal was detected), unknown exit status. ')
                    self.__ret = (2 , RuntimeError(' '.join(a)), None)
            #
            self.__del()
            #
            if len(a)>1:
                logger.error(' '.join(a))
        if self.__ret[0]:
            self.traceback_ = self.__ret[2]
            self.exception_ = self.__ret[1]
            if raise_exception:
                raise self.__ret[1]
        return self.__ret[1]
    ## this is not __del__  
    def __del(self):
        logger.debug('fork_class.__del cmd %r __my_pid %r getpid %r , rm file %r .',
                       self.__cmd, self.__my_pid, os.getpid(), self.tempfile_name)
        if self.tempfile_name is not None:
            try:
                self.os_unlink(self.tempfile_name)
                self.tempfile_name = None
            except FileNotFoundError:
                pass


######################################
class nofork_class(fork_class_base):
    "class that runs a job as usual, and returns results or raises exception"
    fork_type = 'nofork'
    def __init__(self, use_fork = True, timeout=None, queue=None):
        super().__init__(use_fork = use_fork)
        self.__ret = (2 , RuntimeError('Program bug'), None)
        self.__pickle_exception = None
    #
    @staticmethod
    def can_fork():
        return False
    #
    def run(self, cmd, *k, **v):
        assert self.already_run is False
        try:
            self.__ret = (0, cmd(*k, **v), None)
        except Exception as e:
            self.__ret = (1, e, format_exception(e))
        self.already_run = True
    def wait(self, timeout=None,  raise_exception=True):
        assert self.already_run is True
        if self.__ret[0] :
            self.traceback_ = self.__ret[2]
            self.exception_ = self.__ret[1]
            if raise_exception:
                raise self.__ret[1]
        return self.__ret[1]

########

def main(argv):
    if argv  and argv[0] == 'test':
        from task_utils import test_fork
        print('*' * 80)
        logger.setLevel(logging.INFO)
        ret = test_fork(fork_class)
        print('*' * 80)
        F = functools.partial(fork_class, use_fork = False)
        ret += test_fork(fork_class=F)
        print('*' * 80)
        sys.exit(1 if ret else 0)
    else:
        print(""" Commands:
%s test

""" % (sys.argv[0],))


if __name__ == '__main__':
    argv = sys.argv[1:]
    main(argv)
