import sys, os, time, pickle, functools, base64, copy, contextlib
from os.path import join as osjoin
from pathlib import Path

import logging
logger = logging.getLogger(__name__)

try:
    import celery
except ImportError:
    celery = None

try:
    import psutil
except ImportError:
    psutil = None

def mylockfile(fil, timeout=None):
    " fake lockfile context "
    return contextlib.nullcontext()

myLockTimeout = Exception
mylockfile_other_exceptions = ()

try:
    import lockfile
    mylockfile = lockfile.FileLock
    myLockTimeout = lockfile.LockTimeout
    mylockfile_other_exceptions = (lockfile.LockTimeout ,
                                   lockfile.AlreadyLocked,
                                   lockfile.LockFailed)
    #class    mylockfile(lockfile.FileLock):
except ImportError:
    lockfile = None

####

default_chmod = 0o600

def mychmod(f, mode=default_chmod):
    try:
        os.chmod(f, mode)
    except Exception as E:
        logger.exception('Why cant I set chmod %r on %r', mode, f)




####

def proc_join(proc):
    " join, that is, wait for subprocess to end; to be used after `shutdown` is called, to avoid zombies"
    if isinstance(proc, bool):
        # FIXME in Celery, if the process is running, we don't know its PID
        logger.warning("Don't know the true process id, can't wait")
    elif isinstance(proc, int):
        if psutil and not psutil.pid_exists(proc):
            return
        for j in range(5):
            #print('wait %r' % proc)
            pid , wstatus = os.waitpid(proc, os.WNOHANG)
            #if pid != proc:
            #    logger.error(' pid %r  != proc %r ', pid, proc)
            if pid == proc and ( os.WIFEXITED(wstatus) or os.WIFSIGNALED(wstatus) ):
                 return
            logger.info('waitpid(%r) returned pid %r status %r ; waiting more', proc, pid, wstatus)
            time.sleep(0.5)
    elif hasattr(proc, 'join'):
        proc.join()
    elif hasattr(proc, 'wait'):
        proc.wait()
    else:
        logger.warning("Don't know how to wait for process %r", proc)



####

all_fork_classes = ('celery','coldoc','simple','nofork')

def _choose_best_fork_class(infofile=None, celeryconfig=None,
                           preferences=('celery','coldoc','simple')):
    """ returns the first working `fork_class` in the list `preferences` """
    #
    import coldoc_tasks.simple_tasks
    #
    if isinstance(preferences, str):
        preferences = preferences.split(',')
    if set(preferences).difference(all_fork_classes):
        logger.error('`preferences` contains unknown classes: %r', set(preferences).difference(all_fork_classes))
    ok = False
    for j in preferences:
        if celery and celeryconfig and j == 'celery':
            import coldoc_tasks.celery_tasks
            ok = coldoc_tasks.celery_tasks.ping(celeryconfig)
            if not ok:
                logger.critical('Celery backend cannot be contacted')
            else:
                fork_class = functools.partial(coldoc_tasks.celery_tasks.fork_class, celeryconfig=celeryconfig)
                fork_class.fork_type = coldoc_tasks.celery_tasks.fork_class.fork_type
                return fork_class
        #
        if infofile  and j == 'coldoc':
            import coldoc_tasks.coldoc_tasks
            ok, tasks_sock, tasks_auth, tasks_pid = coldoc_tasks.coldoc_tasks.task_server_check(infofile)
            if not ok:
                logger.critical('Coldoc Tasks backend cannot be contacted')
            else:
                fork_class = functools.partial(coldoc_tasks.coldoc_tasks.fork_class,address=tasks_sock, authkey=tasks_auth)
                fork_class.fork_type = coldoc_tasks.coldoc_tasks.fork_class.fork_type
                return fork_class
        if j == 'simple':
            return coldoc_tasks.simple_tasks.fork_class
        if j == 'nofork':
            return coldoc_tasks.simple_tasks.nofork_class
    return coldoc_tasks.simple_tasks.nofork_class


class choose_best_fork_class(object):
    def __init__(self, infofile=None, celeryconfig=None,
                 preferences=('celery','coldoc','simple'),
                 refresh_time_interval = 120,
                 callback = None
                 ):
        """
        Returns a callable instance,         when called, it
        returns the first working `fork_class` in the list `preferences`.
        
        Every `refresh_time_interval` seconds, the choice is reverified  by pinging servers;
        `callback(fork_class)` is called when a choice is made.
        """
        self.infofile = infofile
        self.infofile_mtime = None
        if isinstance(infofile, (str,bytes,Path)) and os.path.isfile(infofile):
            self.infofile_mtime = os.path.getmtime(infofile)
        #
        self.celeryconfig = celeryconfig
        self.celeryconfig_mtime = None
        if isinstance(celeryconfig, (str,bytes,Path)) and os.path.isfile(celeryconfig):
            self.celeryconfig_mtime = os.path.getmtime(celeryconfig)
        #
        self.preferences = preferences
        self.refresh_time_interval = refresh_time_interval
        self.callback = callback
        #
        self.last_fork_class = None
        self.fork_type = 'undecided'
        self.time_choice = 0
    #
    def __call__(self, *args, **kwargs):
        t = time.time()
        refresh = self.last_fork_class is None or \
            ( t > self.time_choice + self.refresh_time_interval)
        if not refresh and self.infofile_mtime and self.infofile_mtime != os.path.getmtime(self.infofile):
            self.infofile_mtime = os.path.getmtime(self.infofile)
            logger.info('infofile was changed: %r', self.infofile)
            refresh = True
        if not refresh and self.celeryconfig_mtime and self.celeryconfig_mtime != os.path.getmtime(self.celeryconfig):
            self.celeryconfig_mtime = os.path.getmtime(self.celeryconfig)
            logger.info('celeryconfig was changed: %r', self.celeryconfig)
            refresh = True
        #
        if refresh:
            self.time_choice = t
            self.last_fork_class = _choose_best_fork_class(self.infofile, self.celeryconfig, self.preferences)
            self.fork_type = self.last_fork_class.fork_type
            logger.info('Refreshing choice of `fork_class` to %r', self.fork_type)
            assert self.fork_type in all_fork_classes
            if self.callback:
                self.callback(self.last_fork_class)
        return self.last_fork_class(*args, **kwargs)


######################################

def _normalize_pythonpath(pythonpath):
    if isinstance(pythonpath, Path):
        pythonpath = [pythonpath]
    elif isinstance(pythonpath, str):
        pythonpath = pythonpath.split(os.pathsep)
    elif isinstance(pythonpath, bytes):
        pythonpath = pythonpath.split(os.pathsep.encode())
    assert isinstance(pythonpath, (list, tuple))
    # convert Paths to str
    pythonpath = list(map(lambda x: str(x) if isinstance(x,Path) else x, pythonpath))
    # convert bytes to str
    pythonpath = list(map(lambda x: x.decode('utf8','replace') if isinstance(x,bytes) else x, pythonpath))
    return pythonpath


def __fork_reentrat_test(fork_class, depth = 3,  sleep=0.1):
    assert isinstance(depth,int) and depth >= 0
    f = fork_class()
    if depth > 0:
        logger.info(' reentring at depth %d', depth)
        f.run(__fork_reentrat_test, fork_class, depth -1, sleep)
    elif sleep > 0:
        logger.info(' sleeping at depth 0 for %r', sleep)
        f.run(time.sleep, sleep)
    else:
        logger.info(' raising ValueError at depth 0')
        raise ValueError('Emitted ValueError to test')
    ret = f.wait(timeout=(2. * sleep + 3.))
    return 'happy' if (sleep > 0 and depth == 0) else ret

############################

def test_fork(fork_class, print_ = print):
    ret = 0
    print_("==== test : return 3.14")
    f = fork_class()
    f.run(str,3.14)
    try:
        s = pickle.dumps(f)
        f = pickle.loads(s)
    except Exception as E:
        print_('** cannot pickle : {!r}'.format(E))
        ret += 1
    r = f.wait()
    if r != '3.14':
        ret += 1
        print_('Returned wrong value {!r}'.format(r))
    else:
        print_('Returned {!r}'.format(r))
    #
    if 1:
        print_("==== test : subprocess raises exception")
        f = fork_class()
        f.run(eval,'0/0')
        try:
            r = f.wait()
        except  ZeroDivisionError:
            print_('caught')
        else:
            print_('WRONG: Returned  {!r}'.format(r))
            ret += 1
    #
    if ret:
        print_(' ** skipping reentrant test, already errors')
    else:
        N = 2
        D = 4
        print_("======= test : check against self locking, instances = {} depth = {}".format(N,D))
        print_("== scheduling")
        ff = list(range(N))
        for j in range(N):
            ff[j] = fork_class()
            ff[j].run(__fork_reentrat_test, fork_class, D,  sleep = (0.2 if j else -1))
        print_("== waiting")
        for j in range(N):
            r = None
            try:
                r = ff[j].wait(timeout = 0.3)
            except ValueError as E:
                if not j:
                    print_(' Caught {!r}, as expected'.format(E))
                else:
                    print_('Failure {!r}'.format(E))
                    ret += 1
            except Exception as E:
                ret += 1
                print_('Failure {!r}'.format(E))
            else:
                if r != 'happy' :
                    print_('Wrong return value {!r}'.format(r))
    #
    if ret:
        print_(' ** skipping speed test, already errors')
    else:
        N = 256
        t = time.time()
        print_("======= speed test instances = {}".format(N))
        print_("== scheduling")
        ff = list(range(N))
        for j in range(N):
            #sys.stderr.write('\r {}  \r'.format(j))
            ff[j] = fork_class()
            ff[j].run(int,'4')
        print_("== scheduling , time per instance {!r} sec".format((time.time() - t) / N))
        print_("== waiting")
        for j in range(N):
            #sys.stderr.write('\r {}  \r'.format(j))
            r = ff[j].wait()
            if r != 4:
                ret += 1
        t = time.time() - t
        print_("======= speed test, fork_type {!r}, total time per instance {!r} sec".format(ff[0].fork_type, t / N))
    #
    if f.use_fork and hasattr(fork_class,'terminate'):
        print_("==== test : terminate subprocess")
        f = fork_class()
        f.run(time.sleep,2)
        f.terminate()
        r = None
        try:
            r = f.wait()
        except RuntimeError as R:
            print_('As expected, raised: {!r}'.format(R))
        else:
            print_('WRONG: Returned  {!r}'.format(r))
            ret += 1
    if ret:
        print_('=== some tests failed')
    else:
        print_('=== all tests successful')
    return ret



import traceback

def format_exception(exc, *key, **val):
    """
    Return a formatted traceback list of strings for an exception instance.
    """
    if sys.version_info >= (3, 10):
        # ✅ Python 3.10+ form (exc only)
        return traceback.format_exception(exc, *key, **val)
    else:
        # Older versions require (type, value, tb)
        return traceback.format_exception(type(exc), exc, exc.__traceback__, *key, **val)
