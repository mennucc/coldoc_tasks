import sys, os, time, pickle, functools, time, base64, copy, contextlib
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


# the format of the infofile is key/modifier=value
# when reading , `value` starts as `str`
# if modifier start with 32 or 64, value is base decoded
# if modifier start with p, value is unpickled
# if modifier start with i, value is converted to integer
# if modifier start with f, value is converted to float
# if modifier start with s, value is converted to string, utf8
# if modifier start with b, value is converted to bytes, utf8
# if modifier start with r, value is evaluated
# combinations of the above are accepted, in correct order


def write_config(infofile, db, sdb=[], rewrite_old = False):
    " `db` is the key/value to write ; `sdb` is the structure of the infofile; `rewrite_old` rewrites k/v not in `db`"
    if isinstance(infofile, (str, bytes, Path)): 
        with open(infofile,'w') as F:
            mychmod(infofile)
            write_config(F, db, sdb)
        return
    F = infofile
    #
    def write_k_v(k,v,F):
        assert '=' not in k and '/' not in k
        if isinstance(v,str):
            if any( (ord(j)<32)  for j in v ):
                v = v.encode('utf8')
                v = base64.b64encode(v)
                v = v.decode()
                F.write('%s/64s=%s\n' % (k, v) )
            else:
                F.write('%s=%s\n' % (k,v))
        elif isinstance(v, bool) or v is None:
            F.write('%s/r=%r\n'% (k,v))
        elif isinstance(v,int):
            F.write('%s/i=%d\n'% (k,v))
        elif isinstance(v,float):
            F.write('%s/f=%r\n'% (k,v))
        elif isinstance(v,bytes):
            F.write('%s/32=%s\n'  % (k, base64.b32encode(v).decode('ascii')))
        #elif v == eval(repr(v)):
        #    F.write('%s/f=%r\n'% (k,v))
        else:
            F.write('%s/64p=%s\n' % (k, base64.b64encode(pickle.dumps(v)).decode('ascii')))
    #
    db = copy.copy(db)
    # write keys that were in file, in same position
    for k,m,l in sdb:
        if k and k in db:
            write_k_v(k,db[k],F)
            db.pop(k)
        elif k and rewrite_old:
            F.write(l)
        # invalid lines are not rewritten
        elif k is None:
            F.write(l)
    # write new keys
    for k in db.keys():
        write_k_v(k,db[k],F)


def read_config(infofile):
    if isinstance(infofile, (str,bytes, Path)): 
        with open(infofile) as F:
            db = _read_config(F)
        return db
    return  _read_config(infofile)

def _read_config(infofile):
    " `infofile` must iterate to text lines; returns `(db, sdb)` where `db` are the extracted key,value, `sdb` is the structure of the file"
    def B(x): # to byte
        if isinstance(x, str):
            return x.encode('utf8')
        return x
    db = {}
    sdb = []
    for line_ in infofile:
        line = line_.rstrip('\n\r')
        # skip comments and empty lines
        if not line.strip() or line.strip().startswith('#'):
            sdb.append( (None, None, line_) )
            continue
        if '=' not in line:
            logger.warning('In info file %r ignored line %r', infofile, line)
            sdb.append( (False, False, line_) )
            continue
        try:
            key,value = line.split('=',1)
            m=''
            if '/' in key:
                key,m = key.split('/',1)
            sdb.append( (key, m, line_) )
            while m:
                if m.startswith('p'):
                    value = pickle.loads(B(value))
                    m = m[1:]
                elif m.startswith('s'):
                    if isinstance(value, bytes):
                        value = value.decode('utf8')
                    elif isinstance(value, int):
                        value = str(value)
                    else:
                        logger.warning('Cannot convert to string the value : %r', value)
                    m = m[1:]
                elif m.startswith('b'):
                    if isinstance(value, str):
                        value = value.encode('utf8')
                    #elif isinstance(value, int):
                    #    value = value.to_bytes(....)
                    else:
                        logger.warning('Cannot convert to bytes the value : %r', value)
                    m = m[1:]
                elif m.startswith('i'):
                    value = int(value)
                    m = m[1:]
                elif m.startswith('f'):
                    value = float(value)
                    m = m[1:]
                elif m.startswith('r'):
                    value = eval(value)
                    m = m[1:]
                elif m.startswith('32'):
                    value = base64.b32decode(B(value))
                    m = m[2:]
                elif m.startswith('64'):
                    value = base64.b64decode(B(value))
                    m = m[2:]
                else:
                    logger.error('error parsing line modifiers : %r', line)
                    break
            db[key] = value
        except Exception as E:
            logger.warning('In info file %r error parsing  %r : %r', infofile, line, E)
    return db, sdb

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
            logger.info('waitpid(%r) returned pid %r status %r ; waiting more' % (proc, pid, wstatus))
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
        logger.error('`preferences` contains unknown classes: %r ', set(preferences).difference(all_fork_classes))
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
            logger.info('infofile was changed: %r',self.infofile)
            refresh = True
        if not refresh and self.celeryconfig_mtime and self.celeryconfig_mtime != os.path.getmtime(self.celeryconfig):
            self.celeryconfig_mtime = os.path.getmtime(self.celeryconfig)
            logger.info('celeryconfig was changed: %r',self.celeryconfig)
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
        logger.info(' reentring at depth %d',depth)
        f.run(__fork_reentrat_test, fork_class, depth -1, sleep)
    elif sleep > 0:
        logger.info(' sleeping at depth 0 for %g', sleep)
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
        print_('** cannot pickle : %r' % E)
        ret += 1
    r = f.wait()
    if r != '3.14':
        ret += 1
        print_('Returned wrong value %r ' % (r,))
    else:
        print_('Returned %r ' % (r,))
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
            print_('WRONG: Returned  %r' % (r,))
            ret += 1
    #
    if ret:
        print_(' ** skipping reentrant test, already errors')
    else:
        N = 2
        D = 4
        print_("======= test : check against self locking, instances = %d depth = %d" %(N,D))
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
                    print_(' Caught %r, as expected ' % E)
                else:
                    print_('Failure %r' % (E,))
                    ret += 1
            except Exception as E:
                ret += 1
                print_('Failure %r' % (E,))
            else:
                if r != 'happy' :
                    print_('Wrong return value %r' % (r,))
    #
    if ret:
        print_(' ** skipping speed test, already errors')
    else:
        N = 256
        t = time.time()
        print_("======= speed test instances = %d " %(N,))
        print_("== scheduling")
        ff = list(range(N))
        for j in range(N):
            #sys.stderr.write('\r %d  \r' % (j,))
            ff[j] = fork_class()
            ff[j].run(int,'4')
        print_("== scheduling , time per instance %g sec" % ((time.time() - t) / N))
        print_("== waiting")
        for j in range(N):
            #sys.stderr.write('\r %d  \r' % (j,))
            r = ff[j].wait()
            if r != 4:
                ret += 1
        t = time.time() - t
        print_("======= speed test, fork_type %r, total time per instance %g sec " % (ff[0].fork_type, t / N,))
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
            print_('As expected, raised: %r ' % (R,))
        else:
            print_('WRONG: Returned  %r' % (r,))
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
