#!/usr/bin/env python3

__doc__ = """

  start infofile [socket] [authkey]
  
      start server
      
      socket is the file for the socket;
       (if missing, a temporary one will be generated)
       
       authkey is a password for the server
       (if missing, a random one will be generated)

      infofile is a file where informations
       regarding the server are stored

  django_start

     start server, getting the above info from django settings
     (see below)

  daemon infofile [socket] [authkey]

     as `start`, but forks a separate process

  django_daemon

     as `django_start` , but forks a separate process

  stop infofile
  django_stop

      stop server

  test infofile
  django_test

      test server

  test_hanging infofile
  django_test_hanging

      create hanging job in  server

  status infofile
  django_status

      status of server

  ping infofile
  django_ping
  
      ping server

  pid infofile
  django_pid

      PID of server process

For each command, there is `django_..` version:
this version initializes a django instance, and looks
into the settings for all informations (see README.md):
it uses `COLDOC_TASKS_INFOFILE` to know where the infofile is;
when starting the server, moreover,
it gets the authkey from settings.COLDOC_TASKS_PASSWORD
and the socket filename from  settings.COLDOC_TASKS_SOCKET
and writes the infofile
"""


import os, sys, time, pickle, base64, functools
import subprocess, multiprocessing.managers
import random, socket, struct, tempfile, copy, threading
from pathlib  import Path
from datetime import datetime

try:
    import psutil
except ImportError:
    psutil = None


python_default_tempdir = tempfile.gettempdir()

import logging, logging.handlers
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    a = os.path.realpath(os.path.dirname(__file__))
    if a in sys.path:
        del sys.path[sys.path.index(a)]
    a = os.path.dirname(a)
    if a not in sys.path:
        sys.path.insert(0,a)
    

from coldoc_tasks.simple_tasks import fork_class_base
from coldoc_tasks.task_utils import _normalize_pythonpath, mychmod, proc_join, format_exception
from coldoc_tasks.wrap_lockfile import mylockfile, myLockTimeout
import plain_config
from coldoc_tasks.exceptions import *


__all__ = ('get_manager', 'run_server', 'ping', 'server_pid', 'status', 'shutdown', 'test', 'fork_class',
           'run_cmd', 'wait', 'get_result', 'join',
           'queue_cmd',
           'tasks_daemon_autostart', 'tasks_daemon_django_autostart',
           'tasks_server_readinfo', 'tasks_server_writeinfo', 'tasks_server_start', 'task_server_check')


class RandGen():
    def __init__(self, seed=None):
        if seed is None:
            try:
                seed = os.urandom(8)
            except Exception as E:
                logger.error('When `os.urandom(8)` : %r', E)
                seed =time.time()
        #
        self.randomsource = randomsource = random.Random(seed)
        if  hasattr(randomsource,'RandGen'):
            self.rand_bytes = randomsource.randbytes
        else: #older Python
            self.rand_bytes = lambda n: self.randomsource.getrandbits(n * 8).to_bytes(n, 'little')
    #
    def rand_string(self, n=8):
        b = self.rand_bytes(n)
        return   base64.b64encode(b,altchars=b'-_').decode('ascii')

general_rand_gen = RandGen()

##########################

actions = ('ping__','getpid__','status__','shutdown__',
           'run_cmd__','get_result_join__','join__','get_wait_socket__',
           'terminate__')

@functools.lru_cache(100)
def get_manager(address, authkey):
    """Return a connected `SyncManager` registered with the RPC actions used by the task server."""
    manager = multiprocessing.managers.SyncManager(address=address, authkey=authkey)
    for j in actions:
        manager.register(j)
    manager.connect()
    return manager

########################

def __socket_server(socket_, access_pair, rets, id_):
    # unpack auth from socket definition
    assert isinstance(access_pair, (tuple,list)), access_pair
    socket_name, auth = access_pair
    assert auth is None or ( isinstance(auth, bytes) and len(auth) == 8)
    # currently the socket_ is defined before, but, just in case..
    if socket_ is None:
        socket_ = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        socket_.bind(socket_name)
        socket_.listen(2)
        mychmod(socket_name)
    #
    sent = False
    with socket_ as s:
        logger.debug('Id %s listening', id_)
        while True:
            conn, addr = s.accept()
            with conn:
                logger.debug('Id %s connected by %r', id_, addr)
                if auth:
                    conn.sendall(b'#AUTH')
                    auth_ = conn.recv(8)
                    if auth_ != auth:
                        conn.sendall(b'#WRNG')
                        break
                conn.sendall(b'#HELO')
                a = conn.recv(5)
                logger.debug('Id %s message is %r', id_, a)
                if a == b'#SEND':
                    l = len(rets)
                    conn.sendall(struct.pack('<Q', l))
                    conn.sendall(rets)
                    sent = True
                elif a == b'#SENT':
                    conn.sendall(struct.pack('<B', int(sent)))
                elif a == b'#QUIT':
                    logger.debug('Exiting id %s socket loop', id_)
                    conn.sendall(b'#GONE')
                    break
                conn.sendall(b'#ACK ')



def __recv_exact(sock, length):
    "Read exactly `length` bytes from `sock` or raise RuntimeError if the peer closes."
    data = bytearray()
    while len(data) < length:
        chunk = sock.recv(length - len(data))
        if not chunk:
            raise RuntimeError('Socket closed before receiving expected data (wanted {}, got {}).'.format(
                length, len(data)))
        data.extend(chunk)
    return bytes(data)


def __send_message(m, F, timeout=None):
    assert isinstance(m,bytes) and len(m) == 5
    # unpack auth from access_pair definition
    F, auth = F
    assert auth is None or ( isinstance(auth, bytes) and len(auth) == 8)
    #
    ret = None
    if F is  None:
        return None
    ret = False
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        if timeout:
            s.settimeout(timeout)
        s.connect(F)
        a = s.recv(5)
        if a == b'#AUTH':
            if auth is None:
                raise RuntimeError('Auth is required for {!r}'.format(F))
            s.sendall(auth)
            a = s.recv(5)
        if a != b'#HELO':
            if auth:
                raise RuntimeError('Wrong Auth for {!r}'.format(F))
            else:
                raise RuntimeError('Unexpected hello {!r} from {!r}'.format(a, F))
        s.sendall(m)
        if m == b'#SEND':
            l = __recv_exact(s, 8)
            l = struct.unpack('<Q', l)[0]
            rets = __recv_exact(s, l)
            ret = pickle.loads(rets)
        elif m == b'#SENT':
            l = __recv_exact(s, 1)
            l = struct.unpack('<B', l)[0]
            ret = bool(l)
        a = s.recv(5)
        if a not in ( b'#ACK ', b'#GONE' ):
            logger.warning('Unexpected response %r', a)
            ret = False
    return ret

######################### starting jobs

def run_cmd(manager, cmd, args, kwarks):
    """Synchronously run `cmd(*args, **kwarks)` via the manager and return its proxy result."""
    proxy = manager.run_cmd__(cmd, args, kwarks)
    return proxy._getvalue()

def queue_cmd(manager, cmd, args, kwarks, queue=True):
    """Schedule a command on the named server queue and return the enqueued task identifier."""
    proxy = manager.run_cmd__(cmd, args, kwarks, queue=queue)
    return proxy._getvalue()


def wait(id_, manager):
    """Block until the remote command identified by `id_` finishes executing."""
    F = manager.get_wait_socket__(id_)
    F = F._getvalue()
    if F is not None:
        return __send_message(b'#WAIT', F)

def get_result(id_, manager, timeout=None):
    """Fetch the serialized `(status, payload, traceback)` tuple associated with `id_`."""
    F = manager.get_wait_socket__(id_)
    F = F._getvalue()
    if F is not None:
        return __send_message(b'#SEND', F, timeout)
    return None

def join(id_, manager):
    """Notify the server that the client collected the result so the worker can exit."""
    F = manager.get_wait_socket__(id_)
    F = F._getvalue()
    if F is not None:
        return __send_message(b'#QUIT', F)


######################## starting jobs, by class


class fork_class(fork_class_base):
    "class that runs a job in a subprocess, and returns results or raises exception"
    fork_type = 'coldoc'
    def __init__(self, address, authkey, use_fork = True, timeout=None, queue=None):
        super().__init__(use_fork = use_fork )
        self.__cmd_id = None
        self.__ret = (2, RuntimeError('This should not happen'), None)
        self.__manager = None
        self.__address = address
        self.__authkey = authkey
        #
        self.__timeout = timeout
        self.__queue = None
        self.__thread_lock = threading.Lock()
    #
    def __getstate__(self):
        state = self.__dict__.copy()
        state.pop('_fork_class__thread_lock', None)
        state['_fork_class__manager'] = None
        return state
    #
    def __setstate__(self, state):
        # Restore the object's state
        self.__dict__.update(state)
        # Recreate the lock
        self.__thread_lock = threading.Lock()    
    #
    @staticmethod
    def can_fork():
        return hasattr(socket,'AF_UNIX')
    #
    def __run(self, cmd, *k, **v):
        assert self.already_run is False
        self.__cmd_name = cmd.__name__
        self.__ret = (2, RuntimeError('Process {!r} : could not read its return value'.format(self.__cmd_name)), None)
        if self.use_fork_:
            if self.__manager is None:
                self.__manager = get_manager(self.__address, self.__authkey)
            proxy = self.__manager.run_cmd__(cmd, k, v)
            self.__cmd_id = proxy._getvalue()
        else:
            try:
                self.__ret = (0, cmd(*k, **v), None)
            except Exception as E:
                self.__ret = (1, E, format_exception(E))
        self.already_run = True
    #
    def run(self, cmd, *k, **v):
        with self.__thread_lock:
            return self.__run(cmd, *k, **v)
    #
    @property
    def task_id(self):
        return self.__cmd_id
    #
    def terminate(self):
        if self.use_fork_:
            if self.__manager is None:
                self.__manager = get_manager(self.__address, self.__authkey)
            self.__manager.terminate__(self.__cmd_id)
    #
    def __wait(self, timeout=None, raise_exception=True):
        timeout = self.__timeout if timeout is None else timeout
        assert self.already_run
        if self.use_fork_ and not self.already_wait:
            if self.__manager is None:
                self.__manager = get_manager(self.__address, self.__authkey)
            self.already_wait = True
            try:
                self.__ret = get_result(self.__cmd_id, self.__manager, timeout=timeout)
                if self.__ret is None:
                    raise ColdocTasksTimeoutError('Process has disappeared: {}'.format(self.__cmd_id))
                self.__manager.join__(self.__cmd_id)
            except socket.timeout as E:
                raise ColdocTasksTimeoutError('timeout on {!r} : {!r}'.format(self.__cmd_id, E))
            except Exception as E:
                raise RuntimeError('Process {!r} exception on wait : {!r}'.format(self.__cmd_name, E))
        if self.__ret[0] :
            self.traceback_ = self.__ret[2]
            self.exception_ = self.__ret[1]
            if raise_exception:
                raise self.__ret[1]
        return self.__ret[1]
    #
    def wait(self, *args, **kwargs):
        with self.__thread_lock:
            return self.__wait(*args, **kwargs)

############################## status

def ping(address, authkey, warn=True):
    """Return True if the remote server responds to a health probe, False otherwise."""
    try:
        manager = get_manager(address, authkey)
        F = manager.ping__()
        F = F._getvalue()
        return F
    except Exception as E:
        if warn:
            logger.warning('When pinging %r', E)

def server_pid(address, authkey, warn=True):
    """Return the PID reported by the remote task server."""
    try:
        manager = get_manager(address, authkey)
        F = manager.getpid__()
        F = F._getvalue()
        return F
    except Exception as E:
        if warn:
            logger.warning('When getting PID %r', E)


def status(address, authkey):
    """Fetch the dictionary returned by the server `status__` RPC call."""
    try:
        manager = get_manager(address, authkey)
        F = manager.status__()
        V = F._getvalue()
        return V
    except Exception as E:
        logger.warning('When status %r', E)

def shutdown(address, authkey):
    """Request a graceful shutdown of the remote server."""
    try:
        manager = get_manager(address, authkey)
        return manager.shutdown__()
    except Exception as E:
        logger.warning('When shutdown %r', E)

def test(address, authkey, print_=print):
    """Exercise fork/non-fork execution paths and print diagnostic information."""
    import coldoc_tasks.task_utils as task_utils
    logger.setLevel(logging.INFO)
    FC = functools.partial(fork_class, address=address, authkey=authkey)
    task_utils.logger.setLevel(logging.INFO)
    print_('*' * 30 + '(coldoc forking)')
    err = task_utils.test_fork(fork_class=FC, print_=print_)
    print_('*' * 30 + '(coldoc not forking)')
    FCN = functools.partial(fork_class, use_fork = False, address=address, authkey=authkey)
    err += task_utils.test_fork(fork_class=FCN, print_=print_)
    print_('*' * 30 + '(coldoc end)')
    if os.environ.get('DJANGO_SETTINGS_MODULE') == 'ColDocDjango.settings':
        f = FC()
        f.run(__countthem)
        print_('---- test of reading the Django database: there are {!r} DMetadata objects'.format(f.wait()))
    return err


############## server code




######

def _fork_mp_wrapper(*args, **kwargs):
    id_, pipe, socket_, access_pair_, cmd, k , v, l = args
    multiprocessing.log_to_stderr(level=l)
    #
    logger = multiprocessing.get_logger() 
    logger.info('Start id %s cmd %r %r %r ', id_, cmd, k, v)
    try:
        ret = (0, cmd(*k, **v), None)
    except Exception as E:
        ret = (1, E, format_exception(E))
    logger.info('Id %r cmd %s return %r', id_, cmd, ret)
    pipe.send(ret)
    rets = pickle.dumps(ret)
    #
    __socket_server(socket_, access_pair_, rets, id_)
    F = access_pair_[0]
    logger.info('Exited socket loop, removing %r', F)
    os.unlink(F)


def run_server(address, authkey, infofile, **kwargs):
    """ returns the `kwargs`, containing also `kwargs["return_code"]`"""
    # Python 3.14+ changed the default start method to 'forkserver' on POSIX platforms.
    # We need 'fork' for socket inheritance to work properly.
    if hasattr(multiprocessing, 'set_start_method'):
        try:
            multiprocessing.set_start_method('fork', force=True)
        except RuntimeError:
            # Start method already set, try to use the current context
            pass
    L = multiprocessing.log_to_stderr()
    global logger
    L.setLevel(logger.getEffectiveLevel())
    logger = L
    #
    default_tempdir = kwargs.get('default_tempdir', python_default_tempdir)
    tempdir     = kwargs.get('tempdir', None)
    with_django = kwargs.get('with_django', False)
    logfile     = kwargs.get('logfile', None)
    if tempdir is None:
        tempdir = tempfile.mkdtemp(prefix='coldoc_tasks_', dir=default_tempdir)
    if logfile is True:
        logfile_f = tempfile.NamedTemporaryFile(dir=tempdir, delete=False, mode='a',
                                                prefix='server_', suffix='.log')
        logfile = logfile_f.name
    if logfile:
        h = logging.handlers.RotatingFileHandler(logfile, maxBytes=2 ** 16, backupCount=5)
        f = logging.Formatter('[%(asctime)s - %(funcName)s - %(levelname)s] %(message)s')
        h.setFormatter(f)
        logger.addHandler(h)
        #logger.setLevel(logging.INFO)
        logger.info('Start log, pid %r', os.getpid())
        kwargs['logfile'] = logfile
    #
    s = set(kwargs.keys()).difference(['default_tempdir','tempdir','with_django','logfile','pid','return_code'])
    if s:
        logger.warning('Some kwargs were ignored: %r ', s)
    #
    manager = multiprocessing.managers.SyncManager(address=address, authkey=authkey)
    #
    kwargs['return_code'] = True
    # currently tje code work better without a subprocess
    run_with_subprocess = False
    #
    #
    server = None
    pools = {}
    # default pool
    pools[True] = default_pool = multiprocessing.pool.Pool()
    processes = kwargs.get('processses', {})
    try:
        if with_django:
            os.environ.pop('COLDOC_TASKS_AUTOSTART', None)
            os.environ.setdefault('DJANGO_SETTINGS_MODULE', with_django)
            import django
            django.setup()
        if 1:
            z = default_pool.apply_async(str,(2,))
            z.wait()
            d = z.get()
            logger.info('********** %r', d)
        #
        rand_gen = RandGen()
        #
        # used if run_with_subprocess is True
        __do_run = multiprocessing.Value('i')
        def stop_server__():
            logger.info('Received shutdown')
            __do_run.value = 0
        def stop_server_now__():
            logger.info('Received fast shutdown')
            __do_run.value = -1
        #
        Nooone = (None, None, None, None)
        def run_cmd__(c, k, v, pipe=None, queue=None):
            id_ = rand_gen.rand_string(9)
            F = os.path.join(tempdir, 'socket_' + id_)
            socket_ = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            socket_.bind(F)
            socket_.listen(2)
            mychmod(F)
            #
            if pipe is None:
                pipe = multiprocessing.Pipe()
            l = logger.getEffectiveLevel()
            #
            ## I would like to use:
            #z = pool.apply_async(c,k,v)
            ## but it hangs
            # add auth
            auth_ = rand_gen.rand_bytes(8)
            access_pair_ = (F, auth_)
            #
            if queue is None:
                proc = multiprocessing.Process(target=_fork_mp_wrapper, args=(id_, pipe[1], socket_, access_pair_, c, k, v, l))
                proc.name = 'coldoc_task '+id_ + ' ' + repr(c.__name__)
                proc.start()
            else:
                if queue not in pools:
                    pool_ = pools[queue] = multiprocessing.pool.Pool()
                else:
                    pool_ = pools[queue]
                proc = pool_.apply_async(_fork_mp_wrapper, args=(id_, pipe[1], socket_, access_pair_, c, k, v, l))
            #
            pipe0 = pipe[0]
            #pipe0._config['authkey'] = bytes(pipe0._config['authkey'])
            processes[id_] = (proc, pipe0, access_pair_, time.time())
            logger.debug('Running cmd %r ( %r , %r ), id = %r, socket = %r', c, k, v, id_, F)
            return id_
        #
        def get_wait_socket__(id_):
            id_ = str(id_)
            logger.debug('getting result pipe for  id = %r ', id_)
            proc, pipe, F, started_at = processes.get(id_, Nooone)
            return F
        #
        def get_result_join__(id_):
            logger.debug('getting result for  id = %r ', id_)
            id_ = str(id_)
            proc, pipe, F, started_at = processes.pop(id_, Nooone)
            if proc is not None:
                try:
                    logger.info('Waiting for result id = %r', id_)
                    r = __send_message(b'#SEND', F) #get_result(id_) #pipe.recv()
                    __send_message(b'#QUIT', F)
                    proc_join(proc)
                except  Exception as E:
                    r = (1, E)
            else:
                logger.error('No process by id %r', id_)
                r = (1, RuntimeError('No process by id {!r}'.format(id_)))
            return r
        #
        def terminate__(id_):
            logger.debug('getting result for  id = %r ', id_)
            id_ = str(id_)
            proc, pipe, F, started_at = processes.pop(id_, Nooone)
            if proc is not None:
                proc.terminate()
                return True
            return False
        #
        def _describe_process_entry(entry):
            proc, pipe, access_pair_, started_at = entry
            summary = {
                'proc': repr(proc)
            }
            if started_at is not None:
                local_tz = datetime.now().astimezone().tzinfo
                summary['started_at'] = datetime.fromtimestamp(started_at, tz=local_tz).isoformat()
            for attr in ('name', 'pid', 'exitcode', 'daemon'):
                value = getattr(proc, attr, None)
                if value is not None:
                    summary[attr] = value
            for callable_attr in ('is_alive', 'ready', 'successful'):
                method = getattr(proc, callable_attr, None)
                if callable(method):
                    try:
                        summary[callable_attr] = method()
                    except Exception:
                        summary[callable_attr] = None
            summary['pipe_closed'] = getattr(pipe, 'closed', None)
            if isinstance(access_pair_, (tuple, list)) and access_pair_:
                socket_name = access_pair_[0]
                summary['socket'] = socket_name
                if len(access_pair_) > 1:
                    auth = access_pair_[1]
                    summary['auth_required'] = bool(auth)
            return summary

        def status__():
            serialized = {}
            for proc_id, entry in processes.items():
                try:
                    serialized[proc_id] = _describe_process_entry(entry)
                except Exception as exc:
                    serialized[proc_id] = {'error': repr(exc)}
            return {'processes': serialized}
        #
        def ping__():
            return True
        #
        def getpid__():
            return os.getpid()
        #
        def join__(id_):
            logger.debug('joining  id = %r ', id_)
            id_ = str(id_)
            proc, pipe, F, started_at = processes.pop(id_, Nooone)
            if proc is not None:
                try:
                    logger.info('Joining id = %r', id_)
                    sent = __send_message(b'#SENT', F)
                    if not sent:
                        logger.critical('The result of process %s was never recovered', id_)
                    __send_message(b'#QUIT', F)
                    proc_join(proc)
                except  Exception as E:
                    logger.exception('Unexpected exception from id_ %r : %r', id_, E)
            else:
                logger.error('No process by id %r', id_)
        #
        def initializer():
            a = set(copy.deepcopy(manager._registry))
            manager.register('run_cmd__', run_cmd__)
            manager.register('get_result_join__', get_result_join__)
            manager.register('join__', join__)
            manager.register('get_wait_socket__',get_wait_socket__)
            manager.register('terminate__',terminate__)
            manager.register('ping__',ping__)
            manager.register('getpid__',getpid__)
            manager.register('status__',status__)
            if run_with_subprocess:
                manager.register('shutdown__', stop_server__)
            else:
                manager.register('shutdown__', lambda : server.stop_event.set())
            b = set(manager._registry)
            return b.difference(a)
        # test consistency
        if 1:
            a = initializer()
            if set(actions).difference(a):
                raise RuntimeError('Actions missing in server', set(actions).difference(a))
            if set(a).difference(actions):
                raise RuntimeError('Actions missing in client', set(a).difference(actions))
        # self test
        if 1:
            i = run_cmd__(str,(2,),{})
            #wait(i) no, the manager is not running
            d = get_result_join__(i)
            assert isinstance(d,tuple) and d[0] == 0 and d[1] == '2', repr(d)
            logger.info('self test OK')
        # queue self test
        if 1:
            i = run_cmd__(str,(5,),{},queue=True)
            #wait(i) no, the manager is not running
            d = get_result_join__(i)
            assert isinstance(d,tuple) and d[0] == 0 and d[1] == '5', repr(d)
            logger.info('queue self test OK')
        # run the server
        if run_with_subprocess:
            logger.info('Start manager')
            manager.start()
            __do_run.value = 1
            while __do_run.value > 0:
                time.sleep(0.1)
        else:
            logger.info('Start server')
            server = manager.get_server()
            # re-register this
            manager.register('shutdown__', lambda : server.stop_event.set())
            server.serve_forever()
    except (KeyboardInterrupt,SystemExit):
        pass
    except Exception:
        logger.exception('in run_server')
        kwargs['return_code'] = False
    
    try:
        logger.info('Shutting down')
        kwargs['processes'] = processes
        for id_  in list(processes.keys()):
            join__(id_)
        kwargs.pop('processes', None)
        for pool in pools.values():
            pool.close()
        for pool in pools.values():
            pool.join()
        if run_with_subprocess:
            manager.shutdown()
            manager.join()
    except (KeyboardInterrupt, SystemExit):
        pass
    except Exception:
        logger.exception('When cleaning after run_server')
        kwargs['return_code'] =  False
    return kwargs


def server_wait(address, authkey, timeout = 2.0):
    " try pinging, up to timeout"
    ok = False
    t = time.time() + timeout
    for j in range(int(float(timeout) * 20.)):
        ok = ping(address, authkey, warn=False)
        if ok or (time.time() > t): break
        time.sleep(0.05)
    return ok

######################## code to start server

infofile_keywords = ('address', 'authkey', 'pid')

def tasks_server_readinfo(infofile):
    "returns  (address, authkey, pid, dict_of_other_options)"
    ret = [None] * (1+ len(infofile_keywords) )
    lock = mylockfile(infofile+'-rw')
    with lock:
        db, sdb = plain_config.read_config(infofile)
    for n,k in enumerate(infofile_keywords):
        if k in db:
            ret [ n ] = db.pop(k)
    ret[-1] = db
    return ret

def tasks_server_writeinfo(infofile, *args, **kwargs):
    "write values to infofile, positional arguments are   (address, authkey, pid)"
    kw = copy.copy(kwargs)
    kw.pop('default_tempdir', None)
    for j in range(len(args)):
        k = infofile_keywords[j]
        v = args[j]
        kw[k] = v
    # we preserve the file structure
    lock = mylockfile(infofile+'-rw')
    with lock:
        if infofile and os.path.exists(infofile):
            db, sdb = plain_config.read_config(infofile)
        else:
            sdb = []
        ret = plain_config.write_config(infofile, kw, sdb, rewrite_old=True)
    return ret

def __tasks_server_start_nolock(infofile, address, authkey, **kwargs):
    kwargs['pid'] = os.getpid()
    tasks_server_writeinfo(infofile, address, authkey, **kwargs )
    ret = False
    try:
        kwargs = run_server(address, authkey, infofile, **kwargs)
        ret = kwargs.get('return_code')
    except Exception as E:
        logger.exception('When running task server')
    kwargs['pid'] = None
    tasks_server_writeinfo(infofile, address, authkey, **kwargs )
    return ret

def tasks_server_start(infofile, address=None, authkey=None,
                       tempdir=None, logfile=None, default_tempdir=python_default_tempdir, **kwargs):
    " start a server with `address` and `authkey` ,  saving info in `infofile (that is locked while in use)"
    infofile, address, authkey, tempdir, logfile, other =  \
        _fix_parameters(infofile, address, authkey, tempdir, logfile, default_tempdir)
    if 'default_tempdir' in other:
        other.pop('default_tempdir')
        logger.warning(' `default_tempdir` ignored in infofile %r ', infofile)
    for k in other:
        if k not in kwargs:
            logger.warning(' %r unused in infofile %r ', k, infofile)
            kwargs[k] = other[k]
    lock = mylockfile(infofile, timeout=2)
    ret = None
    try:
        with lock:
            ret =  __tasks_server_start_nolock(infofile, address, authkey,
                                           tempdir=tempdir, logfile=logfile, default_tempdir=default_tempdir, **kwargs)
    except Exception as E:
        logger.exception('while starting server')
        ret = False
    return ret


def _coerce_path_setting(value, setting_name):
    """Return a string path for the given Django setting or `None` if conversion fails."""
    if value is None:
        return None
    original = value
    if isinstance(value, Path):
        logger.info('Converting settings.%s Path %r to str', setting_name, value)
        value = str(value)
    elif isinstance(value, bytes):
        try:
            value = value.decode('utf-8')
            logger.info('Decoded settings.%s bytes to str', setting_name)
        except UnicodeDecodeError:
            logger.warning('settings.%s bytes value %r could not be decoded', setting_name, original)
            return None
    if not isinstance(value, str):
        logger.warning('settings.%s should be a str path, got %r', setting_name, type(original))
        return None
    return value


def _sanitize_pythonpath_setting(value):
    """Normalize the pythonpath setting into a list/str compatible with `_normalize_pythonpath`."""
    if value is None:
        return None
    if isinstance(value, Path):
        coerced = _coerce_path_setting(value, 'COLDOC_TASKS_PYTHONPATH')
        return [coerced] if coerced else None
    if isinstance(value, (list, tuple)):
        sanitized = []
        for idx, entry in enumerate(value):
            coerced = _coerce_path_setting(entry, f'COLDOC_TASKS_PYTHONPATH[{idx}]')
            if coerced:
                sanitized.append(coerced)
        return sanitized
    if isinstance(value, (str, bytes)):
        return _coerce_path_setting(value, 'COLDOC_TASKS_PYTHONPATH')
    logger.warning('settings.COLDOC_TASKS_PYTHONPATH has unsupported type %r', type(value))
    return None


def _read_django_settings(kwargs, settings):
    """Populate `kwargs` with sanitized Django settings used to start or connect to the server."""
    kwargs['infofile'] = _coerce_path_setting(getattr(settings, 'COLDOC_TASKS_INFOFILE', None),
                                              'COLDOC_TASKS_INFOFILE')
    #
    kwargs['address'] = _coerce_path_setting(getattr(settings, 'COLDOC_TASKS_SOCKET', None),
                                             'COLDOC_TASKS_SOCKET')
    #
    authkey = getattr(settings, 'COLDOC_TASKS_PASSWORD', None)
    if isinstance(authkey, str):
        logger.info('Encoding settings.COLDOC_TASKS_PASSWORD string to bytes')
        authkey = authkey.encode('utf-8')
    elif authkey is not None and not isinstance(authkey, bytes):
        logger.warning('settings.COLDOC_TASKS_PASSWORD must be bytes or str, got %r', type(authkey))
        authkey = None
    kwargs['authkey'] = authkey
    #
    kwargs['logfile'] = _coerce_path_setting(getattr(settings, 'COLDOC_TASKS_LOGFILE', None),
                                             'COLDOC_TASKS_LOGFILE')
    #
    default_tempdir = _coerce_path_setting(getattr(settings, 'COLDOC_TASKS_TEMPDIR', python_default_tempdir),
                                           'COLDOC_TASKS_TEMPDIR')
    kwargs['default_tempdir'] = default_tempdir
    #
    kwargs['pythonpath'] = _sanitize_pythonpath_setting(getattr(settings, 'COLDOC_TASKS_PYTHONPATH', None))
    kwargs['with_django'] = os.environ.get('DJANGO_SETTINGS_MODULE')

def tasks_django_server_start(settings, **kwargs):
    """Start the task server using parameters extracted from Django settings."""
    _read_django_settings(kwargs, settings)
    return tasks_server_start(**kwargs)


def task_server_check(info):
    """ accepts the infofile
    returns  `status, sock, auth, pid` , where `status` is a boolean"""
    if os.path.isfile(info):
        sock, auth, pid = tasks_server_readinfo(info)[:3]
        if not( sock and auth):
            logger.info('One of address, authkey is missing from %r', info)
            return False, sock, auth, pid
        if psutil and pid and not psutil.pid_exists(pid):
            logger.warning('Tasks server pid %r does not exist', pid)
            pid = None
            return False, sock, auth, pid
        if pid and ping(address=sock, authkey=auth):
            return True, sock, auth, pid
        else:
            if pid:
                logger.warning('Tasks server pid %r is not responding', pid)
            else:
                logger.debug('Tasks server sock %r has no pid and is not responding', sock)
            return False, sock, auth, pid
    return False, None, None, None


def _fix_parameters(infofile=None, sock=None, auth=None,
                    tempdir=None, logfile=None, default_tempdir=python_default_tempdir):
    #
    other_ = {}
    if isinstance(infofile, (str, bytes, Path)):
        if  os.path.isfile(infofile):
            sock_, auth_, pid_, other_ = tasks_server_readinfo(infofile)
            tempdir_ = other_.pop('tempdir', None)
            if tempdir and tempdir_ and tempdir != tempdir_:
                logger.warning('Changing infofile tempdir  %r > %r ', tempdir_, tempdir)
            tempdir = tempdir or tempdir_
            if sock and sock_ and sock != sock_:
                logger.warning('Changing infofile  socket  %r > %r ', sock_, sock)
            sock = sock or sock_
            if auth and auth_ and auth != auth_:
                logger.warning('Changing infofile  authkey')
            auth = auth or auth_
            logfile2 = other_.pop('logfile', None)
            logfile = logfile or logfile2
    elif infofile is not None:
        logger.warning('Unsupported type for infofile: %r', infofile)
    #
    ok = False
    try:
        if tempdir is not None:
            ok = os.path.isdir(tempdir)
            if hasattr(os, 'access'):
                ok = ok and os.access(tempdir, os.R_OK | os.W_OK)
            if hasattr(os, 'getuid'):
                s = os.stat(tempdir)
                ok = ok and (s.st_uid == os.getuid())
    except OSError:
        ok = False
    if tempdir is None or not ok:
        newtempdir = tempfile.mkdtemp(prefix='coldoc_tasks_', dir=default_tempdir)
        if tempdir:
            logger.warning('tempdir not available changine  %r -> %r ', tempdir, newtempdir)
        tempdir = newtempdir
    #
    if infofile is None:
        infofile = os.path.join(tempdir, 'infofile')
    #
    ok = False
    if sock is not None:
        d = os.path.dirname(sock)
        ok = os.path.isdir(d) and d.startswith(tempdir)
    if not ok:
        newsock = os.path.join(tempdir, 'manager_socket_'+general_rand_gen.rand_string())
        if sock:
            logger.warning('Changing socket  %r -> %r , wrong directory', sock, newsock)
        sock = newsock
    #
    auth = auth or os.urandom(10)
    #
    if logfile and isinstance(logfile, (str, bytes, Path)):
        d = os.path.dirname(logfile)
        if not os.path.isdir(d):
            logger.warning('Warning, the directory of `logfile`   %r does not exist', logfile)
            logfile = None
    #
    assert isinstance(sock, (str, bytes, Path))
    assert isinstance(auth, bytes)
    assert logfile in (None, True) or isinstance(logfile, (str, bytes, Path))
    return infofile, sock, auth, tempdir, logfile, other_

def tasks_daemon_autostart_nolock(infofile=None, address=None, authkey=None,
                           pythonpath=None,
                           cwd=None,
                           logfile = None,
                           opt = None,
                           tempdir = None,
                           default_tempdir=python_default_tempdir,
                           timeout = 2.0,
                           force = False,
                           # this option is used to wrap this function for Django...
                           subcmd=None,
                           **kwargs
                           ):
    """ Check if there is a server running using `infofile`;
    if there is, return (PID, infofile),      if not, start it (as a subprocess), and return(`proc`, `infofile`)
    (where `proc` is either a `subprocess.Popen` or `multiprocessing.Process` instance),
    in case of exception, the wrapper `tasks_daemon_autostart` returns (`False`, exception)
   
    Arguments notes: 
        `address` is the socket (if `None`, it will be read from `infofile`, that must exist);
       if `authkey` is `None`, generate a random one;
       if `logfile` is `True`, will create a temporary files to store logs;
       if `force` is True, and the server cannot be contacted, remove lock and socket;
       `pythonpath` may be a string, in the format of PYTHONPATH, or a list:
        it  will be added to sys.path.
       
    The env variable 'COLDOC_TASKS_AUTOSTART_OPTIONS' may be used to tune this functions,
    it accepts a comma-separated list of keywords from `nocheck`, `noautostart`, `force` .
    The argument `opt` overrides that env variable. The argument `force`, if set, ignores the previous two.
      """
    #
    if kwargs:
        logger.warning('Some kwargs were ignored: %r', kwargs)
    #
    ok = False
    if opt is None:
        opt = os.environ.get('COLDOC_TASKS_AUTOSTART_OPTIONS','').split(',')
    elif isinstance(opt,str):
        opt = opt.split(',')
    if 'nocheck' not in opt and infofile:
        ok, sock_, auth_, pid_ = task_server_check(infofile)
        if ok:
            return pid_, infofile
        if not ok and sock_ and os.path.exists(sock_):
            try:
                os.unlink(sock_)
                logger.warning('Removed stale socket %r (pid %r) ', sock_, pid_)
            except Exception as E:
                logger.exception('While removing stale socket %r (pid %r) ', sock_, pid_)
    else:
        sock_ = auth_ = pid_  = None,
    #
    if force is None:
        force = 'force' in opt
    if force and not ok:
        if isinstance(infofile,str)  and os.path.exists(infofile+'.lock'):
            logger.warning('Removing stale lock %r', infofile+'.lock')
            os.unlink(infofile+'.lock')
        if isinstance(sock_,str) and os.path.exists(sock_):
            logger.warning('Removing stale socket %r', sock_)
            os.unlink(sock_)
    # this does not work OK
    use_multiprocessing=False
    #
    proc = None
    if not ok and 'noautostart' not in opt:
        #
        logger.info('starting task server')
        #
        infofile, address, authkey, tempdir, logfile, other = \
            _fix_parameters(infofile, address, authkey, tempdir, logfile, default_tempdir)
        if logfile: other['logfile'] = logfile
        #
        p = other.get('pythonpath')
        if pythonpath is not None:
            pythonpath = _normalize_pythonpath(pythonpath)
            other['pythonpath'] = os.path.sep.join(pythonpath)
            if p is not None:
                p = _normalize_pythonpath(p)
                if  p != pythonpath:
                    logger.warning('Changed `pythonpath` from  %r to %r ', p, pythonpath)
        #
        tasks_server_writeinfo(infofile, address, authkey, tempdir=tempdir, **other)
        #
        if use_multiprocessing:
            import multiprocessing
            # FIXME: this fails in some cases, since django is not reentrant
            # TODO: support pythontpath, cwd, subcmd for django, tempdir
            #
            # FIXME this may not work as I would like
            flag = os.environ.get('COLDOC_TASKS_AUTOSTART_OPTIONS')
            os.environ['COLDOC_TASKS_AUTOSTART_OPTIONS'] =  'nocheck,noautostart'
            #
            proc = multiprocessing.Process(target=tasks_server_start,
                                           args=(infofile, address, authkey))
            os.environ.pop('COLDOC_TASKS_AUTOSTART', None)
            if flag is None:
                del os.environ['COLDOC_TASKS_AUTOSTART_OPTIONS']
            else:
                os.environ['COLDOC_TASKS_AUTOSTART_OPTIONS'] =  flag
            proc.start()
        else:
            if logfile is True:
                logfile_ = tempfile.NamedTemporaryFile(dir=os.path.dirname(infofile),
                                            delete=False, mode='a',
                                            prefix='coldoc_tasks_', suffix='.log')
            elif isinstance(logfile, (str,bytes,Path)):
                logfile_ = open(logfile, 'a')
            elif isinstance(logfile, io.IOBase) or isinstance(logfile, tempfile._TemporaryFileWrapper):
                assert logfile.writable(), "This logfile is not writable"
                logfile_ = logfile
            else:
                if logfile is not None:
                    logger.error('parameter `logfile` is of unsupported type %r', type(logfile))
                logfile_ = open(os.devnull, 'a')
            #
            cmd = os.path.realpath(__file__)
            args = ['python3', cmd]
            if subcmd:
                args += subcmd + [infofile]
            else:
                args += ['start_with', infofile]
            env = dict(os.environ)
            # avoid looping 
            env.pop('COLDOC_TASKS_AUTOSTART',None)
            #
            if pythonpath:
                env['PYTHONPATH'] = os.pathsep.join(pythonpath)
            if tempdir:
                for j in ('TMPDIR', 'TEMP', 'TMP'):
                    env[j] = str(tempdir)
            devnull = open(os.devnull)
            proc = subprocess.Popen(args, stdin=devnull, stdout=logfile_,
                                    env = env,
                                    stderr=subprocess.STDOUT, text=True,  cwd=cwd)
            devnull.close()
            if logfile_ != logfile:
                logfile_.close()
        # check it
        ok = server_wait(address, authkey,timeout)
        if ok:
            mychmod(address)
        #
        if not ok:
            logger.critical('Cannot start task process, see %r', getattr(logfile_,'name', logfile_))
            if not use_multiprocessing:
                jt = threading.Thread(target=proc.wait)
                jt.start()
            proc = False
            return proc, infofile
        # check consistency
        real_pid = server_pid(address, authkey)
        end_t = time.time() + timeout
        for repeat_n in range(int(float(timeout)*20.)):
            info_ = tasks_server_readinfo(infofile)
            info_pid = info_[2]
            if info_pid == real_pid or (time.time() > end_t):
                break
            time.sleep(0.05)
        if info_pid != real_pid:
            logger.error('after timeout %g sec, server still inconsistent, %r == info_pid != real_pid == %r',
                         timeout, info_pid, real_pid)
    return proc, infofile

def tasks_daemon_autostart(infofile, **kwargs):
    """ As tasks_daemon_autostart_nolock(), but adds a lock, to avoid race condition,
    i.e. starting two servers with same infofile;
    in case of exception (such as lock timeout),  returns (`False`, exception) ;
    in case of timeout, returns (None, infofile)
    """
    assert isinstance(infofile, (str,bytes, Path))
    if not os.path.isdir( os.path.dirname(infofile)):
        logger.warning("This infofile refers to a non-existant directory %r, cannot lock", infofile)
        return tasks_daemon_autostart_nolock(infofile, **kwargs)
    timeout_ = kwargs.get('timeout', 2.0)
    ret = None, None
    try:
        with mylockfile(infofile+'-autostart', timeout=timeout_):
            ret = tasks_daemon_autostart_nolock(infofile, **kwargs)
    except myLockTimeout as E:
        logger.error('lock timeout %r', E)
        ret = False, E
    except Exception as E:
        logger.exception('while starting server')
        ret = False, E
    return ret


def tasks_daemon_django_autostart(settings, **kwargs):
    """ Check (using information from `settings` module) if there is a server running;
    if there is, return `(PID, info)`,      if not, start it (as a subprocess), and return the `(proc, info)`.
    For keyword arguments, see `tasks_daemon_autostart`.
      """
    _read_django_settings(kwargs, settings)
    #
    kwargs['subcmd'] = ['django_start_with']
    proc, info = tasks_daemon_autostart(**kwargs)
    if proc:
        settings.COLDOC_TASKS_COLDOC_PROC = proc
        settings.COLDOC_TASKS_INFOFILE = info
    return proc, info



########################

def __countthem():
    import UUID.models
    return len(UUID.models.DMetadata.objects.all())


def main(argv):
    assert isinstance(argv, (tuple, list))
    if not argv:
        print( __doc__)
        return False
    if argv[0].startswith('django_'):
        if os.environ.get('DJANGO_SETTINGS_MODULE') is None:
            logger.error('environmental variable DJANGO_SETTINGS_MODULE must be set')
            return False
        # This is crude but it mostly works
        DJANGO_SETTINGS_MODULE = os.environ.get('DJANGO_SETTINGS_MODULE')
        a = DJANGO_SETTINGS_MODULE.replace('.','/') + '.py'
        d = None
        for j in sys.path:
            if os.path.isdir(j) and os.path.isfile(os.path.join(j,a)):
                d = j
                break
        if not d:
            logger.error('Environmental variable DJANGO_SETTINGS_MODULE references a non exixtent file %r. sys.path is\n%s\nTry setting PYTHONPATH', a, str(sys.path))
        #
        # for people hooking this package into Django, this will avoid a recursive server starting
        os.environ['COLDOC_TASKS_AUTOSTART_OPTIONS'] =  'noautostart'
        #
        import django
        django.setup()
        from django.conf import settings
        #
        if argv[0] == 'django_start_with':
            if len(argv)<= 1:
                print( __doc__)
                return False
            info = argv[1]
        else:
            info = getattr(settings, 'COLDOC_TASKS_INFOFILE', None)
        if info is None:
            logger.error('This command needs that `COLDOC_TASKS_INFOFILE` be defined in `settings`')
            return False
        # this is a non advertised command, used to start the daemon subprocess
        if argv[0] ==  'django_start_with':
            return tasks_server_start(infofile=info, with_django=DJANGO_SETTINGS_MODULE)
        if argv[0] ==  'django_start':
            return tasks_django_server_start(settings, infofile= info)
        #
        argv[0]  = argv[0][len('django_'):]
    else:
        if len(argv)<= 1:
            print( __doc__)
            return False
        info = argv[1]
    if  argv[0] in ('start', 'daemon'):
        address = argv[2].encode() if len(argv) > 2 else None
        authkey = argv[3].encode() if len(argv) > 3 else None
        if argv[0] == 'start':
            return tasks_server_start(infofile=info, address=address, authkey=authkey, logfile=True)
        else:
            # re-enable starting daemon
            os.environ['COLDOC_TASKS_AUTOSTART_OPTIONS'] =  ''
            return tasks_daemon_autostart(infofile=info, address=address, authkey=authkey, logfile=True)
    #
    # this is a non advertised command, used to start the daemon subprocess
    if argv[0] == 'start_with':
        return tasks_server_start(infofile=info)
    #
    try:
        address, authkey = tasks_server_readinfo(info)[:2]
        assert address and authkey, 'One of address, authkey is missing'
    except Exception as E:
        print(str(E))
        return False
    #
    #
    if  'ping' == argv[0]:
        z = ping(address, authkey)
        print(z)
        return z
    elif  'pid' == argv[0]:
        z = server_pid(address, authkey)
        print(z)
        return z
    elif 'status' == argv[0]:
        print(status(address, authkey))
        return True
    elif 'test' == argv[0] :
        err = test(address, authkey)
        return err == 0
    elif 'test_hanging'  == argv[0] :
        manager = get_manager(address, authkey)
        return manager.run_cmd__(str,(34,),{})
        # we do not reap the result
        # on shutdown, a warning should appear
    elif  argv[0] in ( 'stop', 'shutdown' ):
        # FIXME this raises a "File Not found error"
        return shutdown(address, authkey)
    else:
        print(__doc__)



if __name__ == '__main__':
    multiprocessing.freeze_support()
    #
    argv = sys.argv[1:]
    if not argv:
        print(__doc__)
        sys.exit(0)
    ret = False
    try:
        ret = main(argv)
    except (SystemExit, KeyboardInterrupt):
        pass
    except Exception as E:
        ret = False
        logger.exception("While %r", argv)
    sys.exit(0 if ret else 1)
