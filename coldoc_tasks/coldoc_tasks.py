
__doc__ = """

  start infofile socket authkey 
  
      start server
      
      socket is the file for the socket;
      
      infofile is a file where informations
       regarding the server are stored
       
       authkey is a password for the server
       (if missing, a random one will be generated)

  django_server_start

     start server, reading credentials from django settings
     (see below)

  start_from infofile
  
     read the information from the infofile and start it

  stop infofile
  django_server_stop

      stop server

  test infofile
  django_server_test

      test server

  test_hanging infofile
  django_server_test_hanging

      create hanging job in  server

  status infofile
  django_server_status

      status of server

  ping infofile
  django_server_ping
  
      ping server
      
For each command, there is `django_server..` version:
this versione initializes a django instance, and looks
into the settings for `COLDOC_TASKS_INFOFILE` to know
where the infofile is; when starting the server,
moreover, it gets the authkey from settings.COLDOC_TASKS_PASSWORD
and the socket filename from  settings.COLDOC_TASKS_SOCKET
and writes the infofile
"""


import os, sys, time, pickle, base64, functools, multiprocessing, multiprocessing.managers
import random, socket, struct, tempfile, copy, threading
from pathlib  import Path

try:
    import psutil
except ImportError:
    psutil = None

try:
    import lockfile
except ImportError:
    lockfile = None


default_tempdir = tempfile.gettempdir()

import logging
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    a = os.path.realpath(os.path.dirname(__file__))
    if a in sys.path:
        del sys.path[sys.path.index(a)]
    a = os.path.dirname(a)
    if a not in sys.path:
        sys.path.insert(0,a)
    

from coldoc_tasks.simple_tasks import fork_class_base



__all__ = ('get_manager', 'run_server', 'ping', 'status', 'shutdown', 'test', 'fork_class',
           'run_cmd', 'wait', 'get_result', 'join',
           'tasks_daemon_autostart', 'tasks_daemon_django_autostart',
           'tasks_server_readinfo', 'tasks_server_writeinfo', 'tasks_server_start', 'task_server_check')

default_chmod = 0o600

def _mychmod(f, mode=default_chmod):
    try:
        os.chmod(f, mode)
    except:
        logger.exception('Why cant I set chmod %r on %r', mode, f)

##########################

actions = ('ping__','status__','shutdown__',
           'run_cmd__','get_result_join__','join__','get_wait_socket__',
           'terminate__')

@functools.lru_cache(100)
def get_manager(address, authkey):
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
        _mychmod(socket_name)
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



def __send_message(m, F):
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
        s.connect(F)
        a = s.recv(5)
        if a == b'#AUTH':
            if auth is None:
                raise RuntimeError('Auth is required for %r' % F)
            s.sendall(auth)
            a = s.recv(5)
        if a != b'#HELO':
            if auth:
                raise RuntimeError('Wrong Auth for %r' % F)
            else:
                raise RuntimeError('Unexpected hello %r from %r', a, F)
        s.sendall(m)
        if m == b'#SEND':
            l = s.recv(8)
            l = struct.unpack('<Q', l)[0]
            rets = s.recv(l)
            ret = pickle.loads(rets)
        elif m == b'#SENT':
            l = s.recv(1)
            l = struct.unpack('<B', l)[0]
            ret = bool(l)
        a = s.recv(5)
        if a not in ( b'#ACK ', b'#GONE' ):
            logger.warning('Unexpected response %r', a)
            ret = False
    return ret

######################### starting jobs

def run_cmd(manager, cmd, args, kwarks):
    " run a command "
    proxy = manager.run_cmd__(cmd, args, kwarks)
    return proxy._getvalue()

def wait(id_, manager):
    " wait for command execution to end "
    F = manager.get_wait_socket__(id_)
    F = F._getvalue()
    if F is not None:
        return __send_message(b'#WAIT', F)

def get_result(id_, manager):
    """ get command result , as a pair (status, result),
      where status is 0 or 1, 
      if status is 0, result is the result
      if status is 1, result contain the exception given by the command
    """
    F = manager.get_wait_socket__(id_)
    F = F._getvalue()
    if F is not None:
        return __send_message(b'#SEND', F)
    return None

def join(id_, manager):
    " let the subprocess of the command terminate gracefully "
    F = manager.get_wait_socket__(id_)
    F = F._getvalue()
    if F is not None:
        return __send_message(b'#QUIT', F)


######################## starting jobs, by class

class fork_class(fork_class_base):
    "class that runs a job in a subprocess, and returns results or raises exception"
    def __init__(self, address, authkey, use_fork = True):
        super().__init__(use_fork = use_fork )
        self.__cmd_id = None
        self.__ret = (2, RuntimeError('This should not happen'))
        self.__manager = None
        self.__address = address
        self.__authkey = authkey
    #
    def __getstate__(self):
        self.__manager = None
        return self.__dict__
    #
    @staticmethod
    def can_fork():
        return hasattr(socket,'AF_UNIX')
    #
    def run(self, cmd, *k, **v):
        assert self.already_run is False
        self.__cmd_name = cmd.__name__
        self.__ret = (2, RuntimeError('Process %r : could not read its return value' % ( self.__cmd_name,) ))
        if self.use_fork_:
            if self.__manager is None:
                self.__manager = get_manager(self.__address, self.__authkey)
            proxy = self.__manager.run_cmd__(cmd, k, v)
            self.__cmd_id = proxy._getvalue()
        else:
            try:
                self.__ret = (0, cmd(*k, **v))
            except Exception as E:
                self.__ret = (1, E)
        self.already_run = True
    #
    def terminate(self):
        if self.use_fork_:
            if self.__manager is None:
                self.__manager = get_manager(self.__address, self.__authkey)
            self.__manager.terminate__(self.__cmd_id)
    #
    def wait(self, timeout=None):
        if timeout is not None:
            logger.warning('Timeout not implemented')
        assert self.already_run
        if self.use_fork_ and not self.already_wait:
            if self.__manager is None:
                self.__manager = get_manager(self.__address, self.__authkey)
            self.already_wait = True
            try:
                self.__ret = get_result(self.__cmd_id, self.__manager)
                if self.__ret is None:
                    raise RuntimeError('Process has disappeared: %s',self.__cmd_id)
                self.__manager.join__(self.__cmd_id)
            except Exception as E:
                raise RuntimeError('Process %r exception on wait : %r' % ( self.__cmd_name, E) )
        if self.__ret[0] :
            raise self.__ret[1]
        return self.__ret[1]


############################## status

def ping(address, authkey, warn=True):
    try:
        manager = get_manager(address, authkey)
        return manager.ping__()
    except Exception as E:
        if warn:
            logger.warning('When pinging %r',E)


def status(address, authkey):
    try:
        manager = get_manager(address, authkey)
        return manager.status__()
    except Exception as E:
        logger.warning('When status %r',E)

def shutdown(address, authkey):
    try:
        manager = get_manager(address, authkey)
        return manager.shutdown__()
    except Exception as E:
        logger.warning('When shutdown %r',E)

def test(address, authkey):
    import coldoc_tasks.task_utils as task_utils
    logger.setLevel(logging.INFO)
    FC = functools.partial(fork_class, address=address, authkey=authkey)
    task_utils.logger.setLevel(logging.INFO)
    print('*' * 80)
    err = task_utils.test_fork(fork_class=FC)
    print('*' * 80)
    FCN = functools.partial(fork_class, use_fork = False, address=address, authkey=authkey)
    err += task_utils.test_fork(fork_class=FCN)
    print('*' * 80)
    if os.environ.get('DJANGO_SETTINGS_MODULE') == 'ColDocDjango.settings':
        f = FC()
        f.run(__countthem)
        print('---- test of reading the Django database: there are %r DMetadata objects' % f.wait())
    return ret


############## server code




######

def _fork_mp_wrapper(*args, **kwargs):
    id_, pipe, socket_, access_pair_, cmd, k , v, l = args
    multiprocessing.log_to_stderr(level=l)
    #
    logger = multiprocessing.get_logger() 
    logger.info('Start id %s cmd %r %r %r ', id_, cmd, k, v)
    try:
        ret = (0, cmd(*k, **v))
    except Exception as E:
        ret = (1, E)
    logger.info('Cmd %r id %s return %r', id_, cmd, ret)
    pipe.send(ret)
    rets = pickle.dumps(ret)
    #
    __socket_server(socket_, access_pair_, rets, id_)
    F = access_pair_[0]
    logger.info('Exited socket loop, removing %r', F)
    os.unlink(F)


def run_server(address, authkey, with_django=False, tempdir=default_tempdir):
    manager = multiprocessing.managers.SyncManager(address=address, authkey=authkey)
    #
    return_code = True
    # currently tje code work better without a subprocess
    run_with_subprocess = False
    #
    L = multiprocessing.log_to_stderr()
    global logger
    L.setLevel(logger.getEffectiveLevel())
    logger = L
    pool = server = None
    processes = {}
    try:
        if with_django:
            os.environ.setdefault('DJANGO_SETTINGS_MODULE', with_django)
            import django
            django.setup()
        if 0:
            # FIXME it would be better to use a Pool, but processes hang forever
            pool = multiprocessing.pool.Pool()
            z = pool.apply_async(str,(2,))
            z.wait()
            d = z.get()
            logger.info('********** %r', d)
        #
        randomsource = random.Random(os.urandom(8))
        #
        if  hasattr(randomsource,'randbytes'):
            randbytes = randomsource.randbytes
        else: #older Python
            def randbytes(n):
                return randomsource.getrandbits(n * 8).to_bytes(n, 'little')
        #
        # used if run_with_subprocess is True
        __do_run = multiprocessing.Value('i')
        def stop_server__():
            logger.info('Received shutdown')
            __do_run.value = 0
        #
        Nooone = (None, None, None)
        def run_cmd__(c,k,v,pipe=None):
            id_ = base64.b64encode(randbytes(9),altchars=b'-_').decode('ascii')
            F = os.path.join(tempdir, 'socket_' + id_)
            socket_ = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            socket_.bind(F)
            socket_.listen(2)
            _mychmod(F)
            #
            if pipe is None:
                pipe = multiprocessing.Pipe()
            l = logger.getEffectiveLevel()
            #
            ## I would like to use:
            #z = pool.apply_async(c,k,v)
            ## but it hangs
            # add auth
            auth_ = randbytes(8)
            access_pair_ = (F, auth_)
            #
            proc = multiprocessing.Process(target=_fork_mp_wrapper, args=(id_, pipe[1], socket_, access_pair_, c, k, v, l))
            proc.name = 'coldoc_task '+id_ + ' ' + repr(c.__name__)
            proc.start()
            pipe0 = pipe[0]
            #pipe0._config['authkey'] = bytes(pipe0._config['authkey'])
            processes[id_] = (proc, pipe0, access_pair_)
            logger.debug('Running cmd %r ( %r , %r ), id = %r, socket = %r', c,  k, v, id_, F)
            return id_
        #
        def get_wait_socket__(id_):
            id_ = str(id_)
            logger.debug('getting result pipe for  id = %r ',  id_)
            proc, pipe, F = processes.get(id_, Nooone)
            return F
        #
        def get_result_join__(id_):
            logger.debug('getting result for  id = %r ',  id_)
            id_ = str(id_)
            proc, pipe, F = processes.pop(id_, Nooone)
            if proc is not None:
                try:
                    logger.info('Waiting for result id = %r',  id_)
                    r = __send_message(b'#SEND', F) #get_result(id_) #pipe.recv()
                    __send_message(b'#QUIT', F)
                    proc.join()
                except  Exception as E:
                    r = (1, E)
            else:
                logger.error('No process by id %r',id_)
                r = (1, RuntimeError('No process by id %r' % id_))
            return r
        #
        def terminate__(id_):
            logger.debug('getting result for  id = %r ',  id_)
            id_ = str(id_)
            proc, pipe, F = processes.pop(id_, Nooone)
            if proc is not None:
                proc.terminate()
                return True
            return False
        #
        def status__():
            return { 'processes' : processes }
        #
        def ping__():
            return True
        #
        def join__(id_):
            logger.debug('joining  id = %r ',  id_)
            id_ = str(id_)
            proc, pipe, F = processes.pop(id_, Nooone)
            if proc is not None:
                try:
                    logger.info('Joining id = %r',  id_)
                    sent = __send_message(b'#SENT', F)
                    if not sent:
                        logger.critical('The result of process %s was never recovered', id_)
                    __send_message(b'#QUIT', F)
                    proc.join()
                except  Exception as E:
                    logger.exception('Unexpected exception from id_ %r : %r', id_, E)
            else:
                logger.error('No process by id %r',id_)
        #
        def initializer():
            a = set(copy.deepcopy(manager._registry))
            manager.register('run_cmd__', run_cmd__)
            manager.register('get_result_join__', get_result_join__)
            manager.register('join__', join__)
            manager.register('get_wait_socket__',get_wait_socket__)
            manager.register('terminate__',terminate__)
            manager.register('ping__',ping__)
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
        # run the server
        if run_with_subprocess:
            logger.info('Start manager')
            manager.start()
            __do_run.value = 1
            while __do_run.value:
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
        return_code = False
    try:
        logger.info('Shutting down')
        for id_  in processes.keys():
            join__(id_)
        if pool:
            pool.close()
            pool.join()
        if run_with_subprocess:
            manager.shutdown()
            manager.join()
    except (KeyboardInterrupt, SystemExit):
        pass
    except Exception:
        logger.exception('When cleaning after run_server')
        return_code =  False
    return return_code


def server_wait(address, authkey, timeout = 2.0):
    " try pinging, up to timeout"
    ok = False
    for j in range(int(float(timeout) * 20.)):
        ok = ping(address, authkey, warn=False)
        if ok: break
        time.sleep(0.05)
    return ok

######################## code to start server

def tasks_server_readinfo(infofile):
    " reads address, authkey, pid"
    address=None
    authkey=None
    pid=None
    #
    for line in open(infofile):
        line = line.strip()
        #
        if line.startswith('pid='):
            pid = int(line[4:])
        #
        elif line.startswith('auth64='):
            line=line[7:].strip()
            try:
                a = line.encode('ascii')
                authkey = base64.b32decode(a)
            except Exception as E:
                logger.warning('In info file %r error parsing auth64 %r: %r', infofile, line, E)
        #
        elif line.startswith('address='):
            line=line[8:].strip()
            try:
                address = eval(line, {'__builtins__':{}}, {})
            except Exception as E:
                logger.warning('In info file %r error parsing address %r: E', infofile, line, E)
        #
        elif line and  not line.startswith('#'):
            logger.warning('In info file %r ignored line %r', infofile, line)
        #
    return address, authkey, pid

def tasks_server_writeinfo(infofile, address, authkey, pid=None):
    with open(infofile,'w') as F:
        _mychmod(infofile)
        if pid:
            F.write('pid=%d\n' % (pid,) )
        F.write('address=%s\nauth64=%s\n' %\
                (repr(address), base64.b32encode(authkey).decode('ascii')))

def __tasks_server_start_nolock(address, authkey, infofile, with_django=None, tempdir=default_tempdir):
    tasks_server_writeinfo(infofile, address, authkey, os.getpid())
    ret = False
    try:
        ret = run_server(address, authkey, with_django, tempdir)
    except:
        logger.exception('When running task server')
    return ret

def tasks_server_start(address, authkey, infofile, with_django=None, tempdir=default_tempdir):
    " start a server with `address` and `authkey` ,  saving info in `infofile (that is locked while in use)"
    if lockfile:
        lock = lockfile.FileLock(infofile, timeout=2)
        with lock:
            return __tasks_server_start_nolock(address, authkey, infofile, with_django, tempdir)
    else:
        return __tasks_server_start_nolock(address, authkey, infofile, with_django, tempdir)


def task_server_check(info):
    """ accepts the infofile
    returns  `status, sock, auth, pid` , where `status` is a boolean"""
    if os.path.isfile(info):
        sock, auth, pid = tasks_server_readinfo(info)
        if not( sock and auth):
            logger.error('One of address, authkey is missing from %r',info)
            return False, sock, auth, pid
        if psutil and pid and not psutil.pid_exists(pid):
            logger.warning('Tasks server pid %r does not exist', pid)
            pid = None
            return False, sock, auth, pid
        if pid and ping(address=sock, authkey=auth):
            return True, sock, auth, pid
        else:
            logger.warning('Tasks server pid %r is not responding', pid)
            return False, sock, auth, pid
    return False, None, None, None



def tasks_daemon_autostart(infofile, sock=None, auth=None,
                           pythonpath=(),
                           cwd=None,
                           logfile = None,
                           opt = None,
                           tempdir = default_tempdir,
                           timeout = 2.0,
                           force = False,
                           # this option is used to wrap this function for Django...
                           subcmd=None,
                           ):
    """ Check if there is a server running using `infofile`;
    if there is, return the PID,      if not, start it (as a subprocess), and return the process
    (either as `subprocess.Popen` or `multiprocessing.Process` instance).
   
    Arguments notes: 
        `sock` is the socket (if `None`, it will be read from `infofile`, that must exist);
       if `auth` is `None`, generate a random one;
       if `logfile` is `True`, will create a temporary files to store logs;
       if `force` is True, and the server cannot be contacted, remove lock and socket;
       `pythonpath` may be a string, in the format of PYTHONPATH, or a list:
        it  will be added to sys.path.
       
    The env variable 'COLDOC_TASKS_AUTOSTART_OPTIONS' may be used to tune this functions,
    it accepts a comma-separated list of keywords from `nocheck`, `noautostart`, `force` .
    The argument `opt` overrides that env variable. The argument `force`, if set, ignores the previous two.
      """
    #
    #
    ok = False
    if opt is None:
        opt = os.environ.get('COLDOC_TASKS_AUTOSTART_OPTIONS','').split(',')
    elif isinstance(opt,str):
        opt = opt.split(',')
    if 'nocheck' not in opt:
        ok, sock_, auth_, pid_ = task_server_check(infofile)
        if ok:
            return pid_
    else:
        sock_ = auth_ = pid_  = None,
    #
    if force is None:
        force = 'force' in opt
    if force and not ok:
        if os.path.exists(infofile+'.lock'):
            logger.warning('Removing stale lock %r', (infofile+'.lock',))
            os.unlink(infofile+'.lock')
        if isinstance(sock_,str) and os.path.exists(sock_):
            logger.warning('Removing stale socket %r', (sock_,))
            os.unlink(sock_)
    #
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
    if not all(isinstance(x,str) for  x in pythonpath):
        logger.error('Weird pythonpath %r', pythonpath)
    # this does not work OK
    use_multiprocessing=False
    #
    proc = None
    if not ok and 'noautostart' not in opt:
        #
        logger.info('starting task server')
        #
        sock = sock if sock is not None else sock_
        if sock is None:
            raise RuntimeError('The argument `sock` is not set, and no `infofile`')
        auth = auth or os.urandom(8)
        assert isinstance(auth, bytes)
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
                                           args=(sock, auth, infofile))
            if flag is None:
                del os.environ['COLDOC_TASKS_AUTOSTART_OPTIONS']
            else:
                os.environ['COLDOC_TASKS_AUTOSTART_OPTIONS'] =  flag
            proc.start()
        else:
            if logfile is True:
                logfile_ = tempfile.NamedTemporaryFile(dir=os.path.dirname(infofile),
                                            delete=False,
                                            prefix='coldoc_tasks_', suffix='.log')
            elif isinstance(logfile, (str,bytes,Path)):
                logfile_ = open(logfile, 'a')
            else:
                if logfile is not None:
                    logger.error('parameter `logfile` is of unsupported type %r', type(logfile))
                logfile_ = open(os.devnull, 'a')
            #
            cmd = os.path.realpath(__file__)
            args = ['python3', cmd]
            if subcmd:
                args += subcmd
            else:
                tasks_server_writeinfo(infofile, sock, auth)
                args += ['start_with', infofile]
            env = dict(os.environ)
            env['COLDOC_TASKS_AUTOSTART_OPTIONS'] = 'nocheck,noautostart'
            if pythonpath:
                env['PYTHONPATH'] = os.pathsep.join(pythonpath)
            import subprocess
            proc = subprocess.Popen(args, stdin=open(os.devnull), stdout=logfile_,
                                    env = env,
                                    stderr=subprocess.STDOUT, text=True,  cwd=cwd)
        # check it
        ok = server_wait(sock, auth,timeout)
        if ok:
            _mychmod(sock)
        #
        if not ok:
            logger.critical('Cannot start task process, see %r', getattr(logfile_,'name', logfile_))
            if not use_multiprocessing:
                jt = threading.Thread(target=proc.wait)
                jt.run()
            proc = False
    return proc

def tasks_daemon_django_autostart(settings, **kwargs):
    """ Check (using information from `settings` module) if there is a server running;
    if there is, return the PID,      if not, start it (as a subprocess), and return the process.
    For keyword arguments, see `tasks_daemon_autostart`.
      """
    info = settings.COLDOC_TASKS_INFOFILE
    sock = settings.COLDOC_TASKS_SOCKET
    auth = getattr(settings, 'COLDOC_TASKS_PASSWORD', None)
    logfile = getattr(settings, 'COLDOC_TASKS_LOGFILE', None)
    #
    kwargs['auth'] = auth
    kwargs['logfile'] = logfile
    kwargs['subcmd'] = ['django_server_start']
    proc = tasks_daemon_autostart(info, sock, **kwargs)
    settings.COLDOC_TASKS_PROC = proc
    return proc

def tasks_server_join(proc):
    " join, that is, wait for subprocess to end; to be used after `shutdown` is called, to avoid zombies"
    if isinstance(proc, int):
        if psutil:
            if psutil.pid_exists(proc):
                os.wait(proc)
    elif hasattr(proc, 'join'):
        proc.join()
    elif hasattr(proc, 'wait'):
        proc.wait()
    else:
        logger.warning("should wait for process %r", proc)



########################

def __countthem():
    import UUID.models
    return len(UUID.models.DMetadata.objects.all())


def main(argv):
    assert isinstance(argv, (tuple, list))
    if argv[0].startswith('django_server_'):
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
            logger.error('Environmental variable DJANGO_SETTINGS_MODULE references a non exixtent file %r. sys.path is\n' +\
                         str(sys.path) +    '\nTry setting PYTHONPATH',a)
        #
        # for people hooking this package into Django, this will avoid a recursive server starting
        os.environ['COLDOC_TASKS_AUTOSTART_OPTIONS'] =  'noautostart'
        #
        import django
        django.setup()
        from django.conf import settings
        info = settings.COLDOC_TASKS_INFOFILE
        #
        if argv[0] == 'django_server_start':
            auth = settings.COLDOC_TASKS_PASSWORD
            sock = settings.COLDOC_TASKS_SOCKET
            tempdir = getattr( settings, 'COLDOC_TMP_ROOT', default_tempdir)
            return tasks_server_start(address=sock, authkey=auth, infofile=info,
                                      with_django=os.environ.get('DJANGO_SETTINGS_MODULE'),
                                      tempdir=tempdir)
        argv[0]  = argv[0][len('django_server_'):]
    else:
        if len(argv)<= 1:
            print( __doc__)
            return False
        info = argv[1]
    if  'start' == argv[0] and len(argv) in (3,4):
        infofile, address= argv[1:3]
        if len(argv) == 3:
            authkey = os.urandom(8)
        else:
            authkey =  argv[3]
            authkey = authkey.encode()
        return tasks_server_start(address, authkey, infofile)
    #
    try:
        address, authkey, pid = tasks_server_readinfo(info)
        assert address and authkey, 'One of address, authkey is missing'
    except Exception as E:
        print(str(E))
        return False
    #
    if argv[0] == 'start_with':
        #
        if not authkey:
            # currently not reached
            authkey = os.urandom(8)
        #
        return tasks_server_start(address=address, authkey=authkey, infofile= info)
    #
    elif  'ping' == argv[0]:
        z = ping(address, authkey)
        print(z)
        return z
    elif 'status' == argv[0]:
        manager = get_manager(address, authkey)
        print(str(manager.status__()))
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
    except:
        ret = False
        logger.exception("While %r", argv)
    sys.exit(1 if ret else 0)
