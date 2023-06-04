
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


import os, sys, time, pickle, base64, lockfile, argparse, functools, multiprocessing, multiprocessing.managers
import random, socket, struct, tempfile

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



__all__ = ('get_client', 'run_server', 'ping', 'status', 'shutdown', 'fork_class',
           'run_cmd', 'wait', 'get_result', 'join',
           'tasks_server_readinfo', 'tasks_server_writeinfo', 'tasks_server_start', 'task_server_check')

##########################

actions = ('ping__','status__','shutdown__',
           'run_cmd__','get_result_join__','join__','get_wait_socket__',
           'terminate__')

@functools.lru_cache
def get_manager(address, authkey):
    manager = multiprocessing.managers.SyncManager(address=address, authkey=authkey)
    for j in actions:
        manager.register(j)
    manager.connect()
    return manager

########################

def __socket_server(socket_, rets, id_):
    sent = False
    with socket_ as s:
        logger.debug('Id %s listening', id_)
        while True:
            conn, addr = s.accept()
            with conn:
                logger.debug('Id %s connected by %r', id_, addr)
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



def __send_message(m,F):
    assert isinstance(m,bytes) and len(m) == 5
    ret = None
    if F is  None:
        return None
    ret = False
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.connect(F)
        a = s.recv(5)
        if a != b'#HELO':
            logger.warning('Unexpected hello %r', a)
            return False
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
    proxy = self.__manager.run_cmd__(cmd, args, kwarks)
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

def ping(address, authkey):
    try:
        manager = get_manager(address, authkey)
        return manager.ping__()
    except Exception as E:
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

############## server code




######

def _fork_mp_wrapper(*args, **kwargs):
    id_, pipe, socket_, F, cmd, k , v, l = args
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
    __socket_server(socket_, rets, id_)
    logger.info('Exited socket loop, removing %r', F)
    os.unlink(F)


def run_server(address, authkey, with_django=False, tempdir=default_tempdir):
    manager = multiprocessing.managers.SyncManager(address=address, authkey=authkey)
    #
    run_with_subprocess = False
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
        random.seed(os.urandom(8))
        # used if run_with_subprocess is True
        __do_run = multiprocessing.Value('i')
        def stop_server__():
            logger.info('Received shutdown')
            __do_run.value = 0
        #
        Nooone = (None, None, None)
        def run_cmd__(c,k,v,pipe=None):
            id_ = base64.b64encode(random.randbytes(9),altchars=b'-_').decode('ascii')
            F = os.path.join(tempdir, 'socket_' + id_)
            socket_ = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            socket_.bind(F)
            socket_.listen(2)
            os.chmod(F,0o600)
            #
            if pipe is None:
                pipe = multiprocessing.Pipe()
            l = logger.getEffectiveLevel()
            #
            ## I would like to use:
            #z = pool.apply_async(c,k,v)
            ## but it hangs
            #
            proc = multiprocessing.Process(target=_fork_mp_wrapper, args=(id_, pipe[1], socket_, F, c, k, v, l))
            proc.name = 'coldoc_task '+id_ + ' ' + repr(c.__name__)
            proc.start()
            pipe0 = pipe[0]
            #pipe0._config['authkey'] = bytes(pipe0._config['authkey'])
            processes[id_] = (proc, pipe0, F)
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
            a = set(manager._registry)
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

def tasks_server_writeinfo(address, authkey, pid, infofile):
    with open(infofile,'w') as F:
        os.chmod(infofile,0o600)
        F.write('pid=%d\naddress=%s\nauth64=%s\n' %\
                (os.getpid(), repr(address),
                 base64.b32encode(authkey).decode('ascii')))

def __tasks_server_start_nolock(address, authkey, infofile, with_django=None, tempdir=default_tempdir):
    tasks_server_writeinfo(address, authkey, os.getpid(), infofile)
    ret = False
    try:
        ret = run_server(address, authkey, with_django, tempdir)
    except:
        logger.exception('When running task server')
    return ret

def tasks_server_start(address, authkey, infofile, with_django=None, tempdir=default_tempdir):
    " start a server with `address` and `authkey` ,  saving info in `infofile (that is locked while in use)"
    if lockfile:
        lock = lockfile.FileLock(infofile)
        with lock:
            return __tasks_server_start_nolock(address, authkey, infofile, with_django, tempdir)
    else:
        return __tasks_server_start_nolock(address, authkey, infofile, with_django, tempdir)


def task_server_check(info):
    """ accepts the infofile
    returns  `status, sock, auth, pid` , where `status` is a boolean"""
    if os.path.isfile(info):
        sock, auth, pid = tasks_server_readinfo(info)
        if ping(address=sock, authkey=auth):
            return True, sock, auth, pid
        else:
            logger.warning('Tasks server pid %r is not responding', pid)
            return False, sock, auth, pid
    return False, None, None, None


########################

def __countthem():
    import UUID.models
    return len(UUID.models.DMetadata.objects.all())


def main(argv):
    if argv[0].startswith('django_server_'):
        if os.environ.get('DJANGO_SETTINGS_MODULE') is None:
            logger.error('environmental variable DJANGO_SETTINGS_MODULE must be set')
            return False
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
    elif len(argv)<= 1:
        print( __doc__)
        return False
    if  'start' == argv[0] and len(argv) in (3,4):
        infofile, address= argv[1:3]
        if len(argv) == 3:
            authkey = os.urandom(8)
        else:
            authkey =  argv[3]
            authkey = authkey.encode()
        return tasks_server_start(address, authkey, infofile)
    #
    info = argv[1]
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
