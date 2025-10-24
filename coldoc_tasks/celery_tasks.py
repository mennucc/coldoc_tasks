#!/usr/bin/env python3

__doc__ = """

  start celeryconfig.py

      start Celery server

  daemon celeryconfig.py [logfile]

      start Celery server as daemon

  shutdown celeryconfig.py

      shutdown Celery servers

  test celeryconfig.py

      test Celery server

  status celeryconfig.py

      status of  Celery server
"""


import os, sys, io, importlib.util, functools, inspect
import tempfile, subprocess, threading, time
from pathlib import Path

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
from coldoc_tasks.task_utils import _normalize_pythonpath,  format_exception
from coldoc_tasks.exceptions import *

####################### celery machinery
try:
    import celery
except ImportError:
    logger.error('Cannot import `celery`')
    if __name__ == '__main__':
        sys.exit(1)
    celery = None


# https://stackoverflow.com/a/53080237/5058564
def import_module_from_string(name: str, source: str):
    """
    Import module from source string.
    Example use:
    import_module_from_string("m", "f = lambda: print('hello')")
    m.f()
    """
    spec = importlib.util.spec_from_loader(name, loader=None)
    module = importlib.util.module_from_spec(spec)
    exec(source, module.__dict__)
    sys.modules[name] = module
    globals()[name] = module
    return module

def get_client(celeryconfig_):
    assert celery is not None
    if isinstance(celeryconfig_, io.IOBase):
        celeryconfig_.seek(0)
        celeryconfig_ = compile(celeryconfig_.read(), 'celeryconfig.py', 'exec')
        return _get_client(celeryconfig_)
    elif isinstance(celeryconfig_,str):
        if os.path.isfile(celeryconfig_):
            return _get_client(celeryconfig_, mtime=os.path.getmtime(celeryconfig_))
    return _get_client(celeryconfig_)

@functools.lru_cache(100)
def _get_client(celeryconfig_, mtime = None):
    """ get celery_app from celeryconfig; `celeryconfig` may be:
      `str` that is a filename to be loaded , or 
      `code` already compiled, or
      an io object (such as open file or `StringIO), to read code from.
    """
    if isinstance(celeryconfig_,str):
        if os.path.isfile(celeryconfig_):
            with open(celeryconfig_) as F:
                celeryconfig = import_module_from_string('celeryconfig', F.read())
            ## this sucks
            #s = sys.path
            #sys.path = [ os.path.dirname(celeryconfig_) ]
            #import importlib
            #celeryconfig = importlib.import_module('celeryconfig')
            #sys.path = s
        else:
            logger.error('no such file: %r', celeryconfig_)
            return None
    elif inspect.iscode(celeryconfig_):
        celeryconfig = import_module_from_string('celeryconfig', celeryconfig_)
    else:
        raise RuntimeError('Cannot load celeryconfig. Unsupporte type is %r %r' %( type(celeryconfig_), celeryconfig_))
    #
    assert inspect.ismodule(celeryconfig)
    celery_app = celery.Celery('coldoc_celery_tasks')
    celery_app.config_from_object(celeryconfig)
    return celery_app

############# status


def celery_server_check(celeryconfig_, timeout=0.1):
    if not celery:
        return False
    app = get_client(celeryconfig_)
    i = app.control.inspect(timeout=timeout)
    if not i.ping():
        return False
    return True

ping = celery_server_check

def shutdown(celeryconfig_):
    app = get_client(celeryconfig_)
    return app.control.shutdown()

stop = shutdown

def test(celeryconfig_, print_=print):
    from coldoc_tasks.task_utils import test_fork
    print_('*' * 30 + '(celery forking)')
    FC = functools.partial(fork_class, celeryconfig=celeryconfig_)
    err = test_fork(fork_class=FC)
    print_('*' * 30 + '(celery end)')
    return err


def status(celeryconfig_):
    app = get_client(celeryconfig_)
    #https://stackoverflow.com/a/53856001/5058564
    i = app.control.inspect()
    availability = i.ping()
    stats = i.stats()
    registered_tasks = i.registered()
    active_tasks = i.active()
    scheduled_tasks = i.scheduled()
    result = {
        'availability': availability,
        'stats': stats,
        'registered_tasks': registered_tasks,
        'active_tasks': active_tasks,
        'scheduled_tasks': scheduled_tasks
    }
    return result

################## running

def __celery_run_it(cmd, k, v):
    " internal function, to be wrapped for server and clients"
    try:
        ret = (0, cmd(*k, **v), None)
    except Exception as E:
        ret = (1, E, format_exception(E))
    return ret

def _get_run_it(celery_app):
    " get factotum function, wrapped for server and clients"
    return celery_app.task(name='celery_run_it')(__celery_run_it)



class fork_class(fork_class_base):
    "class that runs a job in a celery task, and returns results or raises exception"
    fork_type = 'celery'
    def __init__(self,  use_fork = True, celeryconfig=None, celery_app=None, timeout=None, queue=None):
        """ pass either `celeryconfig` or `celery_app` (from `get_client` )"""
        assert celery_app or celeryconfig
        super().__init__(use_fork = use_fork )
        self.__cmd = None
        self.__celery_app = celery_app
        self.__celery_run_it = None
        self.__celeryconfig = celeryconfig
        self.__timeout = timeout
    #
    def __getstate__(self):
        self.__celery_app = None
        return self.__dict__
    #
    @staticmethod
    def can_fork():
        return True
    #
    def run(self, cmd, *k, **v):
        assert self.already_run is False
        if self.__celery_app is None:
            self.__celery_app  = get_client(self.__celeryconfig)
        assert self.__celery_app
        self.__celery_run_it = _get_run_it(self.__celery_app)
        #
        self.__cmd_name = cmd.__name__
        if self.use_fork_:
            self.__cmd = self.__celery_run_it.delay(cmd, k, v)
        else:
            try:
                self.__ret = (0, cmd(*k, **v), None)
            except Exception as E:
                self.__ret = (1, E, format_exception(E))
        self.already_run = True
    #
    def terminate(self):
        if self.use_fork_:
            self.__cmd.revoke(terminate=True)
    #
    def wait(self, timeout=None, raise_exception=True):
        timeout = self.__timeout if timeout is None else timeout
        assert self.already_run
        if self.use_fork_ and not self.already_wait:
            self.already_wait = True
            try:
                with celery.result.allow_join_result():
                    self.__ret = self.__cmd.get(timeout=timeout)
            except celery.exceptions.TaskRevokedError as E:
                raise ColdocTasksProcessLookupError('Process %r terminated : %r' % ( self.__cmd_name, E) )
            except celery.exceptions.TimeoutError as E:
                raise ColdocTasksTimeoutError('For cmd %r ' % ( self.__cmd_name, ) )
        if self.__ret[0] :
            self.traceback_ = self.__ret[2]
            self.exception_ = self.__ret[1]
            if raise_exception:
                raise self.__ret[1]
        return self.__ret[1]

def run_server(celeryconfig=None, with_django=None):
    " start Celery server "
    if with_django and celeryconfig is None:
        os.environ.setdefault('DJANGO_SETTINGS_MODULE', with_django)
        os.environ.pop('COLDOC_TASKS_AUTOSTART', None)
        import django
        django.setup()
        from django.conf import settings
        celeryconfig = settings.COLDOC_TASKS_CELERYCONFIG
    #
    app = get_client(celeryconfig)
    if app is None:
        return None
    # HACK register factotum function
    c = _get_run_it(app)
    globals()['celery_run_it'] = c
    #
    worker = app.Worker()
    return worker.start()

def server_wait(celeryconfig, timeout = 2.0):
    " try pinging, up to timeout"
    ok = False
    t = time.time() + timeout
    for j in range(int(float(timeout) * 20.)):
        ok = ping(celeryconfig, timeout=0.05)
        if ok or (time.time() > t) : break
        time.sleep(0.05)
    return ok



def tasks_daemon_autostart(celeryconfig,
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
    """ Check if there is a server running using `celeryconfig`;
    if there is, return `True`,      if not, start it (as a subprocess), and return `proc`
    (where `proc` is either a `subprocess.Popen` or `multiprocessing.Process` instance),
   
    Arguments notes: 
       if `logfile` is `True`, will create a temporary files to store logs;
       (`force` is ignored);
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
        ok = celery_server_check(celeryconfig)
        logger.debug('A daemon is already running')
        if ok:
            return True
    #
    pythonpath = _normalize_pythonpath(pythonpath)
    # this does not work OK
    use_multiprocessing=False
    #
    proc = None
    if not ok and 'noautostart' not in opt:
        #
        logger.info('starting Celery task server')
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
            proc = multiprocessing.Process(target=run_server,
                                           args=(celeryconfig,))
            if flag is None:
                del os.environ['COLDOC_TASKS_AUTOSTART_OPTIONS']
            else:
                os.environ['COLDOC_TASKS_AUTOSTART_OPTIONS'] =  flag
            proc.start()
        else:
            if logfile is True:
                logfile_ = tempfile.NamedTemporaryFile(dir=tempdir,
                                            delete=False,
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
            args += ['start', celeryconfig]
            env = dict(os.environ)
            env['COLDOC_TASKS_AUTOSTART_OPTIONS'] = 'nocheck,noautostart'
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
        ok = server_wait(celeryconfig,timeout)
        if not ok:
            logger.critical('Cannot start task process, see %r', getattr(logfile_,'name', logfile_))
            #if not use_multiprocessing:
            #    jt = threading.Thread(target=proc.wait)
            #    jt.run()
            proc = False
    return proc


############### testing

def __countthem():
    import UUID.models
    return len(UUID.models.DMetadata.objects.all())


def main(argv):
    #
    sourcedir = os.path.dirname(os.path.dirname(__file__))
    #
    if not argv:
        print(__doc__)
        return False
    #
    if 'status' == argv[0] :
        for k,v in status(argv[1]).items():
            print(' --  ',k)
            print(v)
        return True
    #
    if 'ping' == argv[0] :
        ret = celery_server_check(argv[1])
        print('OK' if ret else 'FAILED')
        return ret
    #
    elif 'test' == argv[0]:
        celeryconfig_ =  argv[1]
        ret = test(celeryconfig_)
        if os.environ.get('DJANGO_SETTINGS_MODULE') == 'ColDocDjango.settings':
            f = fork_class(celeryconfig=celeryconfig_)
            f.run(__countthem)
            print('---- test of reading the Django database: there are %d DMetadata objects' % f.wait())
        return ret == 0
    elif  'start' == argv[0] :
        return run_server(celeryconfig=argv[1])
    elif  'daemon' == argv[0] :
        logger.setLevel(logging.DEBUG)
        if len(argv) > 1:
            celeryconfig=argv[1]
        else:
            celeryconfig = os.path.join(sourcedir,'etc','celeryconfig.py')
        if len(argv) > 2:
            logfile = argv[2]
            assert logfile != celeryconfig
        else:
            logfile_ = tempfile.NamedTemporaryFile(delete=False, prefix='coldoc_tasks_celery_server')
            logfile = logfile_.name

        proc = tasks_daemon_autostart(celeryconfig,
                                      logfile=logfile,
                                      pythonpath=(sourcedir,))
        if not proc:
                logger.critical("could not start server! log follows \n" + ('v' * 70) +\
                                open(logfile.name).read() + '\n' +  ('^' * 70))
                return False
        elif proc is True:
            print('already started')
        elif getattr(proc,'pid'):
            print('started as pid: %r' % proc.pid)
        else:
            assert False
        return True
    elif  argv[0] in ('shutdown', 'stop') :
        return shutdown(argv[1])
    else:
        print(__doc__)
        return False
    return False

if __name__ == '__main__':
    #
    if celery is None:
        logger.critical('Cannot load `celery` module')
        sys.exit(1)
    #
    argv = sys.argv[1:]
    ret = False
    try:
        ret = main(argv)
    except (SystemExit, KeyboardInterrupt):
        pass
    except Exception as E:
        logger.exception("While %r", argv)
        ret = False
    sys.exit(0 if ret else 1)

