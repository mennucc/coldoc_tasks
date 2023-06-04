
__doc__ = """

  start celeryconfig.py

      start Celery server

  test celeryconfig.py

      test Celery server

  status celeryconfig.py

      status of  Celery server
"""


import os, sys, io, importlib.util, functools, inspect


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


@functools.lru_cache
def get_client(celeryconfig_):
    """ get celery_app from celeryconfig; `celeryconfig` may be:
      `str` that is a filename to be loaded , or 
      `code` already compiled, or
      an io object (such as open file or `StringIO), to read code from.
    """
    if isinstance(celeryconfig_, io.IOBase):
        celeryconfig_.seek(0)
        celeryconfig = import_module_from_string('celeryconfig', celeryconfig_.read())
    elif isinstance(celeryconfig_,str):
        if os.path.isfile(celeryconfig_):
            celeryconfig = import_module_from_string('celeryconfig', open(celeryconfig_).read())
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


def celery_server_check(celeryconfig_):
    if not celery:
        return False
    app = get_client(celeryconfig_)
    i = app.control.inspect()
    if not i.ping():
        return False
    return True

ping = celery_server_check


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
    return cmd(*k, **v)

def _get_run_it(celery_app):
    " get factotum function, wrapped for server and clients"
    return celery_app.task(name='celery_run_it')(__celery_run_it)



class fork_class(fork_class_base):
    "class that runs a job in a celery task, and returns results or raises exception"
    def __init__(self,  use_fork = True, celeryconfig=None, celery_app=None):
        """ pass either `celeryconfig` or `celery_app` (from `get_client` )"""
        assert celery_app or celeryconfig
        super().__init__(use_fork = use_fork )
        self.__cmd = None
        self.__celery_app = celery_app
        self.__celery_run_it = None
        self.__celeryconfig = celeryconfig
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
            self.__ret = cmd(*k, **v)
        self.already_run = True
    #
    def terminate(self):
        if self.use_fork_:
            self.__cmd.revoke(terminate=True)
    #
    def wait(self, timeout=None):
        assert self.already_run
        if self.use_fork_ and not self.already_wait:
            self.already_wait = True
            try:
                with celery.result.allow_join_result():
                    self.__ret = self.__cmd.get(timeout=timeout)
            except celery.exceptions.TaskRevokedError as E:
                raise RuntimeError('Process %r terminated : %r' % ( self.__cmd_name, E) )
        return self.__ret

def run_server(celeryconfig=None, with_django=None):
    " start Celery server "
    if with_django and celeryconfig is None:
        os.environ.setdefault('DJANGO_SETTINGS_MODULE', with_django)
        import django
        django.setup()
        from django.conf import settings
        celeryconfig = settings.CELERYCONFIG
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

############### testing

def __countthem():
    import UUID.models
    return len(UUID.models.DMetadata.objects.all())


def main(argv):
    #
    if not argv:
        print(__doc__)
        return False
    #
    if 'status' == argv[0] :
        celery_app = get_client(argv[1])
        for k,v in status(celery_app).items():
            print(' --  ',k)
            print(v)
        return True
    #
    if 'ping' == argv[0] :
        return celery_server_check(argv[1])
    #
    elif 'test' == argv[0]:
        from coldoc_tasks.task_utils import test_fork
        FC = functools.partial(fork_class, celeryconfig=argv[1])
        ret = test_fork(fork_class=FC)
        if os.environ.get('DJANGO_SETTINGS_MODULE') == 'ColDocDjango.settings':
            f = FC()
            f.run(__countthem)
            print('---- test of reading the Django database: there are %d DMetadata objects' % f.wait())
        return ret == 0
    elif  'start' == argv[0] :
        return run_server(celeryconfig=argv[1])
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
    except:
        logger.exception("While %r", argv)
        ret = False
    sys.exit(1 if ret else 0)

