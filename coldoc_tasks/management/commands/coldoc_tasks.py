
import sys, os, logging


from django.core.management.base import BaseCommand
from django.utils import autoreload
from django.conf import settings

logger = logging.getLogger(__name__)

from coldoc_tasks.coldoc_tasks import tasks_daemon_django_autostart , tasks_server_readinfo, tasks_django_server_start, \
     ping, test, status, shutdown

class Command(BaseCommand):
    help = 'Start or ping or test the ColDoc task server'

    # Command options are specified in an abstract way to enable Django < 1.8 compatibility
    OPTIONS = (
        (('--subcmd', ), {
            #'action': 'store_true',
            'dest': 'subcmd',
            'help': 'Subcommand to run, one of: autostart, start, stop, shutdown, test, ping',
            'required': True
        }),
        (('--log-std', ), {
            'action': 'store_true',
            'dest': 'log_std',
            'help': 'Redirect stdout and stderr to the logging system',
        }),
        (('--dev', ), {
            'action': 'store_true',
            'dest': 'dev',
            'help': 'Auto-reload your code on changes. Use this only for development',
        }),
    )

    # Used in Django >= 1.8
    def add_arguments(self, parser):
        for (args, kwargs) in self.OPTIONS:
            parser.add_argument(*args, **kwargs)

    def __init__(self, *args, **kwargs):
        super(Command, self).__init__(*args, **kwargs)
        self.sig_manager = None

    def run(self, *args, **options):
        # the return value of this function must be a string
        subcmd = options.get('subcmd')
        log_std = options.get('log_std', False)
        is_dev = options.get('dev', False)
        #
        # this will avoid a recursive server starting
        # FIXME but is set too late
        os.environ['COLDOC_TASKS_AUTOSTART_OPTIONS'] =  'nocheck,noautostart'
        #
        if is_dev:
            # raise last Exception is exist
            autoreload.raise_last_exception()
        ### FIXME
        #if log_std:
        #    pass
        #
        if subcmd == 'autostart':
            proc, info = tasks_daemon_django_autostart(settings, opt='')
            if proc:
                logger.info('Server is running, proc = %r, info = %r', proc, info)
                return 'Server is running: ' + repr(proc)
            else:
                logger.error('Server autostart failed')
                return 'Server autostart failed!!'
        #
        info = getattr(settings, 'COLDOC_TASKS_INFOFILE', False)
        if not info:
            logger.critical('`settings` file misconfigured, `COLDOC_TASKS_INFOFILE` is not defined')
            return
        #
        if subcmd == 'start':
            ret = tasks_django_server_start(settings)
            return 'Server ended reporting : ' + repr(ret)
        #
        if not os.path.isfile( info):
            logger.critical('`COLDOC_TASKS_INFOFILE` does not exist %r', info)
            return
        address, authkey = tasks_server_readinfo(info)[:2]
        #
        if subcmd == 'stop':
            subcmd = 'shutdown'
        #
        if subcmd in ('ping', 'test', 'status', 'shutdown'):
            s = globals()[subcmd]
            a = s(address, authkey)
            #print(a)
            return 'Command returned: ' +  repr(a)
        
        return 'Unknown sub command: ' + repr(subcmd)


    def handle(self, *args, **options):
        # the return value of this function must be a string
        is_dev = options.get('dev', False)
        if is_dev:
            reload_func = autoreload.run_with_reloader
            reload_func(self.run, *args, **options)
        else:
            return self.run(*args, **options)
