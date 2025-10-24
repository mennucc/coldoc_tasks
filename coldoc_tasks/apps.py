import os

import logging
logger = logging.getLogger(__name__)

from django.apps import AppConfig

from django.conf import settings

def autostart(sett_):
    ' sett_ is usually the Django setting'
    from coldoc_tasks.task_utils import all_fork_classes
    realopt = os.environ.get('COLDOC_TASKS_AUTOSTART')
    if realopt is None:
        # safeguard against unwanted (sometimes recursive) activation
        logger.info('no COLDOC_TASKS_AUTOSTART environ, not starting any server')
        return
    opt = realopt.split(',')
    if 'all' in opt:
        opt = all_fork_classes
    elif set(opt).difference(all_fork_classes):
        logger.error('COLDOC_TASKS_AUTOSTART environ %r contains keywords not in %r',
                  opt,all_fork_classes)
    if not hasattr(sett_, 'COLDOC_TASKS_AUTOSTART'):
        logger.info('no COLDOC_TASKS_AUTOSTART setting, not starting any server')
        return
    autostart = getattr(sett_, 'COLDOC_TASKS_AUTOSTART','')
    autostart=autostart.split(',')
    if set(autostart).difference(all_fork_classes):
        logger.error('setting COLDOC_TASKS_AUTOSTART %r contains keywords not in %r',
                  autostart,all_fork_classes)
    #
    generic_logfile = getattr(sett_, 'COLDOC_TASKS_LOGFILE', None)
    pythonpath = getattr(sett_, 'COLDOC_TASKS_PYTHONPATH', tuple())
    celeryconfig = getattr(sett_, 'COLDOC_TASKS_CELERYCONFIG', None)
    for j in autostart:
        if j == 'celery' and j in opt:
            logfile = getattr(sett_, 'COLDOC_TASKS_CELERY_LOGFILE', generic_logfile)
            if celeryconfig is None:
                logger.error('Coldoc Tasks app, cannot start Celery daemon, COLDOC_TASKS_CELERYCONFIG is not defined')
                continue
            import coldoc_tasks.celery_tasks
            logger.info('Coldoc Tasks: will autostart the Celery daemon')
            proc = coldoc_tasks.celery_tasks.tasks_daemon_autostart(celeryconfig, pythonpath=pythonpath,
                                                                    logfile=logfile, force=True)
            if not proc:
                logger.error('Coldoc Tasks app, failed starting Celery daemon')
            elif proc is not True:
                sett_.COLDOC_TASKS_AUTOSTART_CELERY_PROC = proc
        elif j == 'coldoc'  and j in opt:
            logfile = getattr(sett_, 'COLDOC_TASKS_COLDOC_LOGFILE', generic_logfile)
            import coldoc_tasks.coldoc_tasks
            logger.info('Coldoc Tasks: will autostart the daemon')
            proc, info = coldoc_tasks.coldoc_tasks.tasks_daemon_django_autostart(settings, pythonpath=pythonpath,
                                                                                 logfile=logfile, force=True)
            if not proc:
                logger.error('Coldoc Tasks app, failed starting of daemon')
            else:
                sett_.COLDOC_TASKS_AUTOSTART_COLDOC_PROC = proc
                sett_.COLDOC_TASKS_AUTOSTART_INFOFILE = info
        else:
            logger.error('COLDOC_TASKS_AUTOSTART: settings %r requested to start %r, but environ is %r : skipped',
                         autostart, j, opt)

class ColDocTasksAppConfig(AppConfig):
    name = 'coldoc_tasks'
    version_info = '0.2'
    verbose_name = 'ColDoc Tasks ({})'.format(version_info)

    def ready(self):
        logger.info('Coldoc Tasks app ready')
        autostart(settings)
