import os
from django.apps import AppConfig

from django.conf import settings

class ColDocTasksAppConfig(AppConfig):
    name = 'coldoc_tasks'
    version_info = '0.2'
    verbose_name = 'ColDoc Tasks ({})'.format(version_info)

    def ready(self):
        pass
        ## this is too buggy...
        #print(' tasks app ready')
        #opt = os.environ.get('COLDOC_TASKS_AUTOSTART_OPTIONS','').split(',')
        #if getattr(settings, 'COLDOC_TASKS_AUTOSTART', True) and 'autostart' in opt:
            #import coldoc_tasks.coldoc_tasks
            #print('coldoc tasks will autostart now')
            #coldoc_tasks.coldoc_tasks.tasks_server_django_autostart(settings)
