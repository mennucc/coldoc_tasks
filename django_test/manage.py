#!/usr/bin/env python
"""Django's command-line utility for administrative tasks."""
import os
import sys
from os.path import dirname, realpath

try:
    import coldoc_tasks
except ImportError:
    coldoc_tasks = None

if coldoc_tasks is None:
    source_dir = dirname(dirname(realpath(__file__)))
    if source_dir not in sys.path:
        sys.stderr.write('(** Cannot import coldoc_tasks, tweaking the sys.path)\n')
        sys.path.insert(0, source_dir)
    else:
        sys.stderr.write('** Cannot import coldoc_tasks!\n')
        sys.exit(1)


def main():
    """Run administrative tasks."""
    #
    if (len(sys.argv)>1 and sys.argv[1] in ('runserver',)):
        os.environ['COLDOC_TASKS_AUTOSTART'] = 'all'
    #
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'django_test.settings')
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == '__main__':
    main()
