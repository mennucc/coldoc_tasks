#!/usr/bin/env python3

import sys, os


if __name__ == '__main__':
    a = os.path.realpath(os.path.dirname(__file__))
    if a in sys.path:
        del sys.path[sys.path.index(a)]
    a = os.path.dirname(a)
    if a not in sys.path:
        sys.path.insert(0,a)

import coldoc_tasks.coldoc_tasks, coldoc_tasks.celery_tasks

def main(argv = sys.argv):
    e = 'Please provide a command family, either `coldoc` or `celery`\n'
    if len( argv) <= 1:
        sys.stderr.write(e)
        return False
    argv = argv[1:]
    if argv[0] == 'coldoc':
        return  coldoc_tasks.coldoc_tasks.main(argv[1:])
    if argv[0] == 'celery':
        return  coldoc_tasks.celery_tasks.main(argv[1:])
    sys.stderr.write(e)
    return False


if __name__ == '__main__':
    ret = main(sys.argv)
    sys.exit( 0 if ret else 1)
