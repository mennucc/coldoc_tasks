ColDoc Task server
==================

This package implements a simple task server,
that can run jobs in background.

It is used in the
[ColDoc project](https://mennucc.github.io/ColDoc_project).

It is a simpler alternative to fancier solutions, such as
[Celery](https://docs.celeryq.dev/en/stable/).

At the same time, this package contains a wrapper, so that
the code using this package can be switched to using *Celery* if later needed.
(In a sense, this package can be seen as a gentle way of using  *Celery*)

The code can be easily integrated in a Django portal.

Code
====

The code is open source,
[it is available at GitHub.](https://github.com/mennucc/coldoc_tasks)

The current version of the code is `0.1`.

Authors
-------

This software is Copyright 2023-25
[Andrea C. G. Mennucci](https://www.sns.it/it/persona/andrea-carlo-giuseppe-mennucci)

License
-------

See file `LICENSE.txt` in the code distribution

Tests
-----

The code is tested
[using  *GitHub actions*](https://github.com/mennucc/coldoc_tasks/actions/workflows/test.yaml)
inside an Ubuntu environment, for Python 3.8 up to 3.11

![Test results](https://github.com/mennucc/coldoc_tasks/actions/workflows/test.yaml/badge.svg)


Usage
=====

Dependencies
------------

when used with its internal server, the package has no strict dependencies.
The packages `lockfile` and `psutil`, if installed, will be used to enhance some parts.

If you wish to use *Celery* servers, then you need to install the celery package.

Starting the server
-------------------

The command to start the server has synopsis

    coldoc_tasks_cli coldoc start infofile socket [authkey]

for example

    coldoc_tasks_cli coldoc start /tmp/info1 /tmp/sock1

To start a celery server, you can use

    coldoc_tasks_cli celery daemon celeryconfig.py 

There is an example *etc/celeryconfig.py* in source code of
the package, that uses the *redis* server.
In Ubuntu/Debian machines the redis server can be installed by

    sudo apt install redis

moreover, you need the *celery* python package, that can be installed by

    pip install 'celery[redis]'

For Django projects, there is a provision for autostarting the server,
read below.

Scheduling tasks
----------------

A task is scheduled using the

    fork_class

you need an instance,

    f=fork_class()

then you schedule the the command using

    f.run(cmd, ....)

with the function in `cmd` and all its arguments following; then you call

    ret=f.wait(timeout=None)

to wait for the result; if the *cmd* raised an exception, `ret=f.wait()` will raise the same exception;
if `timeout` occours, `ColdocTasksTimeoutError` is raised; if the subprocess has disappeared,
`ColdocTasksProcessLookupError` is raised.


Task servers
------------

There are four implementations of fork classes:

- `celery` uses the Celery server
- `coldoc` uses the server implemented in this package
- `simple` uses `os.fork`
- `nofork` does not fork

The above name can be retrieved also from `fork_class.fork_type`.

At any time,

    fork_class = coldoc_tasks.task_utils.choose_best_fork_class(infofile=None, celeryconfig=None, preferences=('celery','coldoc','simple'))

will return the first working class in the list `preferences` (or the *nofork* class, if none are found);
where `infofile` is the path to the info file (for the internal server), and
`celeryconfig` is the path to the Celery `celeryconfig.py` file (that was `etc/celeryconfig.py`  in the above example)

Queue
-----

You can use use

    f=fork_class(queue=myqueue)

to queue a job in the queue named `myqueue` ( `True` being the default queue);
note that this is currently meaningful only for the `coldoc` type of `fork_class`.

Further commands
----------------

The method

    f.terminate()

will terminate a running job (this is not available in the `nofork` class).

Each `fork_class` accepts the parameter `use_fork=False` ; this is useful to simplify
the code flow, when forking is not needed.

    def cmd(arg):
        pass # do something useful
    # list of arguments
    a = list([1,3,4,5,6,99])
    # list of jobs
    ff = list(range(len(a))
    # schedule
    for j in a:
        # if there is only one job, no need to fork
        ff[j] = fork_class(use_fork = (len(a) > 1 ))
        ff[j].run(cmd, j)
    # get results
    for j in a:
        ff[j.wait()


Django
======

settings
--------

Integration with Django is thru these variables in the _setting.py_ file

- `COLDOC_TASKS_INFOFILE`  
   the path to the file where the information about the task server are stored.
   If absent, a temporary file is used.

- `COLDOC_TASKS_SOCKET`  
  the path of the socket (used to communicate with the server);
  (used when the server is autostarted).
  If absent, a temporary file is used.

- `COLDOC_TASKS_PASSWORD`  
  the password used to secure communications (used when the server is autostarted);
  if absent, a random password will be generated.

- `COLDOC_TASKS_CELERYCONFIG`  
  if you use Celery, you must tell where the `celeryconfig.py` file is.

- `COLDOC_TASKS_AUTOSTART`  
  comma separated list of servers to autostart, choosing between
  `celery` and `coldoc`; note that this is not enough,  read below.

- `COLDOC_TASKS_LOGFILE`  
  optional path for a logfile for the autostarted server.

- `COLDOC_TASKS_CELERY_LOGFILE`  
  optional path for a logfile for the autostarted Celery server.

- `COLDOC_TASKS_COLDOC_LOGFILE`  
  optional path for a logfile for the autostarted coldoc task server.

- `COLDOC_TASKS_PYTHONPATH`  
  Store here the base path where your django code resides; usually it is ennough to set it to  
  `COLDOC_TASKS_PYTHONPATH = BASE_DIR`  
  in a standard `settings.py`. Without this, the server will not find your code.



starting the server
-------------------

The command to start the server has synopsis

    coldoc_tasks_cli coldoc django_start

getting this to work properly requires that `PYTHONPATH`
be set to the root of the Django code.

autostarting
------------

You need to flag  any code path where the server should be autostarted,
using an environment variable.
This is to avoid unnnecessary (and recursive) server autostarting.

To this end, you should add

    os.environ['COLDOC_TASKS_AUTOSTART'] = 'all'

in certain files, such as  `wsgi.py`.

In `manage.py` you may add (in the `main` function)

    if (len(sys.argv)>1 and sys.argv[1] in ('runserver',)):
      os.environ['COLDOC_TASKS_AUTOSTART'] = 'all'

so that the *task server* is only started when `manage.py` is starting the Django portal.

Instead of `all`, you can specify a restricted set of servers, with the same syntax as the
setting `COLDOC_TASKS_AUTOSTART`.

A server will be started only if

- it is in the list specified in `settings.COLDOC_TASKS_AUTOSTART` , and

- it is in the list specified in the env variable `COLDOC_TASKS_AUTOSTART` , and

- it cannot be pinged.

examples
--------

To hook Django to an already running server, just set

    COLDOC_TASKS_INFOFILE="/path/to/infofile"

To start a server, set

    COLDOC_TASKS_PYTHONPATH=BASE_DIR
    COLDOC_TASKS_AUTOSTART="coldoc"

and set the environment variable as explained above
(if password is not set, a random one will be generated).

If you add

    COLDOC_TASKS_INFOFILE="/path/to/infofile"
    COLDOC_TASKS_SOCKET="/path/to/socket"

then you can control where those elements will be saved,
 otherwise they will go in temporary directory.
Note that the *infofile* will be overwritten at each restart
(to include extra informations, such as the server process pid)

To hook Django to an already running Celery server, just set

    COLDOC_TASKS_CELERYCONFIG="/path/to/celeryconfig.py"

To start a server, set

    COLDOC_TASKS_CELERYCONFIG="/path/to/celeryconfig.py"
    COLDOC_TASKS_PYTHONPATH=BASE_DIR
    COLDOC_TASKS_AUTOSTART="celery"

Note that the *celeryconfig* is never overwritten.

You can also autostart both kind of server, using

    COLDOC_TASKS_AUTOSTART="celery,coldoc"

Developing
==========

If you wish to help in developing, please

    pip -r requirements-test.txt
    git config --local core.hooksPath .githooks/

so that each commit is pre tested.


Acknowledgements
================

The principal author has used the Python code editor "wing" by
[Wingware](http://wingware.com/)
to develop this project.
