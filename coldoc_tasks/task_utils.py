import sys, os, time, multiprocessing, subprocess, pickle
from os.path import join as osjoin


import logging
logger = logging.getLogger(__name__)


####


####





######################################



def __fork_reentrat_test(fork_class, depth = 3,  sleep=0.1):
    assert isinstance(depth,int) and depth >= 0
    f = fork_class()
    if depth > 0:
        logger.info(' reentring at depth %d',depth)
        f.run(__fork_reentrat_test, fork_class, depth -1, sleep)
    elif sleep > 0:
        logger.info(' sleeping at depth 0 for %g', sleep)
        f.run(time.sleep, sleep)
    else:
        logger.info(' raising ValueError at depth 0')
        raise ValueError('Emitted ValueError to test')
    ret = f.wait(timeout=(2. * sleep + 3.))
    return 'happy' if (sleep > 0 and depth == 0) else ret

############################

def test_fork(fork_class):
    ret = 0
    print("==== test : return 3.14")
    f = fork_class()
    f.run(str,3.14)
    try:
        s = pickle.dumps(f)
        f = pickle.loads(s)
    except Exception as E:
        print('** cannot pickle : %r' % E)
        ret += 1
    r = f.wait()
    print('Returned %r ' % (r,))
    if r != '3.14':
        ret += 1
    #
    if 1:
        print("==== test : subprocess raises exception")
        f = fork_class()
        f.run(eval,'0/0')
        try:
            r = f.wait()
        except  ZeroDivisionError:
            print('caught')
        else:
            print('WRONG: Returned  %r' % (r,))
            ret += 1
    #
    if ret:
        print(' ** skipping reentrant test, already errors')
    else:
        N = 2
        D = 4
        print("======= test : check against self locking, instances = %d depth = %d" %(N,D))
        print("== scheduling")
        ff = list(range(N))
        for j in range(N):
            ff[j] = fork_class()
            ff[j].run(__fork_reentrat_test, fork_class, D,  sleep = (0.2 if j else -1))
        print("== waiting")
        for j in range(N):
            r = None
            try:
                r = ff[j].wait(timeout = 0.3)
            except ValueError as E:
                if not j:
                    print(' Caught %r, as expected ' % E)
                else:
                    print('Failure %r' % (E,))
                    ret += 1
            except Exception as E:
                ret += 1
                print('Failure %r' % (E,))
            else:
                if r != 'happy' :
                    print('Wrong return value %r' % (r,))
    #
    if ret:
        print(' ** skipping speed test, already errors')
    else:
        N = 64
        t = time.time()
        print("======= speed test instances = %d " %(N,))
        print("== scheduling")
        ff = list(range(N))
        for j in range(N):
            sys.stderr.write('\r %d  \r' % (j,))
            ff[j] = fork_class()
            ff[j].run(int,'4')
        print("== scheduling , time per instance %g sec" % ((time.time() - t) / N))
        print("== waiting")
        for j in range(N):
            sys.stderr.write('\r %d  \r' % (j,))
            r = ff[j].wait()
            if r != 4:
                ret += 1
        t = time.time() - t
        print("======= speed test, time per instance %g sec " %(t / N,))
    #
    if f.use_fork and hasattr(fork_class,'terminate'):
        print("==== test : terminate subprocess")
        f = fork_class()
        f.run(time.sleep,2)
        f.terminate()
        r = None
        try:
            r = f.wait()
        except RuntimeError as R:
            print('As expected, raised: %r ' % (R,))
        else:
            print('WRONG: Returned  %r' % (r,))
            ret += 1
    if ret:
        print('=== some tests failed')
    else:
        print('=== all tests successful')
    return ret

