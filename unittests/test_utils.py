#!/usr/bin/env -S -- python3 -X tracemalloc

"""
run this as
$ python3 unittests/test_fork.py
or
$ python3 -m unittests unittests/test_fork.py
or
$ pytest-3 unittests/test_fork.py 
"""

import os, sys, io, unittest, tempfile, shutil, time, io
import functools, tempfile, threading, multiprocessing, logging, signal
from os.path import join as osjoin

import logging
logger = logging.getLogger(__name__)

testdir = os.path.dirname(os.path.realpath(__file__))
sourcedir = os.path.dirname(testdir)

if __name__ == '__main__':
    if sourcedir not in sys.path:
        sys.path.insert(0, sourcedir)

import coldoc_tasks.task_utils as TU
import plain_config as SC

example_db = {
    'into': 123456,
    'floato' : 3.141,
    'stringo' : 'hello',
    'texto' : 'hi\n how are you\n',
    'byteo' : b'there',
    'bolleo1' : True,
    'nonno' : None,
}


exampleconfig = """
into/i=123456
nonne/r=None
# comment

floato/f=3.141
stringo=hello
texto/64s=aGkKIGhvdyBhcmUgeW91Cg==
byteo/32=ORUGK4TF
 
# final comment
"""

class TestUtils(unittest.TestCase):
    def test_config_wr(self):
        t = tempfile.NamedTemporaryFile(prefix='testo_')
        n = t.name
        SC.write_config(n, example_db)
        ndb, sdb = SC.read_config(n)
        self.assertEqual(example_db, ndb)

    def test_config_wr2(self):
        out = io.StringIO()
        SC.write_config(out, example_db)
        out.seek(0)
        ndb, sdb = SC.read_config(out)
        #print(out.getvalue())
        self.assertEqual(example_db, ndb)


    def test_config_rw(self):
        ndb, sdb = SC.read_config(io.StringIO(exampleconfig))
        #print(repr(ndb['texto']))
        out = io.StringIO()
        SC.write_config(out, ndb, sdb)
        out = out.getvalue()
        self.assertEqual(exampleconfig, out)


def _dummy_func_sleep_short():
    """Helper function for multiprocessing tests (must be at module level for pickling)"""
    time.sleep(0.1)

def _dummy_func_sleep_long():
    """Helper function for multiprocessing tests (must be at module level for pickling)"""
    time.sleep(0.2)

def _dummy_func_return_42():
    """Helper function for multiprocessing tests (must be at module level for pickling)"""
    time.sleep(0.1)
    return 42


class TestProcJoin(unittest.TestCase):
    """Test the proc_join function with different input types"""

    @classmethod
    def setUpClass(cls):
        """Set up multiprocessing to use 'fork' for these tests"""
        # Python 3.14 uses 'forkserver' by default, but we need 'fork' for these tests
        # Save the current context
        cls._original_start_method = multiprocessing.get_start_method(allow_none=True)
        try:
            multiprocessing.set_start_method('fork', force=True)
        except RuntimeError:
            pass  # Already set

    @classmethod
    def tearDownClass(cls):
        """Restore original multiprocessing start method"""
        if cls._original_start_method:
            try:
                multiprocessing.set_start_method(cls._original_start_method, force=True)
            except RuntimeError:
                pass

    def test_proc_join_with_process(self):
        """Test proc_join with a multiprocessing.Process object"""
        proc = multiprocessing.Process(target=_dummy_func_sleep_short)
        proc.start()
        # Wait a bit to ensure process starts
        time.sleep(0.05)
        # This should work - proc has a join method
        TU.proc_join(proc)
        self.assertFalse(proc.is_alive())

    def test_proc_join_with_process_method(self):
        """Test proc_join with proc.join (the method object)"""
        proc = multiprocessing.Process(target=_dummy_func_sleep_short)
        proc.start()
        # Wait a bit to ensure process starts
        time.sleep(0.05)

        # Capture log output to see what happens
        with self.assertLogs(TU.logger, level='WARNING') as cm:
            # Pass the method object instead of the process
            TU.proc_join(proc.join)

        # Should have logged a warning about not knowing how to wait
        self.assertTrue(any("Don't know how to wait" in msg for msg in cm.output))

        # Process should still be alive since proc_join didn't actually wait
        self.assertTrue(proc.is_alive())

        # Clean up by actually joining it
        proc.join()
        self.assertFalse(proc.is_alive())

    def test_proc_join_with_pool_result(self):
        """Test proc_join with a pool.ApplyResult object"""
        pool = multiprocessing.Pool(1)
        result = pool.apply_async(_dummy_func_return_42)

        # This should work - result has a wait method
        TU.proc_join(result)

        # Result should be ready
        self.assertTrue(result.ready())
        self.assertEqual(result.get(), 42)

        pool.close()
        pool.join()

    @unittest.skip("TODO: proc_join with PID needs investigation - WNOHANG behavior unclear")
    def test_proc_join_with_pid(self):
        """Test proc_join with a PID (integer)"""
        proc = multiprocessing.Process(target=_dummy_func_sleep_short)
        proc.start()
        pid = proc.pid

        # Wait for process to complete
        time.sleep(0.15)

        # This should work - proc_join handles integers as PIDs
        # Note: proc_join uses WNOHANG, so it does non-blocking waits
        TU.proc_join(pid)

        # Process should be done after the sleep
        self.assertFalse(proc.is_alive())

    def test_proc_join_with_bool(self):
        """Test proc_join with a boolean (should just warn)"""
        with self.assertLogs(TU.logger, level='WARNING') as cm:
            TU.proc_join(True)

        # Should have logged a warning about not knowing the process id
        self.assertTrue(any("Don't know the true process id" in msg for msg in cm.output))

if __name__ == '__main__':
    #
    unittest.main()
