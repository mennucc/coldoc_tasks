#!/usr/bin/env python3

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

example_db = {
    'into': 123456,
    'floato' : 3.141,
    'stringo' : 'hello',
    'texto' : 'hi\n how are you\n',
    'byteo' : b'there',
    'typo' : TU.choose_best_fork_class,
}


exampleconfig = """
into/i=123456
# comment

floato/f=3.141
stringo=hello
texto/64s=aGkKIGhvdyBhcmUgeW91Cg==
byteo/32=ORUGK4TF
typo/64p=gASVNgAAAAAAAACMF2NvbGRvY190YXNrcy50YXNrX3V0aWxzlIwWY2hvb3NlX2Jlc3RfZm9ya19jbGFzc5STlC4=
 
# final comment
"""

class TestUtils(unittest.TestCase):
    def test_config_wr(self):
        t = tempfile.NamedTemporaryFile(prefix='testo_')
        n = t.name
        TU.write_config(n, example_db)
        ndb = TU.read_config(n)
        self.assertEqual(example_db, ndb)

    def test_config_wr2(self):
        out = io.StringIO()
        TU.write_config(out, example_db)
        out.seek(0)
        ndb = TU.read_config(out)
        #print(out.getvalue())
        self.assertEqual(example_db, ndb)


    def test_config_rw(self):
        ndb = TU.read_config(io.StringIO(exampleconfig))
        #print(repr(ndb['texto']))
        out = io.StringIO()
        TU.write_config(out, ndb)
        self.assertEqual(exampleconfig, out.getvalue())

if __name__ == '__main__':
    #
    unittest.main()
