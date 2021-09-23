from mock import MagicMock
import unittest
import sys
import sqlite3 as sqlite
import os.path

sys.path.append("..")
sys.path.append(".")

from clues_sim import main

class TestCLUES(unittest.TestCase):
    def __init__(self, *args):
        """Init test class."""
        unittest.TestCase.__init__(self, *args)

    def test_clues(self):
        tests_path = os.path.dirname(os.path.abspath(__file__))
        sys.argv = ["test_clues.py", "-s"]
        options = MagicMock() 
        options.SIM_FILE = os.path.join(tests_path, '../etc/simulator/simple.sim')
        options.OUT_FILE = "/tmp/cluessim.db"
        options.TRUNCATE = True
        options.RT_MODE = False
        options.RANDOM_SEED = 10402
        options.FORCETRUNCATE = True
        options.END = True

        main(options)

        con = sqlite.connect(options.OUT_FILE)
        cur = con.cursor()
        cur.execute('SELECT * FROM requests')
        rows = cur.fetchall()
        con.close()
        if sys.version_info.major == 3:
            res = [('2', 12, 25, 2, 1, 1024, '[""]', 1, 1, 'null', '[]', 1)]
        else:
            res = [('1', 12, 12, 0, 1, 1024, '[""]', 1, 1, 'null', '[]', 1)]

        self.assertEqual(rows, res)


if __name__ == "__main__":
    unittest.main()