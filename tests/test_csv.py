# coding: utf-8

import csv
from pybabe import Babe
from tests_utils import TestCase


class TestCSV(TestCase):

    def test_csv_read_write(self):
        s = """foo\tbar\tf\td
1\t2\t3.2\t2010/10/02
3\t4\t1.2\t2011/02/02
"""
        babe = Babe()
        b = babe.pull(string=s, format='csv', name='Test', delimiter='\t')
        b.push(filename='tests/test2.csv', delimiter='\t')
        with open('tests/test.csv') as f:
            self.assertEquals(f.read(), b.to_string())

    def test_csv_escape(self):
        s = """a\tb\tc
1\tab\t{\\"hello, buzz\\"}
2\tcd\t
"""

        class Dialect(csv.Dialect):
            lineterminator = '\n'
            delimiter = ','
            doublequote = False
            escapechar = '\\'
            quoting = csv.QUOTE_MINIMAL
            quotechar = '|'

        b = Babe()
        b = b.pull(string=s, format='csv', name='Test')
        b.push(filename='tests/test3.csv', dialect=Dialect)
