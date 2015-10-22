# coding: utf-8

import csv
from pybabe import Babe
from .. import TestCase


class TestCSV(TestCase):

    def test_csv_read_write(self):
        s = """foo\tbar\tf\td
1\t2\t3.2\t2010/10/02
3\t4\t1.2\t2011/02/02
"""
        babe = Babe()
        b = babe.pull(string=s, format='csv', name='Test', delimiter='\t')
        b.push(filename='tests/files/test2.csv', delimiter='\t')
        with open('tests/files/test2.csv') as f:
            self.assertEquals(f.read(), s)

    def test_csv_read_write_2_default_delimiter_to_string_bug(self):
        s = """foo,bar,f,d
1,2,3.2,2010/10/02
3,4,1.2,2011/02/02
"""
        babe = Babe()
        b = babe.pull(string=s, format='csv', name='Test')
        b.push(filename='tests/files/test4.csv')
        with open('tests/files/test4.csv') as f:
            self.assertEquals(f.read(), s)

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
        b.push(filename='tests/files/test3.csv', dialect=Dialect)
