# coding: utf-8

from pybabe import Babe
from .. import TestCase


class TestPartition(TestCase):
    s = 'date,name,value\n2012-04-04,John,1\n2012-04-04,Luke,2\n2012-04-05,John,1\n'

    def test_partition(self):
        a = Babe().pull(string=self.s, format='csv')
        a = a.partition(field='date')
        d = {}
        a.push(stream_dict=d, format="csv")
        self.assertEquals(d['2012-04-04'].getvalue(), 'date,name,value\n2012-04-04,John,1\n2012-04-04,Luke,2\n')
        self.assertEquals(d['2012-04-05'].getvalue(), 'date,name,value\n2012-04-05,John,1\n')
