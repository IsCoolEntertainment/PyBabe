# coding: utf-8

from pybabe import Babe
from .. import TestCase


class TestExcel(TestCase):

    def test_excel_read_write(self):
        babe = Babe()
        b = babe.pull(filename='tests/files/test.xlsx', name='Test2').typedetect()
        b = b.mapTo(lambda row: row._replace(Foo=-row.Foo))
        b.push(filename='tests/files/test2.xlsx')
