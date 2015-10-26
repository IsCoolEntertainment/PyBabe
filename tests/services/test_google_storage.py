# coding: utf-8

from .. import TestCase, can_connect_to_the_net, skipUnless
from pybabe import Babe


class TestGS(TestCase):

    @skipUnless(can_connect_to_the_net(), 'Requires net connection')
    def test_s3(self):
        s = "a,b\n1,2\n3,4\n"
        a = Babe().pull(string=s,
                        format='csv',
                        name='Test')
        a.push(filename='test_gs.csv',
               bucket='bertrandtest',
               delimiter="\t",
               protocol="gs")
        # b = Babe().pull(filename='test_gs.csv',
        #                 name='Test',
        #                 bucket='bertrandtest',
        #                 protocol="gs")
        # b.push(filename='tests/files/test_gs.csv',
        #        delimiter='\t')
        # self.assertEquals(b.to_string(), s)
