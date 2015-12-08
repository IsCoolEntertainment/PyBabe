# coding: utf-8

from .. import TestCase, skipUnless, can_connect_to_the_net
from pybabe import Babe


class TestS3(TestCase):

    @skipUnless(can_connect_to_the_net(), 'Requires net connection')
    def test_s3(self):
        s = "a,b\n1,a\n3,b\n"
        filename = 'tests/test_bq.csv'
        a = Babe().pull(string=s,
                        format='csv',
                        name='Test')

        a.push(filename=filename,
               format='csv',
               delimiter='\t',
               quotechar='|',
               encoding='utf8',
               bucket='bertrandtest',
               protocol='gs')

        b = Babe()

        b.push_bigquery(filename=filename,
                        bucket='bertrandtest',
                        project_id='bigquery-testing-1098',
                        dataset_id='ladata',
                        table_name='tests',
                        schema=[
                            {
                                "name": "entier",
                                "type": "INTEGER",
                                "mode": "REQUIRED"
                            },
                            {
                                "name": "string",
                                "type": "STRING",
                                "mode": "REQUIRED"
                            }
                        ])
