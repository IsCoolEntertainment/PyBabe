"""
Need a correct pybabe config file :
~/.pybabe.cfg
And the GOOGLE_APPLICATION_CREDENTIALS environment variable set

"""

from tests_utils import TestCase, can_connect_to_the_net, skipUnless
import csv
import string
import base64
from pybabe import Babe
from datetime import (
    datetime,
    timedelta
)
import time
import logging

v1 = [
    'date', 'hour', 'time',
    # event  name.  VARCHAR(63)
    #   Customer event or:
    #   ucc_new_install, ucc_old_install,
    #   or [event_type]
    #   or gc1, gc2, gc3, gc4
    'name',
    # user id  that performs the action BIGINT
    #   also 'r' for  responses
    'uid',
    # event subtyping  VARCHAR(63)
    # for pgr :
    #   st1 = parsed referer
    #   st2 = source_ip_country
    #   st3 = http or https
    # for cpu
    #   st1 = gender
    #   st2 = local country
    #   st3 = local state (us state)
    'st1', 'st2', 'st3',
    # channel type or transaction type VARCHAR(63)
    #   for pgr : fxb_ref or fx_type
    'channel_type',
    # value associated to event (or revenue)  INTEGER
    # for cpu :
    #   v = number of friends
    # for gc1, ...
    #   value for the goal
    'value',
    # user level associated to event          INTEGER
    # for cpu
    #   l = age
    'level',
    # list of recipients uid, comma separated (ins,nes) VARCHAR(1023)
    'recipients',
    # unique tracking tag ( also match su : short tracking tag) VARCHAR(63)
    'tracking_tag',
    # JSON Payload + additional query parameters not processed VARCHAR(255) Base64 Decoded
    'decoded_data',
    # JSON Payload + additional query parameters not processed VARCHAR(255),
    # 'data'
]


def sanitize(text):
    tbl = string.maketrans('\t\f\n', '   ')
    return string.translate(text, tbl, '\r')


def decode_data(row):
    if row.data:
        if ',' in row.data:
            decoded = row.data
        else:
            try:
                decoded = base64.b64decode(row.data, '-_')
            except TypeError as error:
                logging.warn('skip base64 with {!r}: {}'.format(row.data, error))  # noqa
                return 'invalid base64 data'
            decoded = sanitize(decoded)
        return decoded
    return row.data


def uid_type_check(row):
    try:
        # If UID is not empty, it should be an integer
        if row.uid:
            int(row.uid)
        return True
    except Exception:
        logging.info("[IGNORED] Invalid UID in row : %s " % (repr(row)))
        return False


class TestBigQuery(TestCase):

    @skipUnless(can_connect_to_the_net(), 'Requires net connection')
    def test_gs(self):
        s = "a,b\n1,2\n3,4\n"
        a = Babe().pull(string=s, format='csv', name='Test')
        a.push(filename='test_gs.csv', bucket='bertrandtest', protocol="gs")
        b = Babe().pull(filename='test_gs.csv',
                        name='Test', bucket='bertrandtest', protocol="gs")
        b.push(filename='tests/test_gs.csv', delimiter='\t')
        self.assertEquals(b.to_string(), s)

    @skipUnless(can_connect_to_the_net(), 'Requires net connection')
    def test_gs_load_from_kontagent(self):
        # export 1 full day
        game = 'crazy'
        day = '20151010'
        hour = '03'
        table_name = '{}_{}'.format(game, day)
        filename = '{}.csv'.format(table_name + hour)
        result = time.strptime(day + ' ' + hour, '%Y%m%d %H')
        start_time = datetime(result.tm_year,
                              result.tm_mon,
                              result.tm_mday,
                              result.tm_hour)
        end_time = start_time + timedelta(hours=1)

        class Dialect(csv.Dialect):
            lineterminator = '\n'
            delimiter = '\t'
            doublequote = False
            escapechar = '\\'
            quoting = csv.QUOTE_MINIMAL
            quotechar = '|'

        a = Babe()
        a = a.pull_kontagent(start_time=start_time,
                             sample_mode=False,
                             end_time=end_time,
                             KT_APPID='869fb4a24faa4c61b702ea137cbe16ad',
                             discard_names=["PointSend"])
        a = a.mapTo(decode_data, insert_fields=["decoded_data"])
        a = a.filterColumns(keep_fields=v1)
        a = a.filter(lambda row: uid_type_check(row) is True)
        a.push(filename=filename,
               format='csv',
               dialect=Dialect,
               encoding='utf8',
               bucket='bertrandtest',
               protocol='gs')

        a.push_bigquery(filename=filename,
                        project_id='bigquery-testing-1098',
                        dataset_id='ladata',
                        table_name=table_name,
                        schema=[
                            {
                                "name": "date",
                                "type": "STRING",
                                "mode": "REQUIRED"
                            },
                            {
                                "name": "hour",
                                "type": "INTEGER",
                                "mode": "REQUIRED"
                            },
                            {
                                "name": "time",
                                "type": "TIMESTAMP",
                                "mode": "REQUIRED"
                            },
                            {
                                "name": "name",
                                "type": "STRING",
                                "mode": "REQUIRED"
                            },
                            {
                                "name": "uid",
                                "type": "INTEGER"
                            },
                            {
                                "name": "st1",
                                "type": "STRING"
                            },
                            {
                                "name": "st2",
                                "type": "STRING"
                            },
                            {
                                "name": "st3",
                                "type": "STRING"
                            },
                            {
                                "name": "channel_type",
                                "type": "STRING"
                            },
                            {
                                "name": "value",
                                "type": "INTEGER"
                            },
                            {
                                "name": "level",
                                "type": "INTEGER"
                            },
                            {
                                "name": "recipients",
                                "type": "STRING"
                            },
                            {
                                "name": "tracking_data",
                                "type": "STRING"
                            },
                            {
                                "name": "data",
                                "type": "STRING"
                            }
                        ],
                        job_id='{}_{}'.format(start_time, end_time),
                        num_retries=5)

    @skipUnless(can_connect_to_the_net(), 'Requires net connection')
    def test_check_content_big_query(self):
        dataset_id = 'ladata'
        day = '20151010'
        table_name = 'crazy_{}'.format(day)
        hourfield = 'hour'
        query = """
SELECT
    {}
FROM
    [{}.{}]
GROUP BY 1
HAVING count(1) > 0
ORDER BY 1 ;""".format(hourfield, dataset_id, table_name)
        a = Babe().pull_bigquery(project_id='bigquery-testing-1098',
                                 query=query,
                                 timeout=1000,
                                 num_retries=2)
        l = map(lambda row: int(getattr(row, hourfield)), a.to_list())
        all_hours = set(xrange(0, 24))

        if len(l) != 24:
            for h in all_hours.difference(set(l)):
                print("Missing hour: %s %s " % (table_name, h))

    @skipUnless(can_connect_to_the_net(), 'Requires net connection')
    def test_pull_bigquery(self):
        dataset_id = 'ladata'
        day = '20151010'
        table_name = 'crazy_{}'.format(day)
        query = """
SELECT
    uid,
    count(1)
FROM
    [{}.{}]
WHERE
    name='pgr'
GROUP BY 1
ORDER BY 2 DESC;""".format(dataset_id, table_name)

        a = Babe().pull_bigquery(project_id='bigquery-testing-1098',
                                 query=query,
                                 timeout=1000,
                                 num_retries=2)

        print a.to_string()
