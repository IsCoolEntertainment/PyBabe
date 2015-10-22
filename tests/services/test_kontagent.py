# coding: utf-8

from .. import TestCase
from pybabe import Babe


class TestKontagent(TestCase):
    def test_load(self):
        start_time = '2012-04-23 11:00'
        end_time = '2012-04-23 12:00'
        a = Babe().pull_kontagent(start_time, end_time, sample_mode=True)
        a = a.head(n=10)
        print a.to_string()

    def test_load_partition(self):
        start_time = '2012-04-23 11:00'
        end_time = '2012-04-23 12:00'
        a = Babe().pull_kontagent(start_time, end_time, sample_mode=True)
        a = a.head(n=10)
        d = {}
        a.push(stream_dict=d, format='csv')
        self.assertEquals(list(d.keys()), ['2012-04-23_11'])

    def test_process_line_1(self):
        from pybabe.geoip import get_gic
        from pybabe.kontagent import (
            process_line,
            enumerate_period_per_hour
        )

        start_time = '2012-04-23 11:00'
        end_time = '2012-04-23 12:00'

        gic = get_gic()
        base_date = list(enumerate_period_per_hour(start_time, end_time, 'utc'))[0]
        discard_names = []

        line = ('1563 apa su=85f79f116d829b88b256ec1e00e290bf%2Ca08954fab1ffd6f495bb05cd14ed830d%2'
                'C499E811F-638A-461B-A7B1-AA20BC0CA8CB%2C0041F4F63207473FB9D720F1B7282BBDDEADBEEF&'
                'ts=1437373558&s=8957014&kt_v=iu1.8.2&AdTruthID=0041F4F63207473FB9D720F1B7282BBDDE'
                'ADBEEF&scheme=http 49.181.198.210 "-"')

        v = process_line(gic, base_date, line, discard_names)
        self.assertEquals(v.tracking_tag, '499E811F-638A-461B-A7B1-AA20BC0CA8CB')
        self.assertEquals(v.st3, '0041F4F63207473FB9D720F1B7282BBDDEADBEEF')
        self.assertEquals(v.name, 'apa')
        self.assertEquals(v.uid, '8957014')
        self.assertEquals(v.ip, '49.181.198.210')

    def test_process_line_1_su_without_hyphen(self):
        from pybabe.geoip import get_gic
        from pybabe.kontagent import (
            process_line,
            enumerate_period_per_hour
        )

        start_time = '2012-04-23 11:00'
        end_time = '2012-04-23 12:00'

        gic = get_gic()
        base_date = list(enumerate_period_per_hour(start_time, end_time, 'utc'))[0]
        discard_names = []

        line = ('1563 apa su=85f90bf%2Ca08954fab1dd%2C499E81CA8CB%2C0041F4F6320zeez282BBDDEADBEEF&'
                'ts=1437373558&s=8957014&kt_v=iu1.8.2&AdTruthID=0041F4F63207473FB9D720F1B7282BBDDE'
                'ADBEEF&scheme=http 49.181.198.210 "-"')

        v = process_line(gic, base_date, line, discard_names)
        self.assertEquals(v.tracking_tag,
                          '85f90bf,a08954fab1dd,499E81CA8CB,0041F4F6320zeez282BBDDEADBEEF')
        self.assertEquals(v.st3, '0041F4F63207473FB9D720F1B7282BBDDEADBEEF')
        self.assertEquals(v.name, 'apa')
        self.assertEquals(v.uid, '8957014')
        self.assertEquals(v.ip, '49.181.198.210')

    def test_process_line_1_su_no_commas(self):
        from pybabe.geoip import get_gic
        from pybabe.kontagent import (
            process_line,
            enumerate_period_per_hour
        )

        start_time = '2012-04-23 11:00'
        end_time = '2012-04-23 12:00'

        gic = get_gic()
        base_date = list(enumerate_period_per_hour(start_time, end_time, 'utc'))[0]
        discard_names = []

        line = ('1563 apa su=282BBDDEADBEEF&'
                'ts=1437373558&s=8957014&kt_v=iu1.8.2&AdTruthID=0041F4F63207473FB9D720F1B7282BBDDE'
                'ADBEEF&scheme=http 49.181.198.210 "-"')

        v = process_line(gic, base_date, line, discard_names)
        self.assertEquals(v.tracking_tag, '282BBDDEADBEEF')
        self.assertEquals(v.st3, '0041F4F63207473FB9D720F1B7282BBDDEADBEEF')
        self.assertEquals(v.name, 'apa')
        self.assertEquals(v.uid, '8957014')
        self.assertEquals(v.ip, '49.181.198.210')
