
from pybabe import Babe
import unittest
from stubserver import FTPStubServer
import random

class TestBasicFunction(unittest.TestCase):
    def test_pull_push(self):
        babe = Babe()
        a = babe.pull('tests/test.csv', name='Test').typedetect()
        a = a.map('foo', lambda x : -x).multimap({'bar' : lambda x : x + 1, 'f' : lambda f : f / 2 }).sort('foo')
        a = a.groupkey('foo', int.__add__, 0, keepOriginal=True)
        a.push(filename='tests/test2.csv')
        
    def test_keynormalize(self):
        babe = Babe()
        self.assertEqual('Payant_Gratuit', babe.keynormalize('Payant/Gratuit'))
    
    def test_pull_process(self):
        babe = Babe()
        a = babe.pull_command(['/bin/ls', '-1', '.'], 'ls', ['filename'])
        a.push(filename='tests/ls.csv')
        
class TestZip(unittest.TestCase):
    def test_zip(self):
        babe = Babe()
        a = babe.pull('tests/test.csv', name='Test')
        a.push(filename='test.csv', compress='tests/test.zip')
        
class TestFTP(unittest.TestCase):
    def setUp(self):
        self.port = random.choice(range(9000,11000))
        self.server = FTPStubServer(self.port)
        self.server.run()
        
    def tearDown(self):
        self.server.stop()
    
    def test_ftp(self):
        babe = Babe()
        a = babe.pull('tests/test.csv', name='Test')
        a.push(filename='test.csv', protocol='ftp', host='localhost', port=self.port)

    def test_ftpzip(self):
        babe = Babe()
        a = babe.pull('tests/test.csv', name='Test')
        a.push(filename='test.csv', compress='test.zip', protocol='ftp', host='localhost', port=self.port)
        
class TestCharset(unittest.TestCase):
    def test_writeutf16(self):
        babe = Babe()
        a = babe.pull('tests/test.csv', name='Test')
        a.push(filename='tests/test_utf16.csv', encoding='utf_16')
    
class TestExcel(unittest.TestCase):
    
    def test_excel_read_write(self):
        babe = Babe()
        b = babe.pull('tests/test.xlsx', name='Test2').typedetect()
        b = b.map('Foo', lambda x : -x)
        b.push(filename='tests/test2.xlsx')

if __name__ == "__main__":
    unittest.main()