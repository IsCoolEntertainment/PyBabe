
from base import MetaInfo, BabeBase
import csv

def log(stream, logfile):
    if isinstance(logfile, basestring):
        logstream = open(logfile, 'wb')
        do_close = True
    else:
        logstream = logfile
        do_close = False
    for row in stream:
        if isinstance(row, MetaInfo):
            writer = csv.writer(logstream, row.dialect)
            writer.writerow(row.names)
        else:
            writer.writerow(list(row))
        yield row
    if do_close:
       logstream.close()
       
BabeBase.register("log", log)