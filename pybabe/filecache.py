# coding: utf-8
from __future__ import print_function
from base import BabeBase
import os
from os.path import join, getsize, getctime
import datetime
import time
import csv


class FileCache(object):
    "Manage a file-based cache"

    def __init__(self, **kwargs):
        self.size_limit = kwargs.get('size_limit',5<<30) #1<<20 (1MB), 1<<30 (1GB)
        self.cache_directories = []
        cache = BabeBase.get_config("s3", "cache", default=False)
        if cache:
            default_cache_dir = "/tmp/pybabe-s3-cache-%s" % os.getenv('USER')
            cache_dir = BabeBase.get_config("s3", "cache_dir", default=default_cache_dir)
            self.cache_directories.append(cache_dir)
        self.cache_directories.append(BabeBase.get_config_with_env(section='kontagent', key='KT_FILECACHE', default='/tmp/kontagent-cache'))

    def cleanup(self, debug=False):
        "Apply a global cleanup to the cache, trimming it to size_limit by removing oldest files first"
        files = []
        global_size = 0
        for cache_dir in self.cache_directories:
            if os.path.exists(cache_dir):
                for (root, dir_names, file_names) in os.walk(cache_dir):
                    for name in file_names:
                        filepath = join(root, name)
                        file_size = getsize(filepath)
                        created_date = getctime(filepath)
                        files.append({'filepath':filepath, 'size':file_size, 'created_date':created_date})
                        global_size += file_size

        if global_size > self.size_limit:
            sortedlist = sorted(files , key=lambda elem: int(elem['created_date']))
            with open('/tmp/filecache_cleanup.log', 'wb') as csvfile:
                spamwriter = csv.writer(csvfile, delimiter=' ',
                                        quotechar='|', quoting=csv.QUOTE_MINIMAL)
                spamwriter.writerow(['global_size = ' + str(global_size)])
                for junk_file in sortedlist:
                    if global_size > self.size_limit:
                        if not debug:
                            # Remove junk file
                            if os.path.exists(junk_file['filepath']):
                                os.remove(junk_file['filepath'])
                                # Try removing parent directories recursively if not empty
                                try:
                                    dir_name = os.path.dirname(os.path.realpath(junk_file['filepath']))
                                    if not dir_name == '/tmp':
                                        os.removedirs(dir_name)
                                except OSError as e:
                                    pass
                            else:
                                print('[{date}] DELETE - File {f} not found'.format(date=str(datetime.datetime.now()), f=junk_file['filepath']))
                        global_size -= junk_file['size']
                        spamwriter.writerow([junk_file['created_date'], junk_file['filepath']])
                    else:
                        break
