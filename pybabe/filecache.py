
import os
import time
from base import BabeBase
from pybabe import Babe


class FileCache(object):
    "Manage a file-based cache, with direction"

    def __init__(self, **kwargs):
        self.size_limit = kwargs.get('size_limit',1<<30) #1<<20 (1MB), 1<<30 (1GB)
        self.cache_directories = []
        cache = BabeBase.get_config("s3", "cache", default=False)
        if cache:
            default_cache_dir = "/tmp/pybabe-s3-cache-%s" % os.getenv('USER')
            cache_dir = BabeBase.get_config("s3", "cache_dir", default=default_cache_dir)
            self.cache_directories.append(cache_dir)
            self.cache_directories.append("/tmp/pybabe-s3-cache-None")
        self.cache_directories.append(Babe.get_config_with_env(section='kontagent', key='KT_FILECACHE', default='/tmp/kontagent-cache'))

    def cleanup(self):
        "Apply a global cleanup to the cache, trimming it to size_limit by removing oldest files first"
        if len(self.cache_directories):
            for cache_dir in self.cache_directories:
                if os.path.exists(cache_dir):
                    global_size = 0
                    files = []
                    for (dir, dir_names, file_names) in os.walk(cache_dir):
                        for f in file_names:
                            file_stat = os.stat(cache_dir + '/' + f)
                            file_size = int(file_stat.st_size)
                            last_modified = int(file_stat.st_mtime)
                            files.append({'filename':f, 'size':file_size, 'modified':last_modified})
                            global_size += file_size

                    if global_size > self.size_limit and len(files):
                        deleted_size = 0
                        sortedlist = sorted(files , key=lambda elem: elem['modified'])
                        for old_file in files:
                            if global_size >= self.size_limit:
                                os.remove(cache_dir + '/' + old_file['filename'])
                                global_size -= old_file['size']



if __name__ == '__main__':
    a = FileCache()
    a.cleanup()