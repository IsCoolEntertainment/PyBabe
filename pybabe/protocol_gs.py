# coding: utf-8
from __future__ import print_function
from glcoud import get_storage
from base import BabeBase
from protocol_s3 import ReadLineWrapper
from googleapiclient.errors import HttpError
import logging

log = logging.getLogger('Google Storage')


def push(filename_topush, filename_remote, **kwargs):
    log.info('pushing to {}/{}'.format(kwargs['bucket'], filename_remote))
    service = get_storage()
    req = service.objects().insert(
        media_body=filename_topush,
        name=filename_remote,
        bucket=kwargs['bucket'])
    resp = req.execute()
    logging.info(resp)


def check_exists(filename_remote, **kwargs):
    log.info('checking if {}/{} exists'.format(kwargs['bucket'], filename_remote))
    service = get_storage()
    req = service.objects().get(
        bucket=kwargs['bucket'],
        object=filename_remote)
    try:
        req.execute()
        return True
    except HttpError as e:
        if e.resp.status == 404:
            return False
        else:
            raise


def pull(filename_remote, **kwargs):
    service = get_storage()
    req = service.objects().get(
        object=filename_remote,
        bucket=kwargs['bucket'])
    resp = req.execute()
    print(resp)
    files = []
    for key in keys:
        logging.info("S3 Load: %s", key)
        files.append(ReadLineWrapper(key))
    return files


BabeBase.addProtocolPushPlugin('gs', push, None, check_exists)
BabeBase.addProtocolPullPlugin('gs', pull)
