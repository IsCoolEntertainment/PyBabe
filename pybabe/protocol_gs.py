# coding: utf-8

from base import BabeBase
from protocol_s3 import ReadLineWrapper
import logging


def get_service():
    from apiclient import discovery
    from oauth2client.client import GoogleCredentials

    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('storage', 'v1', credentials=credentials)
    return service


def push(filename_topush, filename_remote, **kwargs):
    service = get_service()
    req = service.objects().insert(
        media_body=filename_topush,
        name=filename_remote,
        bucket=kwargs['bucket'])
    resp = req.execute()
    logging.info(resp)


def check_exists(filename_remote, ** kwargs):
    return True


def pull(filename_remote, **kwargs):
    service = get_service()
    req = service.objects().get(
        object=filename_remote,
        bucket=kwargs['bucket'])
    resp = req.execute()
    print resp
    files = []
    for key in keys:
        logging.info("S3 Load: %s", key)
        files.append(ReadLineWrapper(key))
    return files


BabeBase.addProtocolPushPlugin('gs', push, None, check_exists)
BabeBase.addProtocolPullPlugin('gs', pull)
