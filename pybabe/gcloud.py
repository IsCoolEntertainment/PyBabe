# coding: utf-8
from __future__ import print_function
import logging
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

"""
import gcloud

glcoud.init()

for res in glcoud.bigquery(
    project_id="xxxxx",
        query='SELECT * FROM ladata.iscool_20151201'):
    print(res)
"""

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('gcloud')
log.setLevel(logging.INFO)

conf = {}


def init():
    global conf
    conf = {
        'credentials': get_credentials(),
        'bigquery': get_bigquery(),
        'google_storage': get_storage()
    }


def get_credentials():
    global conf
    if 'credentials' not in conf:
        # [START build_service]
        # Grab the application's default credentials from the environment.
        # https://developers.google.com/identity/protocols/application-default-credentials
        # environment variable GOOGLE_APPLICATION_CREDENTIALS must be set
        conf['credentials'] = GoogleCredentials.get_application_default()
    return conf['credentials']


def get_bigquery():
    global conf
    if 'bigquery' not in conf:
        credentials = get_credentials()
        conf['bigquery'] = discovery.build('bigquery', 'v2', credentials=credentials)
    return conf['bigquery']


def get_storage():
    global conf
    if 'google_storage' not in conf:
        credentials = get_credentials()
        conf['google_storage'] = discovery.build('storage', 'v1', credentials=credentials)
    return conf['google_storage']


def bigquery(project_id, query, timeout=1000, num_retries=2):
    global conf
    bigquery = conf['bigquery']

    query_data = {
        'query': query,
        'timeoutMs': 0,  # use a timeout of 0 means we'll always need
        # to get the results via getQueryResults
    }

    response = bigquery.jobs().query(
        projectId=project_id,
        body=query_data
    ).execute(
        num_retries=num_retries
    )

    job_ref = response['jobReference']

    while True:
        page_token = response.get('pageToken', None)
        query_complete = response.get('jobComplete', False)

        if query_complete:
            for row in response['rows']:
                yield [field['v'] for field in row['f']]

            if page_token is None:
                # The query is done and there are no more results
                # to read.
                break

        response = bigquery.jobs().getQueryResults(
            pageToken=page_token,
            timeoutMs=timeout,
            **job_ref
        ).execute(
            num_retries=num_retries
        )
