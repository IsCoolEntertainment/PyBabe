# coding: utf-8
from __future__ import print_function

import time
# import json
from base import BabeBase, StreamHeader, StreamFooter
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials


def get_bigquery():
    # [START build_service]
    # Grab the application's default credentials from the environment.
    # https://developers.google.com/identity/protocols/application-default-credentials
    # environment variable GOOGLE_APPLICATION_CREDENTIALS must be set
    credentials = GoogleCredentials.get_application_default()
    # Construct the service object for interacting with the BigQuery API.
    bigquery = discovery.build('bigquery', 'v2', credentials=credentials)
    # [END build_service]
    return bigquery


def push_bigquery(stream,
                  bucket,
                  filename,
                  project_id,
                  dataset_id,
                  table_name,
                  schema,
                  job_id,
                  data_path=None,
                  num_retries=2,
                  **kwargs):

    bigquery = get_bigquery()

    job_data = {
        'jobReference': {
            'projectId': project_id,
            'jobId': job_id,
        },
        'configuration': {
            'load': {
                'sourceUris': ['gs://{}/{}'.format(bucket, filename)],
                'schema': {
                    'fields': schema
                },
                'destinationTable': {
                    'projectId': project_id,
                    'datasetId': dataset_id,
                    'tableId': table_name
                },
                'fieldDelimiter': '\t',
                'skipLeadingRows': 1
            }
        }
    }

    job = bigquery.jobs().insert(
        projectId=project_id,
        body=job_data
    ).execute(
        num_retries=num_retries
    )

    print('inserting the file from google storage to bigquery')
    print('Waiting for job to finish...')

    request = bigquery.jobs().get(
        projectId=job['jobReference']['projectId'],
        jobId=job['jobReference']['jobId'])

    while True:
        result = request.execute(num_retries=num_retries)

        if result['status']['state'] == 'DONE':
            if 'errorResult' in result['status']:
                raise RuntimeError(result['status']['errorResult'])
            print('Job complete.')
            return

        time.sleep(1)


def pull_bigquery(false_stream,
                  project_id,
                  query=None,
                  timeout=1000,
                  num_retries=2,
                  **kwargs):

    bigquery = get_bigquery()

    query_data = {
        'query': query,
        'timeoutMs': timeout,
    }

    query_job = bigquery.jobs().query(
        projectId=project_id,
        body=query_data
    ).execute(
        num_retries=num_retries
    )

    metainfo = None
    page_token = None
    while True:
        if not metainfo:
            fields = [f['name'] for f in query_job['schema']['fields']]
            typename = kwargs.get('typename', 'BigQuery')
            metainfo = StreamHeader(**dict(kwargs, typename=typename, fields=fields))
            yield metainfo

        page = bigquery.jobs().getQueryResults(
            pageToken=page_token,
            **query_job['jobReference']
        ).execute(
            num_retries=num_retries
        )

        for row in page['rows']:
            yield metainfo.t(*[field['v'] for field in row['f']])
        page_token = page.get('pageToken')
        if not page_token:
            yield StreamFooter()
            break


BabeBase.register('pull_bigquery', pull_bigquery)
BabeBase.registerFinalMethod('push_bigquery', push_bigquery)
