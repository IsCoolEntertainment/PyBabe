# coding: utf-8
from __future__ import print_function

import time
# import json
from base import BabeBase, StreamHeader, StreamFooter
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials


class BigQueryException(RuntimeError):
    pass


class FailedJobException(BigQueryException):
    pass


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
                  num_retries=2,
                  **kwargs):

    bigquery = get_bigquery()
    job_data = {
        'jobReference': {
            'projectId': project_id
        },
        'configuration': {
            'load': {
                'quote': '|',
                'allowQuotedNewlines': True,
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
                raise FailedJobException(result['status']['errorResult'])
            print('Job complete.')
            return

        time.sleep(1)


def pull_bigquery(false_stream,
                  project_id,
                  query=None,
                  timeout=10000,
                  num_retries=2,
                  **kwargs):

    bigquery = get_bigquery()

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

    metainfo = None
    job_ref = response['jobReference']

    while True:

        page_token = response.get('pageToken', None)
        query_complete = response.get('jobComplete', False)

        if query_complete:
            if not metainfo:
                fields = [f['name'] for f in response['schema']['fields']]
                typename = kwargs.get('typename', 'BigQuery')
                metainfo = StreamHeader(**dict(kwargs, typename=typename, fields=fields))
                yield metainfo

            for row in response['rows']:
                yield metainfo.t(*[field['v'] for field in row['f']])

            if page_token is None:
                # The query is done and there are no more results
                # to read.
                yield StreamFooter()
                break

        response = bigquery.jobs().getQueryResults(
            pageToken=page_token,
            timeoutMs=timeout,
            **job_ref
        ).execute(
            num_retries=num_retries
        )


BabeBase.register('pull_bigquery', pull_bigquery)
BabeBase.registerFinalMethod('push_bigquery', push_bigquery)
