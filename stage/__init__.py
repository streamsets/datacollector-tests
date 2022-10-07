# Copyright 2017 StreamSets Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from google.cloud.bigquery import Table

logger = logging.getLogger(__name__)


def _clean_up_bigquery(bigquery_client, dataset_ref):
    try:
        logger.info(f'Deleting dataset {dataset_ref}')
        bigquery_client.delete_dataset(dataset_ref, delete_contents=True)
        logger.info(f'Dataset "{dataset_ref}" deleted successfully')
    except Exception as ex:
        logger.error(f'Error encountered while deleting Google Bigquery dataset = {dataset_ref} as {ex}')


def _clean_up_gcs(gcp, bucket, bucket_name):
    try:
        logger.info(f'Deleting temporary bucket {bucket_name}')
        gcp.retry_429(bucket.delete)(force=True)
        logger.info(f'Temporary bucket "{bucket_name}" deleted successfully')
    except Exception as ex:
        logger.error(f'Error encountered while deleting Google Cloud Storage bucket = {bucket_name} as {ex}')


def _bigquery_insert_dml(bigquery_client, table, data):
    for row in data:
        fields_columns = ",".join(row.keys())
        fields_data = ",".join([f"'{v}'" for v in row.values()])
        sql = f"INSERT {table} ({fields_columns}) VALUES ({fields_data})"
        bigquery_client.query(sql).result()


def _bigquery_insert_streaming(bigquery_client, table, db_data):
    bigquery_client.insert_rows(table, db_data)


def _bigquery_get_rows(bigquery_client, table_name):
    return bigquery_client.query(f"SELECT * FROM {table_name}").result()


def _bigquery_create_table(bigquery_client, dataset_ref, dataset_name, table_name, table_schema):
    logger.info(f'Creating dataset {dataset_name} and table {table_name}')
    bigquery_client.create_dataset(dataset_ref)
    table = bigquery_client.create_table(Table(dataset_ref.table(table_name), schema=table_schema))
    logger.info(f'Dataset {dataset_name} and table {table_name} created successfully')
    return table
