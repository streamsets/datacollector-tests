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

"""The tests in this module follow a pattern of creating pipelines with
:py:obj:`testframework.sdc_models.PipelineBuilder` in one version of SDC and then importing and running them in
another.
"""

import logging
import time
from string import ascii_letters

from google.cloud.bigquery import SchemaField

from testframework.markers import gcp, sdc_min_version
from testframework.utils import get_random_string

logger = logging.getLogger(__name__)

DEFAULT_COLUMN_FAMILY_NAME = 'cf'  # for Google Bigtable
# For Google pub/sub
MSG_DATA = 'Hello World from SDC and DPM!'
SNAPSHOT_TIMEOUT_SEC = 120
# For Google BigQuery, data to insert- needs to be in the sorted order by name.
ROWS_TO_INSERT = [('Cristiano Ronaldo', 32),
                  ('David Beckham', 32),
                  ('Gerard Pique', 30),
                  ('Lionel Messi', 30),
                  ('Mario Gotze', 25),
                  ('Neymar', 25),
                  ('Pele', 76),
                  ('Ronaldinho', 40),
                  ('Ronaldo', 40),
                  ('Zinedine Zidane', 42)]


@gcp
@sdc_min_version('2.7.0.0')
def test_google_bigquery_origin(sdc_builder, sdc_executor, gcp):
    """
    Create data using Google BigQuery client
    and then check if Google BigQuery origin receives them using snapshot.

    The pipeline looks like:
        google_bigquery >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dataset_name = get_random_string(ascii_letters, 5)
    table_name = get_random_string(ascii_letters, 5)
    google_bigquery = pipeline_builder.add_stage('Google BigQuery', type='origin')
    query_str = f'SELECT * FROM {dataset_name}.{table_name} ORDER BY full_name'
    google_bigquery.set_attributes(query=query_str)

    trash = pipeline_builder.add_stage('Trash')
    google_bigquery >> trash
    pipeline = pipeline_builder.build(title='Google BigQuery').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Using Google bigquery client, create dataset, table and data inside table
        logger.info('Creating dataset %s using Google bigquery client ...', dataset_name)
        bigquery_client = gcp.bigquery_client
        dataset = bigquery_client.dataset(dataset_name)
        dataset.create()
        assert dataset.exists()
        table = dataset.table(table_name)
        table.create()
        assert table.exists()
        table.schema = [SchemaField('full_name', 'STRING', mode='required'),
                        SchemaField('age', 'INTEGER', mode='required')]
        table.update()
        table.insert_data(ROWS_TO_INSERT)

        # Start pipeline and verify correct rows are received using snaphot.
        logger.info('Starting pipeline and snapshot')
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)
        rows_from_snapshot = [(record.value['value'][0]['value'], int(record.value['value'][1]['value']))
                              for record in snapshot[google_bigquery].output]

        assert rows_from_snapshot == ROWS_TO_INSERT
    finally:
        table.delete()
        dataset.delete()


@gcp
@sdc_min_version('2.7.0.0')
def test_google_pubsub_subscriber(sdc_builder, sdc_executor, gcp):
    """Publish messages using Google pub/sub client and then check if Google pub/sub
    subscriber receives them using snapshot.

    The pipeline looks like:
        google_pubsub_subscriber >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    subscription_id = get_random_string(ascii_letters, 5)
    topic_name = get_random_string(ascii_letters, 5)
    google_pubsub_subscriber = pipeline_builder.add_stage('Google Pub Sub Subscriber', type='origin')
    google_pubsub_subscriber.set_attributes(batch_wait_time_in_ms=20000,
                                            data_format='TEXT',
                                            max_batch_size_in_records=10,
                                            num_pipeline_runners=1,
                                            subscriber_thread_pool_size=1,
                                            subscription_id=subscription_id)

    trash = pipeline_builder.add_stage('Trash')
    google_pubsub_subscriber >> trash
    pipeline = pipeline_builder.build(title='Google Pub/Sub Subscriber').configure_for_environment(gcp)

    try:
        # Using Google pub/sub client, create topic and subscription
        logger.info('Creating topic %s using Google pub/sub client ...', topic_name)
        pubsub_client = gcp.pubsub_client
        topic = pubsub_client.topic(topic_name)
        topic.create()
        assert topic.exists()
        subscription = topic.subscription(subscription_id)
        subscription.create()
        assert subscription.exists()

        # Start pipeline and verify messages are received using snaphot.
        logger.info('Starting pipeline and snapshot')
        sdc_executor.add_pipeline(pipeline)
        snapshot_pipeline_command = sdc_executor.capture_snapshot(pipeline, start_pipeline=True, wait=False)

        logger.info('Publishing messages to topic %s using Google pub/sub client ...', topic_name)
        encoded_msg_data = MSG_DATA.encode()
        num = 10
        for _ in range(num):
            topic.publish(encoded_msg_data)

        logger.debug('Finish the snapshot and verify')
        snapshot = snapshot_pipeline_command.wait_for_finished(timeout_sec=SNAPSHOT_TIMEOUT_SEC).snapshot
        sdc_executor.stop_pipeline(pipeline)
        rows_from_snapshot = [record.value['value']['text']['value']
                              for record in snapshot[google_pubsub_subscriber].output]

        logger.debug(rows_from_snapshot)
        assert rows_from_snapshot == [MSG_DATA] * num
    finally:
        subscription.delete()
        topic.delete()


@gcp
@sdc_min_version('2.7.0.0')
def test_google_pubsub_publisher(sdc_builder, sdc_executor, gcp):
    """
    Send simple text messages to Google pub/sub from Dev Raw Data Source and
    confirm that Google pub/sub successfully reads them using Google pub/sub client from gcp.

    The pipeline looks like:
        dev_raw_data_source >> google_pubsub_publisher
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'TEXT'
    dev_raw_data_source.raw_data = MSG_DATA

    subscription_id = get_random_string(ascii_letters, 5)
    topic_name = get_random_string(ascii_letters, 5)
    google_pubsub_publisher = pipeline_builder.add_stage('Google Pub Sub Publisher', type='destination')
    google_pubsub_publisher.set_attributes(topic_id=topic_name,
                                           data_format='TEXT')

    dev_raw_data_source >> google_pubsub_publisher
    pipeline = pipeline_builder.build(title='Google Pub/Sub Publisher').configure_for_environment(gcp)
    pipeline.rate_limit = 1

    try:
        # Using Google pub/sub client, create topic and subscription
        logger.info('Creating topic %s using Google pub/sub client ...', topic_name)
        pubsub_client = gcp.pubsub_client
        topic = pubsub_client.topic(topic_name)
        topic.create()
        assert topic.exists()
        logger.info('Creating subscription')
        subscription = topic.subscription(subscription_id)
        subscription.create()
        assert subscription.exists()

        # Send messages using pipeline to Google pub/sub publisher Destination.
        logger.info('Starting Publisher pipeline and waiting for it to produce records ...')
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(10)

        logger.info('Stopping Publisher pipeline and getting the count of records produced in total ...')
        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.pipeline_history(pipeline)
        msgs_sent_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        logger.info('No. of messages sent in the pipeline = %s ...', msgs_sent_count)

        # Receive messages in a loop until timeout or expected no. of messages are received
        results = []
        msgs_received_count = msgs_sent_count
        timeout = 5  # in seconds
        start_time = time.time()
        while time.time() < start_time + timeout and msgs_received_count > 0:
            messages = subscription.pull(max_messages=msgs_sent_count)
            results.extend(messages)
            msgs_received_count = msgs_received_count - len(messages)

        # Acknowldege the received messages so that the pub/sub does not resend them
        if results:
            subscription.acknowledge([ack_id for ack_id, message in results])

        # Verify
        msgs_received = [message[1].data.decode().rstrip('\n') for message in results]
        assert msgs_received == [dev_raw_data_source.raw_data] * msgs_sent_count

    finally:
        subscription.delete()
        topic.delete()


@gcp
@sdc_min_version('2.7.0.0')
def test_google_bigtable_destination(sdc_builder, sdc_executor, gcp):
    """
    Send text to Google bigtable from Dev Raw Data Source and
    confirm that Google bigtable successfully reads them using happybase connection from gcp.

    The pipeline looks like:
        dev_raw_data_source >> google_bigtable
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    # Data with 3 distinct TransactionIDs
    data = ['TransactionID,Type,UserID', '0003420301,01,1001', '0003420302,02,1002', '0003420303,03,1003',
            '0003420301,04,1004', '0003420302,05,1005', '0003420303,06,1006']
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(data))

    table_name = get_random_string(ascii_letters, 5)
    google_bigtable = pipeline_builder.add_stage('Google Bigtable', type='destination')
    # Field paths, name of columns, storage types
    fields_list = [{'source': '/TransactionID', 'storageType': 'TEXT', 'column': 'TransactionID'},
                   {'source': '/Type', 'storageType': 'TEXT', 'column': 'Type'},
                   {'source': '/UserID', 'storageType': 'TEXT', 'column': 'UserID'}]
    google_bigtable.set_attributes(create_table_and_column_families=True,
                                   default_column_family_name=DEFAULT_COLUMN_FAMILY_NAME,
                                   explicit_column_family_mapping=False,
                                   fields=fields_list,
                                   row_key='/TransactionID',
                                   table_name=table_name)

    dev_raw_data_source >> google_bigtable
    pipeline = pipeline_builder.build(title='Google Bigtable').configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)
    pipeline.rate_limit = 1

    connection = gcp.bigtable_connection
    try:
        # Produce Google Bigtable records using pipeline.
        logger.info('Starting Bigtable pipeline and waiting for it to produce records ...')
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)

        logger.info('Stopping Bigtable pipeline and getting the count of records produced in total ...')
        sdc_executor.stop_pipeline(pipeline)

        # Using happybase connection, read the contents in the Google Bigtable.
        logger.info('Reading contents from table %s ...', table_name)
        table = connection.table(table_name)
        scan_result = list(table.scan())

        read_data = [(f'{row["cf:TransactionID".encode()].decode()},'
                      f'{row["cf:Type".encode()].decode()},'
                      f'{row["cf:UserID".encode()].decode()}')
                     for key, row in scan_result]

        logger.debug('read_data = {}'.format(read_data))

        # Verify: Note we expect only 3 rows since there are only 3 distinct TransactionIDs in data.
        # The result expected is the latest inserted data which are the last 3 rows.
        # Reason is in Google Bigtable, each row is indexed by a single row key resulting in
        # each row/column intersection can contain multiple cells at different timestamps,
        # providing a record of how the stored data has been altered over time.
        assert data[4:] == read_data
    finally:
        connection.delete_table(table_name)
