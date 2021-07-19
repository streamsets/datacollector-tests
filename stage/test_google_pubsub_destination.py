# Copyright 2020 StreamSets Inc.
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
import time
from string import ascii_letters
from time import sleep

import pytest
from streamsets.testframework.markers import gcp, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.category('nonstandard')]

# For Google pub/sub
MSG_DATA = 'Hello World from SDC and DPM!'


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
    google_pubsub_publisher.set_attributes(topic_id=topic_name, data_format='TEXT')

    dev_raw_data_source >> google_pubsub_publisher
    pipeline = pipeline_builder.build(title='Google Pub Sub Publisher Pipeline').configure_for_environment(gcp)
    pipeline.rate_limit = 1

    pubsub_publisher_client = gcp.pubsub_publisher_client
    pubsub_subscriber_client = gcp.pubsub_subscriber_client

    project_id = gcp.project_id
    topic_path = pubsub_publisher_client.topic_path(project_id, topic_name)
    subscription_path = pubsub_subscriber_client.subscription_path(project_id, subscription_id)
    try:
        # Using Google pub/sub client, create topic and subscription
        logger.info('Creating topic %s using Google pub/sub client ...', topic_name)

        topic = pubsub_publisher_client.create_topic(topic_path)

        subscription = pubsub_subscriber_client.create_subscription(subscription_path, topic_path)

        # Send messages using pipeline to Google pub/sub publisher Destination.
        logger.info('Starting Publisher pipeline and waiting for it to produce records ...')
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(10)

        logger.info('Stopping Publisher pipeline and getting the count of records produced in total ...')
        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        msgs_sent_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        logger.info('No. of messages sent in the pipeline = %s ...', msgs_sent_count)

        msgs_to_be_received = msgs_sent_count
        results = []

        def callback(message):
            nonlocal msgs_to_be_received
            msgs_to_be_received = msgs_to_be_received - 1
            results.append(message)
            message.ack()

        # Open the subscription, passing the callback.
        future = pubsub_subscriber_client.subscribe(subscription_path, callback)

        timeout = 5  # in seconds
        start_time = time.time()
        while time.time() < start_time + timeout and msgs_to_be_received > 0:
            sleep(0.5)
            logger.info('To be received messages %d...', msgs_to_be_received)

        future.cancel()  # cancel the feature there by stopping subscribers

        # Verify
        msgs_received = [message.data.decode().rstrip('\n') for message in results]
        assert msgs_received == [dev_raw_data_source.raw_data] * msgs_sent_count

    finally:
        pubsub_subscriber_client.delete_subscription(subscription_path)
        pubsub_publisher_client.delete_topic(topic_path)
