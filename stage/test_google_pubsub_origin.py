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
from string import ascii_letters

from streamsets.testframework.markers import gcp, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

# For Google pub/sub
MSG_DATA = 'Hello World from SDC and DPM!'


@gcp
@sdc_min_version('2.7.0.0')
def test_google_pubsub_subscriber(sdc_builder, sdc_executor, gcp):
    """Publish messages using Google pub/sub client and then check if Google pub/sub
    subscriber receives them using wiretap.

    The pipeline looks like:
        google_pubsub_subscriber >> wiretap
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

    wiretap = pipeline_builder.add_wiretap()

    google_pubsub_subscriber >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    pubsub_publisher_client = gcp.pubsub_publisher_client
    pubsub_subscriber_client = gcp.pubsub_subscriber_client

    project_id = gcp.project_id
    topic_path = pubsub_publisher_client.topic_path(project_id, topic_name)
    subscription_path = pubsub_subscriber_client.subscription_path(project_id, subscription_id)

    sdc_executor.add_pipeline(pipeline)
    try:
        # Using Google pub/sub client, create topic and subscription
        logger.info('Creating topic %s using Google pub/sub client ...', topic_name)
        pubsub_publisher_client.create_topic(topic_path)

        pubsub_subscriber_client.create_subscription(subscription_path, topic_path)

        # Start pipeline and verify messages are received using wiretap.
        logger.info('Starting pipeline')

        logger.info('Publishing messages to topic %s using Google pub/sub client ...', topic_name)
        encoded_msg_data = MSG_DATA.encode()
        num = 10
        for _ in range(num):
            pubsub_publisher_client.publish(topic_path, encoded_msg_data)

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', num)
        sdc_executor.stop_pipeline(pipeline)

        assert [record.field['text'].value for record in wiretap.output_records] == [MSG_DATA] * num
    finally:
        pubsub_subscriber_client.delete_subscription(subscription_path)
        pubsub_publisher_client.delete_topic(topic_path)


@gcp
@sdc_min_version('2.7.0.0')
def test_google_pubsub_subscriber_stop_start(sdc_builder, sdc_executor, gcp):
    """Publish messages using Google pub/sub client and then check if Google pub/sub
    subscriber receives them using wiretap.

    We add a start and stop to check that everything has been read.

    The pipeline looks like:
        google_pubsub_subscriber >> wiretap
    """
    def get_data():
        data = []
        for _ in range(10):
            data.append(get_random_string())
        return data

    pipeline_builder = sdc_builder.get_pipeline_builder()

    first_data = get_data()
    second_data = get_data()

    subscription_id = get_random_string(ascii_letters, 5)
    topic_name = get_random_string(ascii_letters, 5)
    google_pubsub_subscriber = pipeline_builder.add_stage('Google Pub Sub Subscriber', type='origin')
    google_pubsub_subscriber.set_attributes(batch_wait_time_in_ms=20000,
                                            data_format='TEXT',
                                            max_batch_size_in_records=10,
                                            num_pipeline_runners=1,
                                            subscriber_thread_pool_size=1,
                                            subscription_id=subscription_id)

    wiretap = pipeline_builder.add_wiretap()

    google_pubsub_subscriber >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)

    pubsub_publisher_client = gcp.pubsub_publisher_client
    pubsub_subscriber_client = gcp.pubsub_subscriber_client

    project_id = gcp.project_id
    topic_path = pubsub_publisher_client.topic_path(project_id, topic_name)
    subscription_path = pubsub_subscriber_client.subscription_path(project_id, subscription_id)
    try:
        # Using Google pub/sub client, create topic and subscription
        logger.info('Creating topic %s using Google pub/sub client ...', topic_name)
        pubsub_publisher_client.create_topic(topic_path)

        pubsub_subscriber_client.create_subscription(subscription_path, topic_path)

        logger.info('Publishing messages to topic %s using Google pub/sub client ...', topic_name)
        for element in first_data:
            pubsub_publisher_client.publish(topic_path, element.encode())

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(first_data))
        sdc_executor.stop_pipeline(pipeline)

        records_from_wiretap = [record.field['text'].value for record in wiretap.output_records]

        assert records_from_wiretap == first_data

        logger.info('Publishing messages to topic %s using Google pub/sub client ...', topic_name)
        for element in second_data:
            pubsub_publisher_client.publish(topic_path, element.encode())

        wiretap.reset()

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(second_data))
        sdc_executor.stop_pipeline(pipeline)

        records_from_wiretap = [record.field['text'].value for record in wiretap.output_records]

        assert records_from_wiretap == second_data

    finally:
        pubsub_subscriber_client.delete_subscription(subscription_path)
        pubsub_publisher_client.delete_topic(topic_path)
