# Copyright 2021 StreamSets Inc.
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
import string

from pulsar import MessageId
from streamsets.testframework.markers import pulsar, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@pulsar
@sdc_min_version('3.5.0')
def test_pulsar_producer(sdc_builder, sdc_executor, pulsar):
    """Test for Pulsar producer target stage. We do so by publishing data to a test topic using Pulsar producer
    stage and then read the data from that topic using Pulsar client. We assert the data from the client to what has
    been injected by the producer pipeline. The pipeline looks like:

    Pulsar Producer pipeline:
        dev_raw_data_source >> pulsar_producer
    """
    topic_name = get_random_string(string.ascii_letters, 10)
    input_text = 'Hello World!'

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  raw_data=input_text)
    pulsar_producer = builder.add_stage('Pulsar Producer')
    pulsar_producer.set_attributes(topic=topic_name, data_format='TEXT')

    dev_raw_data_source >> pulsar_producer
    producer_dest_pipeline = builder.build(title='Pulsar Producer pipeline').configure_for_environment(pulsar)

    # add pipeline and capture pipeline messages to assert
    sdc_executor.add_pipeline(producer_dest_pipeline)
    sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_pipeline_batch_count(10)
    sdc_executor.stop_pipeline(producer_dest_pipeline)

    history = sdc_executor.get_pipeline_history(producer_dest_pipeline)
    msgs_sent_count = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
    logger.debug('Number of messages ingested into the pipeline = %s', msgs_sent_count)

    client = pulsar.client
    admin = pulsar.admin
    try:
        reader = client.create_reader(topic_name, MessageId.earliest)
        msgs_received = []
        while reader.has_message_available():
            msgs_received.append(reader.read_next().data().decode().strip()) # strip to remove newlines
    finally:
        reader.close()  # reader needs to be closed before topic can be deleted without force
        client.close()
        admin.delete_topic(reader.topic())

    logger.debug('Number of messages received from Pulsar = %d', len(msgs_received))
    assert msgs_received == [input_text] * msgs_sent_count

