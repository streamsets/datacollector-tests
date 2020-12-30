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
import time

from streamsets.testframework.markers import mqtt

logger = logging.getLogger(__name__)


@mqtt
def test_raw_to_mqtt(sdc_builder, sdc_executor, mqtt_broker):
    """Integration test for the MQTT destination stage.

     1) load a pipeline that has a raw data (text) origin and MQTT destination
     2) create MQTT instance (broker and a subscribed client) and inject appropriate values into
        the pipeline config
     3) run the pipeline
     4) check messages received by the MQTT client and ensure their number and contents match the
        origin data
    """
    # pylint: disable=too-many-locals

    data_topic = 'testframework_mqtt_topic'

    try:
        mqtt_broker.initialize(initial_topics=[data_topic])

        pipeline_builder = sdc_builder.get_pipeline_builder()

        raw_str = 'dummy_value'
        dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_str, stop_after_first_batch=True)

        mqtt_target = pipeline_builder.add_stage('MQTT Publisher')
        mqtt_target.configuration.update({'publisherConf.topic': data_topic,
                                          'publisherConf.dataFormat': 'TEXT'})

        dev_raw_data_source >> mqtt_target

        pipeline = pipeline_builder.build().configure_for_environment(mqtt_broker)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # with QOS=2 (default), exactly one message should be received per published message
        # so we should have no trouble getting as many messages as output records from the
        # snapshot
        pipeline_msgs = mqtt_broker.get_messages(data_topic)
        for msg in pipeline_msgs:
            assert msg.payload.decode().rstrip() == raw_str
            assert msg.topic == data_topic
    finally:
        mqtt_broker.destroy()


@mqtt
def test_mqtt_to_trash(sdc_builder, sdc_executor, mqtt_broker):
    """Integration test for the MQTT origin stage.

     1) load a pipeline that has an MQTT origin (text format) to wiretap
     2) create MQTT instance (broker and a subscribed client) and inject appropriate values into
        the pipeline config
     3) run the pipeline
     4) (in parallel) send message to the topic the pipeline is subscribed to
     5) after pipeline completes, verify outputs from pipeline against published messages
    """
    # pylint: disable=too-many-locals

    data_topic = 'mqtt_subscriber_topic'
    try:
        mqtt_broker.initialize(initial_topics=[data_topic])

        pipeline_builder = sdc_builder.get_pipeline_builder()

        mqtt_source = pipeline_builder.add_stage('MQTT Subscriber')
        mqtt_source.configuration.update({'subscriberConf.dataFormat': 'TEXT',
                                          'subscriberConf.topicFilters': [data_topic]})

        wiretap = pipeline_builder.add_wiretap()

        mqtt_source >> wiretap.destination

        pipeline = pipeline_builder.build().configure_for_environment(mqtt_broker)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        # can't figure out a cleaner way to do this; it takes a bit of time for the pipeline
        # to ACTUALLY start listening on the MQTT port, so if we don't sleep here, the
        # messages won't be delivered (without setting persist)
        time.sleep(1)
        expected_messages = set()
        for i in range(10):
            expected_message = 'Message {0}'.format(i)
            mqtt_broker.publish_message(topic=data_topic, payload=expected_message)
            expected_messages.add(expected_message)

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 10)
        sdc_executor.stop_pipeline(pipeline)

        assert len(wiretap.output_records) == 10

        for output_record in wiretap.output_records:
            value = output_record.field['text'].value
            assert value in expected_messages
            assert expected_messages.remove(value) is None
        assert len(expected_messages) == 0
    finally:
        mqtt_broker.destroy()
