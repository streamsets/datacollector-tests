import logging
import time

from testframework.markers import mqtt

logger = logging.getLogger(__name__)


@mqtt
def test_raw_to_mqtt(sdc_builder, sdc_executor, mqtt_broker):
    """Integration test for the MQTT destination stage.

     1) load a pipeline that has a raw data (text) origin and MQTT destination
     2) create MQTT instance (broker and a subscribed client) and inject appropriate values into
        the pipeline config
     3) run the pipeline and capture a snapshot
     4) check messages received by the MQTT client and ensure their number and contents match the
        pipeline origin data
    """
    # pylint: disable=too-many-locals

    data_topic = 'testframework_mqtt_topic'

    try:
        mqtt_broker.initialize(initial_topics=[data_topic])

        pipeline_builder = sdc_builder.get_pipeline_builder()

        raw_str = 'dummy_value'
        dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.data_format = 'TEXT'
        dev_raw_data_source.raw_data = raw_str

        mqtt_target = pipeline_builder.add_stage('MQTT Publisher')
        mqtt_target.configuration.update({'publisherConf.topic': data_topic,
                                          'publisherConf.dataFormat': 'TEXT'})

        dev_raw_data_source >> mqtt_target

        pipeline = pipeline_builder.build().configure_for_environment(mqtt_broker)

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        output_records = snapshot[dev_raw_data_source.instance_name].output
        for output_record in output_records:
            # sanity checks on output of raw data source
            assert output_record.value['value']['text']['value'] == raw_str

        # with QOS=2 (default), exactly one message should be received per published message
        # so we should have no trouble getting as many messages as output records from the
        # snapshot
        pipeline_msgs = mqtt_broker.get_messages(data_topic, num=len(output_records))
        for msg in pipeline_msgs:
            assert msg.payload.decode().rstrip() == raw_str
            assert msg.topic == data_topic
    finally:
        mqtt_broker.destroy()


@mqtt
def test_mqtt_to_trash(sdc_builder, sdc_executor, mqtt_broker):
    """Integration test for the MQTT origin stage.

     1) load a pipeline that has an MQTT origin (text format) to trash
     2) create MQTT instance (broker and a subscribed client) and inject appropriate values into
        the pipeline config
     3) run the pipeline and capture a snapshot
     4) (in parallel) send message to the topic the pipeline is subscribed to
     5) after snapshot completes, verify outputs from pipeline snapshot against published messages
    """
    # pylint: disable=too-many-locals

    data_topic = 'mqtt_subscriber_topic'
    try:
        mqtt_broker.initialize(initial_topics=[data_topic])

        pipeline_builder = sdc_builder.get_pipeline_builder()

        mqtt_source = pipeline_builder.add_stage('MQTT Subscriber')
        mqtt_source.configuration.update({'subscriberConf.dataFormat': 'TEXT',
                                          'subscriberConf.topicFilters': [data_topic]})

        trash = pipeline_builder.add_stage('Trash')

        mqtt_source >> trash

        pipeline = pipeline_builder.build().configure_for_environment(mqtt_broker)
        sdc_executor.add_pipeline(pipeline)

        # the MQTT origin produces a single batch for each message it receieves, so we need
        # to run a separate snapshot for each message to be received
        running_snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True,
                                                         batches=10, wait=False)

        # can't figure out a cleaner way to do this; it takes a bit of time for the pipeline
        # to ACTUALLY start listening on the MQTT port, so if we don't sleep here, the
        # messages won't be delivered (without setting persist)
        time.sleep(1)
        expected_messages = set()
        for i in range(10):
            expected_message = 'Message {0}'.format(i)
            mqtt_broker.publish_message(topic=data_topic, payload=expected_message)
            expected_messages.add(expected_message)

        snapshot = running_snapshot.wait_for_finished().snapshot
        sdc_executor.stop_pipeline(pipeline)

        for batch in snapshot.snapshot_batches:
            output_records = batch[mqtt_source.instance_name].output
            # each batch should only contain one record
            assert len(output_records) == 1

            for output_record in output_records:
                value = output_record.value['value']['text']['value']
                assert value in expected_messages
                assert expected_messages.remove(value) is None
        assert len(expected_messages) == 0
    finally:
        mqtt_broker.destroy()
