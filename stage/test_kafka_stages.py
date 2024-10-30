# Copyright 2022 StreamSets Inc.
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

import pytest

from streamsets.testframework.environments.kafka import KafkaCluster
from streamsets.testframework.markers import cluster, sdc_min_version
from streamsets.testframework.utils import get_random_string, Version

logger = logging.getLogger(__name__)


@cluster('cdh', 'kafka')
@sdc_min_version('5.3.0')
@pytest.mark.parametrize('records_to_be_generated', [100])
def test_kafka_headers(sdc_builder, sdc_executor, cluster,
                       records_to_be_generated):
    """Run two pipelines in parallel. One creates multiples records with their headers and sends to a Kafka destination.
    The other reads from the same Kafka instance as an origin. We check headers are preserved through all the pipelines.
    """
    if isinstance(cluster, KafkaCluster) and Version(cluster.version) < Version("0.11.0"):
        pytest.skip(f'kafka version {cluster.version} does not support headers')

    topic = get_random_string(string.ascii_letters, 10)
    key = get_random_string(string.ascii_letters, 10)
    value = get_random_string(string.ascii_letters, 10)

    producer_builder = sdc_builder.get_pipeline_builder()
    producer_origin = producer_builder.add_stage('Dev Data Generator')
    producer_origin.set_attributes(records_to_be_generated=records_to_be_generated)
    producer_origin.fields_to_generate = [{'field': 'foo', 'type': 'STRING'}]
    producer_origin.header_attributes = [{ "value": value, "key": key }]
    producer_destination = producer_builder.add_stage('Kafka Producer')
    producer_destination.set_attributes(data_format="JSON",
                                        topic=topic)
    producer_origin >> producer_destination
    producer_pipeline = producer_builder.build().configure_for_environment(cluster)

    consumer_builder = sdc_builder.get_pipeline_builder()
    consumer_origin = consumer_builder.add_stage('Kafka Multitopic Consumer')
    consumer_origin.set_attributes(data_format="JSON",
                                   topic_list=[topic])
    consumer_wiretap = consumer_builder.add_wiretap()
    consumer_origin >> consumer_wiretap.destination
    consumer_pipeline = consumer_builder.build().configure_for_environment(cluster)

    sdc_executor.add_pipeline(producer_pipeline)
    sdc_executor.start_pipeline(producer_pipeline).wait_for_finished()
    sdc_executor.add_pipeline(consumer_pipeline)
    sdc_executor.start_pipeline(consumer_pipeline)
    sdc_executor.wait_for_pipeline_metric(consumer_pipeline, 'output_record_count', records_to_be_generated)

    output_records = consumer_wiretap.output_records
    try:
        for i in range(len(output_records)):
            assert output_records[i].header.values[key] == value
    except KeyError:
        assert False, "Header key not found"
    except AssertionError:
        assert False, "Header value not found"


@cluster('cdh', 'kafka')
@sdc_min_version('6.1.0')
@pytest.mark.parametrize('propagate', [True, False])
def test_kafka_destination_propagate_headers(propagate, sdc_builder, sdc_executor, cluster):
    """
    Test the behavior of the new configuration property "Propagate Headers" for Kafka Destination stage.
    When set to True, the Kafka attributes are propagated into the records as headers. This is the default behavior
    since SDC 5.3.0. For previous SDC versions, the behavior was to ignore them, which corresponds to setting the
    new configuration to False.

    Testing with simple pipeline:
        dev_data_generator >> kafka_destination
    """
    if isinstance(cluster, KafkaCluster) and Version(cluster.version) < Version("0.11.0"):
        pytest.skip(f'Kafka version {cluster.version} does not support headers')

    records_to_be_generated = 100
    topic = f'stf_{get_random_string(string.ascii_letters, 10)}'
    key = f'stf_{get_random_string(string.ascii_letters, 10)}'
    value = f'stf_{get_random_string(string.ascii_letters, 10)}'

    producer_builder = sdc_builder.get_pipeline_builder()

    producer_origin = producer_builder.add_stage('Dev Data Generator')
    producer_origin.set_attributes(records_to_be_generated=records_to_be_generated)
    producer_origin.fields_to_generate = [{'field': 'foo', 'type': 'STRING'}]
    producer_origin.header_attributes = [{"key": key, "value": value}]

    producer_destination = producer_builder.add_stage('Kafka Producer')
    producer_destination.set_attributes(data_format="JSON",
                                        propagate_headers=propagate,
                                        topic=topic)

    producer_origin >> producer_destination

    producer_pipeline = producer_builder.build().configure_for_environment(cluster)

    consumer_builder = sdc_builder.get_pipeline_builder()

    consumer_origin = consumer_builder.add_stage('Kafka Multitopic Consumer')
    consumer_origin.set_attributes(data_format="JSON",
                                   topic_list=[topic])
    consumer_wiretap = consumer_builder.add_wiretap()

    consumer_origin >> consumer_wiretap.destination

    consumer_pipeline = consumer_builder.build().configure_for_environment(cluster)

    try:
        sdc_executor.add_pipeline(producer_pipeline)
        sdc_executor.start_pipeline(producer_pipeline).wait_for_finished()

        sdc_executor.add_pipeline(consumer_pipeline)
        sdc_executor.start_pipeline(consumer_pipeline)
        sdc_executor.wait_for_pipeline_metric(consumer_pipeline, 'output_record_count', records_to_be_generated)

        output_records = consumer_wiretap.output_records

        for i in range(len(output_records)):
            if propagate:
                try:
                    assert output_records[i].header.values[key] == value
                except KeyError:
                    assert False, "Header key not found"
                except AssertionError:
                    assert False, "Header value not found"
            else:
                assert key not in output_records[i].header.values

    finally:
        try:
            sdc_executor.stop_pipeline(producer_pipeline, force=True)
        except Exception:
            pass
        try:
            sdc_executor.stop_pipeline(consumer_pipeline, force=True)
        except Exception:
            pass
