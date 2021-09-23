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

import json
import logging
import string
import time
from datetime import timezone

import pytest
from streamsets.testframework.markers import influxdb2, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

pytestmark = [influxdb2, sdc_min_version('4.2.0')]

RAW_DATA = [
    {'measurement': get_random_string(string.ascii_letters, 10),
     'record': {'time': round(time.time() * 1000), 'butterflies': 12, 'location': '1', 'scientist': 'langstroth'}},
    {'measurement': get_random_string(string.ascii_letters, 10),
     'record': {'time': round(time.time() * 1000), 'butterflies': 8, 'location': '2', 'scientist': 'perpetua'}},
    {'measurement': get_random_string(string.ascii_letters, 10),
     'record': {'time': round(time.time() * 1000), 'butterflies': 3, 'location': '1', 'scientist': 'perpetua'}},
    {'measurement': get_random_string(string.ascii_letters, 10),
     'record': {'time': round(time.time() * 1000), 'butterflies': 1, 'location': '2', 'scientist': 'langstroth'}}]


# Reference: InfluxDB does not document what do they support or not, we are just trying to break them
BUCKET_NAMES = [
    ('lowercase', get_random_string(string.ascii_lowercase)),
    ('uppercase', get_random_string(string.ascii_uppercase)),
    ('letters', get_random_string(string.ascii_letters)),
    ('digits', get_random_string(string.digits)),
    ('hexadecimal', get_random_string(string.hexdigits).lower()),
    ('hypen', get_random_string() + '-' + get_random_string()),
    ('start_hypen', '-' + get_random_string()),
    ('end_hypen', get_random_string() + '-'),
    ('underscore', get_random_string() + '_' + get_random_string()),
    ('start_underscore', get_random_string() + '_'),
    # ('end_underscore', '_' + get_random_string()), Not supported by InfluxDB
    ('dot', get_random_string() + '.' + get_random_string()),
    ('start_dot', '.' + get_random_string()),
    ('end_dot', get_random_string() + '.'),
    ('minsize', get_random_string(string.ascii_lowercase, 1)),
    ('maxsize', get_random_string(string.ascii_lowercase, 9999)),
    ('forward_slash', get_random_string() + '/' + get_random_string()),
    ('start_forward_slash', '/' + get_random_string()),
    ('end_forward_slash', get_random_string() + '/'),
    ('exclamation_point', get_random_string() + '!' + get_random_string()),
    ('start_exclamation_point', '!' + get_random_string()),
    ('end_exclamation_point', get_random_string() + '!'),
    ('comma', get_random_string() + ',' + get_random_string()),
    ('start_comma', ',' + get_random_string()),
    ('end_comma', get_random_string() + ','),
    ('asterisk', get_random_string() + '*' + get_random_string()),
    ('start_asterisk', '*' + get_random_string()),
    ('end_asterisk', get_random_string() + '*'),
    ('single_quote', get_random_string() + '\'' + get_random_string()),
    ('start_single_quote', '\'' + get_random_string()),
    ('end_single_quote', get_random_string() + '\''),
    ('open_parenthesis', get_random_string() + '(' + get_random_string()),
    ('start_open_parenthesis', '(' + get_random_string()),
    ('end_open_parenthesis', get_random_string() + '('),
    ('close_parenthesis', get_random_string() + ')' + get_random_string()),
    ('start_close_parenthesis', ')' + get_random_string()),
    ('end_close_parenthesis', get_random_string() + ')'),
    ('at', get_random_string() + '@' + get_random_string()),
    ('start_at', '@' + get_random_string()),
    ('end_at', get_random_string() + '@'),
    ('dollar', get_random_string() + '$' + get_random_string()),
    ('start_dollar', '$' + get_random_string()),
    ('end_dollar', get_random_string() + '$'),
    ('euro', get_random_string() + '€' + get_random_string()),
    ('start_euro', '€' + get_random_string()),
    ('end_euro', get_random_string() + '€'),
    ('ampersand', get_random_string() + '&' + get_random_string()),
    ('start_ampersand', '&' + get_random_string()),
    ('end_ampersand', get_random_string() + '&'),
    ('percentage', get_random_string() + '%' + get_random_string()),
    ('start_percentage', '%' + get_random_string()),
    ('end_percentage', get_random_string() + '%'),
    ('percentage', get_random_string() + '%' + get_random_string()),
    ('start_percentage', '%' + get_random_string()),
    ('end_percentage', get_random_string() + '%'),
    ('number', get_random_string() + '#' + get_random_string()),
    ('start_number', '#' + get_random_string()),
    ('end_number', get_random_string() + '#'),
]


@pytest.mark.parametrize('auto_create', [True, False])
@pytest.mark.parametrize('test_name, bucket_name', BUCKET_NAMES, ids=[t[0] for t in BUCKET_NAMES])
def test_bucket_names_topic(sdc_builder, sdc_executor, influxdb2, test_name, bucket_name, auto_create):
    """
    Verify that we can respect all the documented topic names possible.
    """
    client = influxdb2.client

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=json.dumps(RAW_DATA),
                                       stop_after_first_batch=True)
    influxdb_destination = builder.add_stage('InfluxDB 2.x', type='destination')
    influxdb_destination.set_attributes(create_bucket=auto_create,
                                        bucket=bucket_name,
                                        measurement_field='/measurement',
                                        time_field='/record/time',
                                        write_precision='MS',
                                        tag_fields=["/record/location", "/record/scientist"],
                                        value_fields=["/record/butterflies"])

    if not auto_create:
        influxdb2.create_bucket(bucket_name)

    dev_raw_data_source >> influxdb_destination
    pipeline = builder.build().configure_for_environment(influxdb2)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # read data from InfluxDB and assert to what pipeline ingested
        raw_records = [item['record'] for item in RAW_DATA]

        query = f'from(bucket: "{bucket_name}") |> range(start: -1d)'

        result = client.query_api().query(query=query, org=influxdb2.organization)

        result_records = []
        for table in result:
            for record in table.records:
                result_records.append({'butterflies': int(record['_value']), 'location': record['location'],
                                       'scientist': record['scientist'],
                                       'time': round(record['_time'].replace(tzinfo=timezone.utc).timestamp() * 1000)})

        assert len(raw_records) == len(result_records)
        assert all([influx_record in raw_records for influx_record in result_records])
    finally:
        influxdb2.drop_bucket(bucket_name)


def test_multiple_batch(sdc_builder, sdc_executor, influxdb2):
    """
    Test that we can produce multiple batches and the pipeline produces all the records.
    """

    client = influxdb2.client

    bucket_name = get_random_string()

    builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = builder.add_stage('Dev Data Generator')

    dev_data_generator.set_attributes(records_to_be_generated=10,
                                      batch_size=2,
                                      fields_to_generate=[
                                          {'field': 'measurement', 'type': 'STRING'},
                                          {'field': 'time', 'type': 'TIME'},
                                          {'field': 'butterflies', 'type': 'INTEGER'},
                                          {'field': 'scientist', 'type': 'POKEMON'}])

    influxdb_destination = builder.add_stage('InfluxDB 2.x', type='destination')
    influxdb_destination.set_attributes(bucket=bucket_name,
                                        create_bucket=True,
                                        retention_time=0,
                                        measurement_field='/measurement',
                                        time_field='/time',
                                        write_precision='MS',
                                        tag_fields=["/scientist"],
                                        value_fields=["/butterflies"])

    wiretap = builder.add_wiretap()

    dev_data_generator >> [influxdb_destination, wiretap.destination]
    pipeline = builder.build().configure_for_environment(influxdb2)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # read data from InfluxDB and assert to what pipeline ingested
        raw_records = [record.field for record in wiretap.output_records]

        query = f'from(bucket: "{bucket_name}") |> range(start: -100y)'

        result = client.query_api().query(query=query, org=influxdb2.organization)

        result_records = []
        for table in result:
            for record in table.records:
                record_aux = {'measurement': record.get_measurement(),
                              'time': record.get_time().replace(tzinfo=None),
                              'butterflies': int(record.get_value()),
                              'scientist': record.values.get("scientist")}
                result_records.append(record_aux)

        assert len(raw_records) == len(result_records)
        assert all([influx_record in raw_records for influx_record in result_records])

        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchCount.counter').count == 5
    finally:
        influxdb2.drop_bucket(bucket_name)


def test_data_types(sdc_builder, sdc_executor, cluster, influxdb2):
    pytest.skip('InfluxDB 2.x Destination does not has data types.')


def test_dataflow_events(sdc_builder, sdc_executor, influxdb2):
    pytest.skip('InfluxDB 2.x Destination does not generate events.')


def test_data_format_avro(sdc_builder, sdc_executor, influxdb2):
    pytest.skip('InfluxDB 2.x Destination does not support destination formats.')


def test_data_format_binary(sdc_builder, sdc_executor, influxdb2):
    pytest.skip('InfluxDB 2.x Destination does not support destination formats.')


def test_data_format_delimited(sdc_builder, sdc_executor, influxdb2):
    pytest.skip('InfluxDB 2.x Destination does not support destination formats.')


def test_data_format_json(sdc_builder, sdc_executor, influxdb2):
    pytest.skip('InfluxDB 2.x Destination does not support destination formats.')


def test_data_format_protobuf(sdc_builder, sdc_executor, influxdb2):
    pytest.skip('InfluxDB 2.x Destination does not support destination formats.')


def test_data_format_text(sdc_builder, sdc_executor, influxdb2):
    pytest.skip('InfluxDB 2.x Destination does not support destination formats.')


def test_data_format_sdc_record(sdc_builder, sdc_executor, influxdb2):
    pytest.skip('InfluxDB 2.x Destination does not support destination formats.')


def test_data_format_xml(sdc_builder, sdc_executor, influxdb2):
    pytest.skip('InfluxDB 2.x Destination does not support destination formats.')


def test_push_pull(sdc_builder, sdc_executor, influxdb2):
    pytest.skip('InfluxDB 2.x Destination existing tests already stress both type of Origins.')

