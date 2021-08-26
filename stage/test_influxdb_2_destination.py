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
from streamsets.sdk import sdc_api
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


@pytest.mark.parametrize('auto_create', [True, False])
def test_influxdb2_bucket_creation(sdc_builder, sdc_executor, influxdb2, auto_create):
    """Test for InfluxDB destination stage.

    Using Custom mappings and using parametrizing on the auto create option

    The pipeline looks like:

        dev_raw_data_source >> influxdb_destination
    """
    client = influxdb2.client

    bucket_name = get_random_string()

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
                result_records.append({
                    f"'butterflies': {record['_value']}, 'location': '{record['location']}', 'scientist':  '{record['scientist']}', 'time': {round(record['_time'].replace(tzinfo=timezone.utc).timestamp() * 1000)}"})

        assert len(raw_records) == len(result_records)
        assert [influx_record in raw_records for influx_record in result_records]
    finally:
        influxdb2.drop_bucket(bucket_name)


@pytest.mark.parametrize('write_precision', ['S', 'MS', 'NS', 'US'])
def test_influxdb2_write_precision(sdc_builder, sdc_executor, influxdb2, write_precision):
    """Test for InfluxDB destination stage.

    Using Custom mappings and using parametrizing on the write precision option

    The pipeline looks like:

        dev_raw_data_source >> influxdb_destination
    """
    client = influxdb2.client

    bucket_name = get_random_string()

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=json.dumps(RAW_DATA),
                                       stop_after_first_batch=True)
    influxdb_destination = builder.add_stage('InfluxDB 2.x', type='destination')
    influxdb_destination.set_attributes(bucket=bucket_name,
                                        measurement_field='/measurement',
                                        time_field='/record/time',
                                        write_precision=write_precision,
                                        tag_fields=["/record/location", "/record/scientist"],
                                        value_fields=["/record/butterflies"])

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
                result_records.append({
                    f"'butterflies': {record['_value']}, 'location': '{record['location']}', 'scientist':  '{record['scientist']}', 'time': {round(record['_time'].replace(tzinfo=timezone.utc).timestamp() * 1000)}"})

        assert len(raw_records) == len(result_records)
        assert [influx_record in raw_records for influx_record in result_records]
    finally:
        influxdb2.drop_bucket(bucket_name)


@pytest.mark.parametrize('date_format', ['DATE', 'DATETIME', 'TIME'])
def test_influxdb2_time_format(sdc_builder, sdc_executor, influxdb2, date_format):
    """Test for InfluxDB destination stage.

    Stressing the auto-time field

    The pipeline looks like:

        dev_raw_data_source >> influxdb_destination
    """

    client = influxdb2.client

    bucket_name = get_random_string()

    builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = builder.add_stage('Dev Data Generator')

    dev_data_generator.set_attributes(records_to_be_generated=4,
                                      fields_to_generate=[
                                          {'field': 'measurement', 'type': 'STRING'},
                                          {'field': 'time', 'type': date_format},
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
        raw_records = wiretap.output_records

        query = f'from(bucket: "{bucket_name}") |> range(start: -100y)'

        result = client.query_api().query(query=query, org=influxdb2.organization)

        result_records = []
        for table in result:
            for record in table.records:
                print(record)
                result_records.append(record)
                # result_records.append({f"'butterflies': {record['_value']}, 'location': '{record['location']}', 'scientist':  '{record['scientist']}', 'time': {round(record['_time'].replace(tzinfo=timezone.utc).timestamp() * 1000)}"})

        assert len(raw_records) == len(result_records)
        assert [influx_record in raw_records for influx_record in result_records]
    finally:
        influxdb2.drop_bucket(bucket_name)


@pytest.mark.parametrize('retention_time', [0, 1, 3600])
def test_influxdb2_retention_time(sdc_builder, sdc_executor, influxdb2, retention_time):
    """Test for InfluxDB destination stage.

    Using Custom mappings and using parametrizing on the retention time

    The pipeline looks like:

        dev_raw_data_source >> influxdb_destination
    """
    client = influxdb2.client

    bucket_name = get_random_string()

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=json.dumps(RAW_DATA),
                                       stop_after_first_batch=True)

    influxdb_destination = builder.add_stage('InfluxDB 2.x', type='destination')
    influxdb_destination.set_attributes(create_bucket=True,
                                        bucket=bucket_name,
                                        retention_time=retention_time,
                                        measurement_field='/measurement',
                                        time_field='/record/time',
                                        write_precision='MS',
                                        tag_fields=["/record/location", "/record/scientist"],
                                        value_fields=["/record/butterflies"])

    dev_raw_data_source >> influxdb_destination
    pipeline = builder.build().configure_for_environment(influxdb2)
    sdc_executor.add_pipeline(pipeline)

    try:
        if retention_time == 1:
            with pytest.raises(sdc_api.StartError) as exception:
                sdc_executor.start_pipeline(pipeline)

            assert f'INFLUX_03 - Retention time should be either 0 (infinite) or bigger than 1h. Current value: {retention_time} seconds' in f'{exception.value}'
        else:
            sdc_executor.start_pipeline(pipeline).wait_for_finished()

            # read data from InfluxDB and assert to what pipeline ingested
            raw_records = [item['record'] for item in RAW_DATA]

            query = f'from(bucket: "{bucket_name}") |> range(start: -1d)'

            result = client.query_api().query(query=query, org=influxdb2.organization)

            result_records = []
            for table in result:
                for record in table.records:
                    result_records.append({
                        f"'butterflies': {record['_value']}, 'location': '{record['location']}', 'scientist':  '{record['scientist']}', 'time': {round(record['_time'].replace(tzinfo=timezone.utc).timestamp() * 1000)}"})

            assert len(raw_records) == len(result_records)
            assert [influx_record in raw_records for influx_record in result_records]
    finally:
        if retention_time != 1:
            influxdb2.drop_bucket(bucket_name)


def test_influxdb2_wrong_url(sdc_builder, sdc_executor, influxdb2):
    """Test for InfluxDB destination stage.

    Negative testing with a wrong URL

    The pipeline looks like:

        dev_raw_data_source >> influxdb_destination
    """
    raw_data = [{'measurement': get_random_string(string.ascii_letters, 10)}]

    bucket_name = get_random_string()

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=json.dumps(raw_data),
                                       stop_after_first_batch=True)
    influxdb_destination = builder.add_stage('InfluxDB 2.x', type='destination')
    influxdb_destination.set_attributes(bucket=bucket_name,
                                        create_bucket=True,
                                        measurement_field='/measurement',
                                        write_precision='MS',
                                        tag_fields=["/record/location", "/record/scientist"],
                                        value_fields=["/record/butterflies"])

    dev_raw_data_source >> influxdb_destination
    pipeline = builder.build().configure_for_environment(influxdb2)

    influxdb_destination.set_attributes(url='thisIsNotTheURL')
    sdc_executor.add_pipeline(pipeline)

    with pytest.raises(sdc_api.StartError) as exception:
        sdc_executor.start_pipeline(pipeline)

    assert 'INFLUX_08 - Failed to create InfluxDB client.' in f'{exception.value}'


def test_influxdb2_wrong_token(sdc_builder, sdc_executor, influxdb2):
    """Test for InfluxDB destination stage.

    Negative testing with a wrong token

    The pipeline looks like:

        dev_raw_data_source >> influxdb_destination
    """
    raw_data = [{'measurement': get_random_string(string.ascii_letters, 10)}]

    bucket_name = get_random_string()

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=json.dumps(raw_data),
                                       stop_after_first_batch=True)
    influxdb_destination = builder.add_stage('InfluxDB 2.x', type='destination')
    influxdb_destination.set_attributes(bucket=bucket_name,
                                        create_bucket=True,
                                        measurement_field='/measurement',
                                        write_precision='MS',
                                        tag_fields=["/record/location", "/record/scientist"],
                                        value_fields=["/record/butterflies"])

    dev_raw_data_source >> influxdb_destination
    pipeline = builder.build().configure_for_environment(influxdb2)

    influxdb_destination.set_attributes(token='thisIsNotTheToken')
    sdc_executor.add_pipeline(pipeline)

    with pytest.raises(sdc_api.StartError) as exception:
        sdc_executor.start_pipeline(pipeline)

    assert 'INFLUX_07 - Unable to authenticate using the provided token.' in f'{exception.value}'


def test_influxdb2_non_existing_org(sdc_builder, sdc_executor, influxdb2):
    """Test for InfluxDB destination stage.

    Negative testing with a non existing org

    The pipeline looks like:

        dev_raw_data_source >> influxdb_destination
    """
    raw_data = [{'measurement': get_random_string(string.ascii_letters, 10)}]

    bucket_name = get_random_string()

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=json.dumps(raw_data),
                                       stop_after_first_batch=True)
    influxdb_destination = builder.add_stage('InfluxDB 2.x', type='destination')
    influxdb_destination.set_attributes(bucket=bucket_name,
                                        create_bucket=True,
                                        measurement_field='/measurement',
                                        write_precision='MS',
                                        tag_fields=["/record/location", "/record/scientist"],
                                        value_fields=["/record/butterflies"])

    dev_raw_data_source >> influxdb_destination
    pipeline = builder.build().configure_for_environment(influxdb2)

    influxdb_destination.set_attributes(organization='thisIsNotAnOrg')
    sdc_executor.add_pipeline(pipeline)

    with pytest.raises(sdc_api.StartError) as exception:
        sdc_executor.start_pipeline(pipeline)

    assert "INFLUX_02 - Organization 'thisIsNotAnOrg' not found." in f'{exception.value}'


def test_influxdb2_non_existing_bucket(sdc_builder, sdc_executor, influxdb2):
    """Test for InfluxDB destination stage.

    Negative testing with a non existing bucket

    The pipeline looks like:

        dev_raw_data_source >> influxdb_destination
    """
    raw_data = [{'measurement': get_random_string(string.ascii_letters, 10)}]

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=json.dumps(raw_data),
                                       stop_after_first_batch=True)
    influxdb_destination = builder.add_stage('InfluxDB 2.x', type='destination')
    influxdb_destination.set_attributes(bucket='NonExistingBucket',
                                        create_bucket=False,
                                        measurement_field='/measurement',
                                        write_precision='MS',
                                        tag_fields=["/record/location", "/record/scientist"],
                                        value_fields=["/record/butterflies"])

    dev_raw_data_source >> influxdb_destination
    pipeline = builder.build().configure_for_environment(influxdb2)

    influxdb_destination.set_attributes(organization='thisIsNotAnOrg')
    sdc_executor.add_pipeline(pipeline)

    with pytest.raises(sdc_api.StartError) as exception:
        sdc_executor.start_pipeline(pipeline)

    assert "INFLUX_01 - Bucket 'NonExistingBucket' not found." in f'{exception.value}'


def test_influxdb2_missing_value_field(sdc_builder, sdc_executor, influxdb2):
    """Test for InfluxDB destination stage.

    Negative testing with a missing value field

    The pipeline looks like:

        dev_raw_data_source >> influxdb_destination
    """
    client = influxdb2.client
    bucket_name = get_random_string()

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=json.dumps(RAW_DATA),
                                       stop_after_first_batch=True)
    influxdb_destination = builder.add_stage('InfluxDB 2.x', type='destination')
    influxdb_destination.set_attributes(create_bucket=True,
                                        bucket=bucket_name,
                                        measurement_field='/measurement',
                                        time_field='/record/time',
                                        write_precision='MS',
                                        tag_fields=["/record/location", "/record/scientist"],
                                        value_fields=["/record/bees"])

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
                result_records.append({
                    f"time': {round(record['_time'].replace(tzinfo=timezone.utc).timestamp() * 1000)}"})

        assert 0 == len(result_records)
    finally:
        influxdb2.drop_bucket(bucket_name)


def test_influxdb2_missing_time_field(sdc_builder, sdc_executor, influxdb2):
    """Test for InfluxDB destination stage.

    Missing time field

    The pipeline looks like:

        dev_raw_data_source >> influxdb_destination
    """
    raw_data = [
        {'measurement': get_random_string(string.ascii_letters, 10),
         'record': {'butterflies': 12, 'location': '1', 'scientist': 'langstroth'}},
        {'measurement': get_random_string(string.ascii_letters, 10),
         'record': {'butterflies': 8, 'location': '2', 'scientist': 'perpetua'}},
        {'measurement': get_random_string(string.ascii_letters, 10),
         'record': {'butterflies': 3, 'location': '1', 'scientist': 'perpetua'}},
        {'measurement': get_random_string(string.ascii_letters, 10),
         'record': {'butterflies': 1, 'location': '2', 'scientist': 'langstroth'}}]

    client = influxdb2.client

    bucket_name = get_random_string()

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=json.dumps(raw_data),
                                       stop_after_first_batch=True)
    influxdb_destination = builder.add_stage('InfluxDB 2.x', type='destination')
    influxdb_destination.set_attributes(bucket=bucket_name,
                                        measurement_field='/measurement',
                                        write_precision='MS',
                                        tag_fields=["/record/location", "/record/scientist"],
                                        value_fields=["/record/butterflies"])

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
                result_records.append({
                    f"'butterflies': {record['_value']}, 'location': '{record['location']}', 'scientist':  '{record['scientist']}', 'time': {round(record['_time'].replace(tzinfo=timezone.utc).timestamp() * 1000)}"})

        assert len(raw_records) == len(result_records)
        assert [influx_record in raw_records for influx_record in result_records]
    finally:
        influxdb2.drop_bucket(bucket_name)


def test_influxdb2_missing_tag_field(sdc_builder, sdc_executor, influxdb2):
    """Test for InfluxDB destination stage.

    Stressing the auto-time field

    The pipeline looks like:

        dev_raw_data_source >> influxdb_destination
    """
    client = influxdb2.client

    bucket_name = get_random_string()

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=json.dumps(RAW_DATA),
                                       stop_after_first_batch=True)
    influxdb_destination = builder.add_stage('InfluxDB 2.x', type='destination')
    influxdb_destination.set_attributes(bucket=bucket_name,
                                        measurement_field='/measurement',
                                        write_precision='MS',
                                        time_field='/record/time',
                                        value_fields=["/record/butterflies"])

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
                result_records.append({
                    f"'butterflies': {record['_value']}', 'time': {round(record['_time'].replace(tzinfo=timezone.utc).timestamp() * 1000)}"})

        assert len(raw_records) == len(result_records)
        assert [influx_record in raw_records for influx_record in result_records]
    finally:
        influxdb2.drop_bucket(bucket_name)
