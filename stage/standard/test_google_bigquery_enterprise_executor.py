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

import json
import logging
import pytest
from decimal import Decimal
from google.cloud.bigquery import SchemaField, DatasetReference
from streamsets.testframework.utils import get_random_string
from string import ascii_lowercase

from streamsets.testframework.markers import gcp, sdc_min_version, sdc_enterprise_lib_min_version

from .. import _clean_up_bigquery, _bigquery_insert_dml, _bigquery_insert_streaming, _bigquery_create_table

logger = logging.getLogger(__name__)
EXECUTOR_STAGE_NAME = 'com_streamsets_pipeline_stage_bigquery_enterprise_executor_BigQueryDExecutor'

pytestmark = [gcp, sdc_min_version('5.3.0'), pytest.mark.category('standard'), ]

# https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
# https://docs.streamsets.com/portal/#datacollector/latest/help/datacollector/UserGuide/Destinations/BigQuery.html
# For every data type supported
@pytest.mark.parametrize('origin_data_type, gcp_data_type, origin_data, gcp_expected_data', [
    # (DECIMAL is an alias for NUMERIC in GCP - BIGDECIMAL for BIGNUMERIC)
    # Boolean
    ('BOOLEAN', 'BOOLEAN', True, True),
    # Byte Array
    ('BYTE_ARRAY', 'BYTES', 'dataAsBytes', b'dataAsBytes'),
    ('BYTE_ARRAY', 'STRING', 'dataAsBytes', 'ZGF0YUFzQnl0ZXM='),
    # Date
    # For the next two: When date is casted to string to build the sql statement, the format is not valid for BigQuery
    # ('DATE', 'DATE', '2020-01-01 10:00:00', datetime.date(2020, 1, 1)),
    # ('DATE', 'DATETIME', '2020-01-01 10:00:00', datetime.datetime(2020, 1, 1, 10)),
    ('DATE', 'STRING', '2020-01-01 10:00:00', 'Wed Jan 01 10:00:00 GMT 2020'),
    ## Datetime
    # ('DATETIME', 'DATETIME', '2019-02-05 23:59:59', datetime.datetime(2019, 2, 5, 23, 59, 59)),
    # ('DATETIME', 'TIMESTAMP', '2007-05-28 07:52:31 UTC', datetime.datetime(2007, 5, 28, 7, 52, 31, tzinfo=UTC)),
    ('DATETIME', 'STRING', '2019-02-05 23:59:59', 'Tue Feb 05 23:59:59 GMT 2019'),
    # Time
    # ('TIME', 'TIME', '2020-01-01 10:00:00',  datetime.time(10, 0)),
    # Double
    ('DOUBLE', 'FLOAT', 2424.2424, 2424.2424),
    ('DOUBLE', 'STRING', 2424.2424, '2424.2424'),
    ('DOUBLE', 'DECIMAL', -123456789.12345, Decimal('-123456789.12345')),
    ('DOUBLE', 'BIGDECIMAL', -123456789.12345, '-123456789.12345'),
    # Float
    ('FLOAT', 'FLOAT', 2424.2424, 2424.2424),
    ('FLOAT', 'STRING', 2424.2424, '2424.2424'),
    ('FLOAT', 'DECIMAL', 2424.2424, Decimal('2424.2424')),
    ('FLOAT', 'BIGDECIMAL', 2424.2424, '2424.2424'),
    # Long
    ('LONG', 'INTEGER', 2424, 2424),
    ('LONG', 'STRING', 2424, '2424'),
    ('LONG', 'DECIMAL', 2424, 2424),
    ('LONG', 'BIGDECIMAL', -123456789, '-123456789'),
    # Integer
    ('INTEGER', 'INTEGER', 2424, 2424),
    ('INTEGER', 'STRING', 2424, '2424'),
    ('INTEGER', 'DECIMAL', 2424, Decimal('2424')),
    ('INTEGER', 'BIGDECIMAL', 2424, '2424'),
    # Decimal
    ('DECIMAL', 'DECIMAL', -123456789.12345, Decimal('-123456789.12345')),
    ('DECIMAL', 'BIGDECIMAL', -123456789.12345, '-123456789.12345'),
    ('DECIMAL', 'STRING', -123456789.12345, '-123456789.12345000000000000000000000000000000000'),
    # String
    ('STRING', 'STRING', 'gcp standard test 123', 'gcp standard test 123'),
    # ('STRING', 'DATE', '2020-01-01', datetime.date(2020, 1, 1)),
    # ('STRING', 'DATETIME', '2003-04-12 04:05:06', datetime.datetime(2003, 4, 12, 4, 5, 6)),
    ('STRING', 'FLOAT', '2424.2424', 2424.2424),
    ('STRING', 'INTEGER', '2424', 2424),
    ('STRING', 'DECIMAL', '-123456789.12345', Decimal('-123456789.12345')),
    ('STRING', 'BIGDECIMAL', '-123456789.1234512345123451234512345', '-123456789.1234512345123451234512345'),
])
def test_data_types(sdc_builder, sdc_executor, gcp, origin_data_type, gcp_data_type, origin_data, gcp_expected_data):
    dataset_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name = f'stf_{get_random_string(ascii_lowercase, 10)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       stop_after_first_batch=True,
                                       raw_data=json.dumps({"data": origin_data}))

    field_type_converter = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter.conversion_method = 'BY_FIELD'
    field_type_converter.field_type_converter_configs = [{
        'fields': ['/data'],
        'targetType': origin_data_type,
        'dataLocale': 'en,US',
        'dateFormat': 'YYYY_MM_DD_HH_MM_SS',
        'zonedDateTimeFormat': 'ISO_OFFSET_DATE_TIME',
        'scale': 38
    }]

    # How the value will be inserted into the query depends on it's type. Normal values are inserted
    # raw, byte arrays and binaries are encoded, and date, time or character related types are enquoted.
    value_in_query = "${record:value('/data')}"
    if origin_data_type == "BYTE_ARRAY":
        value_in_query = "${base64:encodeBytes(record:value('/data'), false)}"

    if "STRING" in gcp_data_type or "DATE" in gcp_data_type or "TIME" in gcp_data_type:
        value_in_query = f"'{value_in_query}'"

    if gcp_data_type == "BYTES":
        value_in_query = f"CAST('{value_in_query}' AS BYTES FORMAT 'BASE64M')"

    query = f"INSERT INTO {dataset_name}.{table_name} VALUES ({value_in_query})"
    bigquery_executor = pipeline_builder.add_stage(name=EXECUTOR_STAGE_NAME)
    bigquery_executor.set_attributes(sql_queries=[query])

    dev_raw_data_source >> field_type_converter >> bigquery_executor

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)
    try:
        # Create the table in BigQuery with the proper schema
        table = _bigquery_create_table(bigquery_client, dataset_ref, dataset_name, table_name,
                                       [SchemaField('data', gcp_data_type)])

        # Start pipeline, the executor will insert the data
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        data_from_bigquery = [row.values() for row in bigquery_client.list_rows(table)]
        data_from_bigquery.sort()

        assert len(data_from_bigquery) == 1
        assert data_from_bigquery[0][0] == gcp_expected_data
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)


# https://cloud.google.com/bigquery/docs/tables#table_naming
@pytest.mark.parametrize('use_legacy_sql', [True, False])
@pytest.mark.parametrize('table_name', [
    'table-01',  # kind of standard name
    '92TABLE',  # begin with numeric characters
    'myTABLE_upperANDlowerCaSEs',  # case sensitiveness
    ' EVEN THIS AND THAT',  # allowed blank spaces even at the start
    'ग्राहक',  # more characters
    '00_お客様',  # more characters
    'étudiant'  # accents
])
def test_object_names(sdc_builder, sdc_executor, gcp, table_name, use_legacy_sql):
    """ In this case we are escaping the table names by using ` as the user would do. Here we are testing that there is
    no internal manipulation of the query that makes the queries with properly escaped table names fail """
    dataset_name = f'stf_{get_random_string(ascii_lowercase, 10)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       stop_after_first_batch=True,
                                       raw_data=json.dumps({"data": "irrelevant"}))

    if use_legacy_sql:
        escaped_table_name = f"[{dataset_name}.{table_name}]"
    else:
        escaped_table_name = f"`{dataset_name}.{table_name}`"


    query = f"SELECT * FROM {escaped_table_name}"
    bigquery_executor = pipeline_builder.add_stage(name=EXECUTOR_STAGE_NAME)
    bigquery_executor.set_attributes(query_result_count_in_events=True,
                                     use_legacy_sql=use_legacy_sql,
                                     sql_queries=[query])

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> bigquery_executor >= wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)
    try:
        # Create the table in BigQuery
        table = _bigquery_create_table(bigquery_client, dataset_ref, dataset_name, table_name,
                                       [SchemaField('data', 'STRING')])
        _bigquery_insert_dml(bigquery_client, f"`{dataset_name}.{table_name}`", [{"data": "some_string"}])

        # Start pipeline, the executor will insert the data
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Assert the result
        assert len(wiretap.output_records) == 1  # Single query
        assert wiretap.output_records[0].header.values["sdc.event.type"] == "successful-query"
        assert wiretap.output_records[0].field['query-result'] == 1
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)


@pytest.mark.parametrize("include_query_result_count_in_events", [True, False])
def test_dataflow_event_successful_query(sdc_builder, sdc_executor, gcp, include_query_result_count_in_events):
    test_dataset_name = f'stf_dataset_{get_random_string(ascii_lowercase, 10)}'
    test_table_name = f'stf_table_{get_random_string(ascii_lowercase, 10)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data='{"title": "Don Quijote"}',
                                       stop_after_first_batch=True)

    query = f"SELECT * from {test_dataset_name}.{test_table_name}"
    bigquery = pipeline_builder.add_stage(name=EXECUTOR_STAGE_NAME)
    bigquery.set_attributes(query_result_count_in_events=include_query_result_count_in_events,
                            sql_queries=[query])
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> bigquery >= wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, test_dataset_name)

    try:
        # Creating table and inserting testing data
        table = _bigquery_create_table(bigquery_client, dataset_ref, test_dataset_name, test_table_name,
                                       [SchemaField('title', 'STRING')])
        _bigquery_insert_streaming(bigquery_client, table, [{"title": "Don Quijote"}])

        # Running the pipeline with the select query
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].header.values["sdc.event.type"] == "successful-query"
        assert wiretap.output_records[0].field['query'] == query
        assert ("query-result" in wiretap.output_records[0].field) == include_query_result_count_in_events
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)


def test_dataflow_event_failed_query(sdc_builder, sdc_executor, gcp):
    test_dataset_name = f'stf_dataset_{get_random_string(ascii_lowercase, 10)}'

    # Unescaped '-' character will make the query fail
    test_table_name = f'stf_table_{get_random_string(ascii_lowercase, 10)}-test'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data='{"title": "Don Quijote"}',
                                       stop_after_first_batch=True)

    query = f"SELECT * from {test_dataset_name}.{test_table_name}"
    bigquery = pipeline_builder.add_stage(name=EXECUTOR_STAGE_NAME)
    bigquery.set_attributes(sql_queries=[query])
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> bigquery >= wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, test_dataset_name)

    try:
        # Creating table and inserting testing data
        table = _bigquery_create_table(bigquery_client, dataset_ref, test_dataset_name, test_table_name,
                                       [SchemaField('title', 'STRING')])
        _bigquery_insert_streaming(bigquery_client, table, [{"title": "Don Quijote"}])

        # Running the pipeline with the select query
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].header.values["sdc.event.type"] == "failed-query"
        assert wiretap.output_records[0].field['query'] == query
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)


def test_multiple_batches(sdc_builder, sdc_executor, gcp):
    test_dataset_name = f'stf_dataset_{get_random_string(ascii_lowercase, 10)}'
    test_table_name = f'stf_table_{get_random_string(ascii_lowercase, 10)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = pipeline_builder.add_stage("Dev Data Generator")
    dev_data_generator.set_attributes(batch_size=10,
                                      delay_between_batches=1,
                                      fields_to_generate=[
                                          {"field": "title", "type": "APP_NAME"},
                                      ])

    query = f"INSERT {test_dataset_name}.{test_table_name} (title) VALUES (\"${{record:value('/title')}}\") "
    bigquery = pipeline_builder.add_stage(name=EXECUTOR_STAGE_NAME)
    bigquery.set_attributes(query_result_count_in_events=True,
                            sql_queries=[query])
    wiretap = pipeline_builder.add_wiretap()
    wiretap_events = pipeline_builder.add_wiretap()

    dev_data_generator >> [wiretap.destination, bigquery]
    bigquery >= wiretap_events.destination

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, test_dataset_name)

    try:
        # Creating table
        table = _bigquery_create_table(bigquery_client, dataset_ref, test_dataset_name, test_table_name,
                                       [SchemaField('title', 'STRING')])

        # Running the pipeline
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(5, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline=pipeline)

        data_generated = [r.field["title"].value for r in wiretap.output_records]
        data_from_bigquery = [row.values()[0] for row in bigquery_client.list_rows(table)]

        assert len(data_from_bigquery) == len(data_generated)
        assert sorted(data_from_bigquery) == sorted(data_generated)
        assert all([r.field["query-result"] == 1 and r.header.values["sdc.event.type"] == "successful-query"
                    for r in wiretap_events.output_records])

    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)


def test_data_formats(sdc_builder, sdc_executor, gcp):
    pytest.skip("BigQuery Executor does not use a data format library.")


def test_multithreading(sdc_builder, sdc_executor, gcp):
    pytest.skip("BigQuery Executor does not use multithreading")


@pytest.mark.parametrize("origin_name, origin_config", [
    (
            "Dev Raw Data Source",
            {
                "data_format": "JSON",
                "raw_data": json.dumps({"data": "some_value"})
            }
    ),  # Pull
    (
            "Dev Data Generator",
            {
                "batch_size": 2,
                "delay_between_batches": 1,
                "fields_to_generate": [{"field": "data", "type": "STRING"}],
                "number_of_threads": 1
            }
    ),  # Push
    (
            "Dev Data Generator",
            {
                "batch_size": 2,
                "delay_between_batches": 1,
                "fields_to_generate": [{"field": "data", "type": "STRING"}],
                "number_of_threads": 2
            }
    ),  # Push
    (
            "Dev Data Generator",
            {
                "batch_size": 2,
                "delay_between_batches": 1,
                "fields_to_generate": [{"field": "data", "type": "STRING"}],
                "number_of_threads": 4
            }
    ),  # Push
])
def test_push_pull(sdc_builder, sdc_executor, gcp, origin_name, origin_config):
    dataset_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name = f'stf_{get_random_string(ascii_lowercase, 10)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    data_source = pipeline_builder.add_stage(origin_name)
    data_source.set_attributes(**origin_config)

    bigquery_executor = pipeline_builder.add_stage(name=EXECUTOR_STAGE_NAME)
    bigquery_executor.set_attributes(
        sql_queries=[f"INSERT INTO {dataset_name}.{table_name} VALUES (\"${{record:value('/data')}}\")"])

    wiretap = pipeline_builder.add_wiretap()

    data_source >> [bigquery_executor, wiretap.destination]

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)
    try:
        # Create the table
        table = _bigquery_create_table(bigquery_client, dataset_ref, dataset_name, table_name,
                                       [SchemaField('data', 'STRING')])

        # Running the pipeline
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(5, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline=pipeline)

        # Assert all went well
        data_from_bigquery = [row.values()[0] for row in bigquery_client.list_rows(table)]
        data_from_wiretap = [r.field["data"].value for r in wiretap.output_records]

        assert len(data_from_bigquery) == len(wiretap.output_records)
        assert sorted(data_from_bigquery) == sorted(data_from_wiretap)
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)


def test_start():
    pytest.skip("BigQuery executor in not used in Start events.")


def test_stop():
    pytest.skip("BigQuery executor is not used in Stop events.")
