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
from google.cloud.bigquery import SchemaField, DatasetReference
from streamsets import sdk
from streamsets.testframework.markers import gcp, sdc_min_version
from streamsets.testframework.utils import get_random_string
from string import ascii_lowercase

from . import _clean_up_bigquery, _bigquery_get_rows, _bigquery_insert_streaming, _bigquery_insert_dml, \
    _bigquery_create_table

logger = logging.getLogger(__name__)

EXECUTOR_STAGE_NAME = 'com_streamsets_pipeline_stage_bigquery_enterprise_executor_BigQueryDExecutor'
SCHEMA = [SchemaField('title', 'STRING')]

pytestmark = [gcp, sdc_min_version('5.3.0'), pytest.mark.category('nonstandard')]


@pytest.mark.parametrize("queries", [
    ([""]),
    ([" "]),
    (["SELECT * FROM {table}", ""]),
    (["SELECT * FROM {table}", " "]),
])
def test_google_bigquery_wrong_query(sdc_builder, sdc_executor, gcp, queries):
    """ Testing the queries validation. It validates that at least one query
    is provided and there is no blank queries """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data='{"title": "Don Quijote"}',
                                       stop_after_first_batch=True)

    bigquery = pipeline_builder.add_stage(name=EXECUTOR_STAGE_NAME)
    bigquery.set_attributes(query_result_count_in_events=True,
                            sql_queries=queries)
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> bigquery >= wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    try:
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert False
    except sdk.sdc_api.StartError as e:
        assert 'BIGQUERY_EXECUTOR_02' in str(e)


@pytest.mark.parametrize("db_data, query_title, rows_selected", [
    ([{"title": "Don Quijote"}], "Don Quijote", 1),
    ([{"title": "Don Quijote"}], "Sancho Panza", 0),
    ([{"title": "Don Quijote"}, {"title": "El Maestro y Margarita"}], "Don Quijote", 1),
    ([{"title": "Don Quijote"}, {"title": "Don Quijote"}], "Don Quijote", 2),
    ([{"title": "Don Quijote"}, {"title": "El Maestro y Margarita"}], "${record:value('/title')}", 1),
    ([{"title": "Don Quijote"}, {"title": "Don Quijote"}], "${record:value('/title')}", 2),
])
def test_google_bigquery_executor_single_select_query(sdc_builder, sdc_executor, gcp, db_data, query_title,
                                                      rows_selected):
    test_dataset_name = f'stf_dataset_{get_random_string(ascii_lowercase, 10)}'
    test_table_name = f'stf_table_{get_random_string(ascii_lowercase, 10)}'
    input_json_record = {"title": "Don Quijote"}

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=json.dumps(input_json_record),
                                       stop_after_first_batch=True)

    query = f"SELECT * FROM {test_dataset_name}.{test_table_name} WHERE title='{query_title}'"
    bigquery = pipeline_builder.add_stage(name=EXECUTOR_STAGE_NAME)
    bigquery.set_attributes(query_result_count_in_events=True,
                            sql_queries=[query])
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> bigquery >= wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, test_dataset_name)

    try:
        # Creating table and inserting testing data
        table = _bigquery_create_table(bigquery_client, dataset_ref, test_dataset_name, test_table_name, SCHEMA)
        _bigquery_insert_streaming(bigquery_client, table, db_data)

        # Running the pipeline with the select query
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert len(wiretap.output_records) == 1  # Single query
        assert wiretap.output_records[0].header.values["sdc.event.type"] == "successful-query"
        assert wiretap.output_records[0].field['query'] == \
               query.replace("${record:value('/title')}", input_json_record['title'])
        assert wiretap.output_records[0].field['query-result'] == rows_selected
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)


@pytest.mark.parametrize("db_data, old_title, new_title, rows_affected", [
    ([{"title": "Don Quijote"}], "Don Quijote", "Sancho Panza", 1),
    ([{"title": "Don Quijote"}], "Sancho Panza", "Don Quijote", 0),
    ([{"title": "Don Quijote"}, {"title": "Don Quijote"}], "Don Quijote", "Sancho Panza", 2),
    ([{"title": "Don Quijote"}, {"title": "Sancho Panza"}], "Don Quijote", "Sancho Panza", 1),
])
def test_google_bigquery_executor_single_update_query(sdc_builder, sdc_executor, gcp, db_data, old_title, new_title, rows_affected):
    test_dataset_name = f'stf_dataset_{get_random_string(ascii_lowercase, 10)}'
    test_table_name = f'stf_table_{get_random_string(ascii_lowercase, 10)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data='{"title": "Don Quijote"}',
                                       stop_after_first_batch=True)

    query = f"UPDATE {test_dataset_name}.{test_table_name} SET title='{new_title}' WHERE title='{old_title}'"
    bigquery = pipeline_builder.add_stage(name=EXECUTOR_STAGE_NAME)
    bigquery.set_attributes(query_result_count_in_events=True,
                            sql_queries=[query])
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> bigquery >= wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, test_dataset_name)

    try:
        # Creating table and inserting testing data
        _bigquery_create_table(bigquery_client, dataset_ref, test_dataset_name, test_table_name, SCHEMA)
        # BigQuery can not update/delete rows when inserted in streaming mode, so using DML mode
        _bigquery_insert_dml(bigquery_client, f"{test_dataset_name}.{test_table_name}", db_data)

        # Running the pipeline with the select query
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert len(wiretap.output_records) == 1  # Single query
        assert wiretap.output_records[0].header.values["sdc.event.type"] == "successful-query"
        assert wiretap.output_records[0].field['query'] == query
        assert wiretap.output_records[0].field['query-result'] == rows_affected

        # Check whether the database has been updated properly
        rows = _bigquery_get_rows(bigquery_client, f"{test_dataset_name}.{test_table_name}")
        assert rows.total_rows == len(db_data)
        for old_row, new_row in zip(db_data, rows):
            if old_row["title"] == old_title:
                assert new_row["title"] == new_title
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)


def test_google_bigquery_executor_single_error_event(sdc_builder, sdc_executor, gcp):
    """ The error is generated by inserting data using streaming api calls and then try to update this data.
     This will fail since it is not supported to update/delete data when is streaming data """
    test_dataset_name = f'stf_dataset_{get_random_string(ascii_lowercase, 10)}'
    test_table_name = f'stf_table_{get_random_string(ascii_lowercase, 10)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data='{"title": "Don Quijote"}',
                                       stop_after_first_batch=True)

    bigquery = pipeline_builder.add_stage(name=EXECUTOR_STAGE_NAME)
    bigquery.set_attributes(query_result_count_in_events=True,
                            sql_queries=[
                                f"UPDATE {test_dataset_name}.{test_table_name} SET title='Sancho Panza' WHERE title='Don Quijote'"
                            ])
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> bigquery >= wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, test_dataset_name)

    try:
        # Creating table and inserting testing data
        table = _bigquery_create_table(bigquery_client, dataset_ref, test_dataset_name, test_table_name, SCHEMA)
        _bigquery_insert_streaming(bigquery_client, table, [{"title": "Don Quijote"}])

        # Running the pipeline with the select query
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert len(wiretap.output_records) == 1  # Single query
        assert wiretap.output_records[0].header.values['sdc.event.type'] == 'failed-query'
        assert wiretap.output_records[0].field['query'] == f"UPDATE {test_dataset_name}.{test_table_name} SET title='Sancho Panza' WHERE title='Don Quijote'"
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)


def test_google_bigquery_executor_multiple_error_event(sdc_builder, sdc_executor, gcp):
    test_dataset_name = f'stf_dataset_{get_random_string(ascii_lowercase, 10)}'
    test_table_name = f'stf_table_{get_random_string(ascii_lowercase, 10)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data='{"title": "Don Quijote"}',
                                       stop_after_first_batch=True)

    bigquery = pipeline_builder.add_stage(name=EXECUTOR_STAGE_NAME)
    bigquery.set_attributes(query_result_count_in_events=True,
                            sql_queries=[
                                f"UPDATE {test_dataset_name}.{test_table_name} SET title='Sancho Panza' WHERE title='Don Quijote'",
                                f"UPDATE {test_dataset_name}.{test_table_name} SET title='Sancho Panza' WHERE title='Don Quijote'"
                            ])
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> bigquery >= wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, test_dataset_name)

    try:
        # Creating table and inserting testing data
        table = _bigquery_create_table(bigquery_client, dataset_ref, test_dataset_name, test_table_name, SCHEMA)
        _bigquery_insert_streaming(bigquery_client, table, [{"title": "Don Quijote"}])

        # Running the pipeline with the select query
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert len(wiretap.output_records) == 2
        assert wiretap.output_records[0].header.values['sdc.event.type'] == 'failed-query'
        assert wiretap.output_records[0].field['query'] == f"UPDATE {test_dataset_name}.{test_table_name} SET title='Sancho Panza' WHERE title='Don Quijote'"
        assert wiretap.output_records[1].header.values['sdc.event.type'] == 'failed-query'
        assert wiretap.output_records[1].field['query'] == f"UPDATE {test_dataset_name}.{test_table_name} SET title='Sancho Panza' WHERE title='Don Quijote'"

        # The record error is notified twice as Jdbc Query executor.
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter("stage.GoogleBigQuery_01.errorRecords.counter").count == 2
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)


@pytest.mark.parametrize("query_submission", ["SEQUENTIAL", "PARALLEL"])
@pytest.mark.parametrize("db_data, input_records, queries, expected_events, expected_titles", [
    (
            [
                {"book_id": '1', "title": "Don Quijote"},
                {"book_id": '2', "title": "Novelas Ejemplares"}
            ],
            [
                {"cardinal": 0, "title": "La Gitanilla"},
                {"cardinal": 1, "title": "La espanola inglesa"},
                {"cardinal": 2, "title": "La ilustre fregona"}
            ],
            [
                "SELECT * from {table} WHERE book_id='${{record:value(\'/cardinal\')}}' AND title='Don Quijote'",
                "INSERT {table} (book_id, title) VALUES ('${{record:value(\'/cardinal\')}}', '${{record:value(\'/title\')}}')",
                "INSERT nonexisting.table (book_id, title) VALUES ('${{record:value(\'/cardinal\')}}', 'Hamlet')"
            ],
            [
                [
                    # Events for the first record
                    {"sdc.event.type": "successful-query", "query-result": 0,
                     "query": "SELECT * from {table} WHERE book_id='0' AND title='Don Quijote'"},
                    {"sdc.event.type": "successful-query", "query-result": 1,
                     "query": "INSERT {table} (book_id, title) VALUES ('0', 'La Gitanilla')"},
                    {"sdc.event.type": "failed-query",
                     "query": "INSERT nonexisting.table (book_id, title) VALUES ('0', 'Hamlet')"},
                ],
                [
                    # Events for the second record
                    {"sdc.event.type": "successful-query", "query-result": 1,
                     "query": "SELECT * from {table} WHERE book_id='1' AND title='Don Quijote'"},
                    {"sdc.event.type": "successful-query", "query-result": 1,
                     "query": "INSERT {table} (book_id, title) VALUES ('1', 'La espanola inglesa')"},
                    {"sdc.event.type": "failed-query",
                     "query": "INSERT nonexisting.table (book_id, title) VALUES ('1', 'Hamlet')"},
                ],
                [
                    # Events for the third record
                    {"sdc.event.type": "successful-query", "query-result": 0,
                     "query": "SELECT * from {table} WHERE book_id='2' AND title='Don Quijote'"},
                    {"sdc.event.type": "successful-query", "query-result": 1,
                     "query": "INSERT {table} (book_id, title) VALUES ('2', 'La ilustre fregona')"},
                    {"sdc.event.type": "failed-query",
                     "query": "INSERT nonexisting.table (book_id, title) VALUES ('2', 'Hamlet')"},
                ]
            ],
            ["Don Quijote", "Novelas Ejemplares", "La Gitanilla", "La espanola inglesa", "La ilustre fregona"]
    ),
])
def test_google_bigquery_executor_multi_query(sdc_builder, sdc_executor, gcp, db_data, input_records, queries,
                                              expected_events, expected_titles, query_submission):
    test_dataset_name = f'stf_dataset_{get_random_string(ascii_lowercase, 10)}'
    test_table_name = f'stf_table_{get_random_string(ascii_lowercase, 10)}'
    fq_table_name = f'{test_dataset_name}.{test_table_name}'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data="\n".join([json.dumps(r) for r in input_records]),
                                       stop_after_first_batch=True)

    queries = [q.format(table=fq_table_name) for q in queries]
    bigquery = pipeline_builder.add_stage(name=EXECUTOR_STAGE_NAME)
    bigquery.set_attributes(query_result_count_in_events=True,
                            query_submission=query_submission,
                            sql_queries=queries)
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> bigquery >= wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, test_dataset_name)

    try:
        # Creating table and inserting testing data
        table = _bigquery_create_table(bigquery_client, dataset_ref, test_dataset_name, test_table_name,
                                       [SchemaField('book_id', 'STRING'), SchemaField('title', 'STRING')])
        _bigquery_insert_streaming(bigquery_client, table, db_data)

        # Running the pipeline with the select query
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Assert all went well
        expected_events = [[{**e, "query": e["query"].format(table=fq_table_name)} for e in record_expected_events]
                           for record_expected_events in expected_events]
        assert len(wiretap.output_records) == len(sum(expected_events, []))  # All queries on every input record

        output_events = []
        for e in wiretap.output_records:
            output_event = {"sdc.event.type": e.header.values["sdc.event.type"], "query": e.field["query"]}
            if "query-result" in e.field:
                output_event.update({"query-result": e.field["query-result"]})
            output_events.append(output_event)

        if query_submission == "PARALLEL":
            # In case the submission of the queries are in parallel, the events could be generated out of order
            expected_events = sum(expected_events, [])
            for output_event in output_events:
                ee_index = expected_events.index(output_event)
                del expected_events[ee_index]
            assert len(expected_events) == 0
        else:
            # If queries are submitted consecutively, the events must be in order
            for i, record in enumerate(input_records):
                expected_events_for_record = expected_events[i]
                # Assert the expected events appeared in order in the generated events
                events_index = [output_events.index(expected_event) for expected_event in expected_events_for_record]
                # Assert the indexes are strinctly increasing, it means that they were generated in the expected order
                assert all(i < j for i, j in zip(events_index, events_index[1:]))

        # Check inserted titles
        rows = _bigquery_get_rows(bigquery_client, f"{test_dataset_name}.{test_table_name}")
        titles_in_db = [r['title'] for r in rows]

        assert rows.total_rows == len(expected_titles)
        assert set(titles_in_db) == set(expected_titles)
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)


# https://cloud.google.com/bigquery/docs/reference/standard-sql/migrating-from-legacy-sql#function_comparison
@pytest.mark.parametrize("use_legacy_sql, query, expected_fail", [
    (True, "SELECT INTEGER(title) from {dataset_name}.{table_name}", False),
    (False, "SELECT INTEGER(title) from {dataset_name}.{table_name}", True),
    (True, "SELECT SAFE_CAST(title AS INT64) from {dataset_name}.{table_name}", True),
    (False, "SELECT SAFE_CAST(title AS INT64) from {dataset_name}.{table_name}", False),
])
def test_google_bigquery_executor_advanced_option_legacy_sql(sdc_builder, sdc_executor, gcp, use_legacy_sql, query,
                                                             expected_fail):
    test_dataset_name = f'stf_dataset_{get_random_string(ascii_lowercase, 10)}'
    test_table_name = f'stf_table_{get_random_string(ascii_lowercase, 10)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data='{"title": "Don Quijote"}',
                                       stop_after_first_batch=True)

    query = query.format(dataset_name=test_dataset_name, table_name=test_table_name)
    bigquery = pipeline_builder.add_stage(name=EXECUTOR_STAGE_NAME)
    bigquery.set_attributes(query_result_count_in_events=False,
                            use_legacy_sql=use_legacy_sql,
                            sql_queries=[query])
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> bigquery >= wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, test_dataset_name)

    try:
        # Creating table and inserting testing data
        table = _bigquery_create_table(bigquery_client, dataset_ref, test_dataset_name, test_table_name, SCHEMA)
        _bigquery_insert_streaming(bigquery_client, table, [{"title": "10.0"}])

        # Running the pipeline with the select query
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        expected_event_type = "successful-query" if not expected_fail else "failed-query"
        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].header.values["sdc.event.type"] == expected_event_type
        assert wiretap.output_records[0].field['query'] == query
        assert "query-result" not in wiretap.output_records[0].field
    finally:
        _clean_up_bigquery(bigquery_client, dataset_ref)


@pytest.mark.parametrize("include_query_result_count_in_events", [True, False])
def test_google_bigquery_executor_include_query_result_count_in_events(sdc_builder, sdc_executor, gcp,
                                                                       include_query_result_count_in_events):
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
        table = _bigquery_create_table(bigquery_client, dataset_ref, test_dataset_name, test_table_name, SCHEMA)
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


@pytest.mark.parametrize("connection_pool_size, number_of_threads", [(1, 1), (1, 5), (5, 1), (5, 5), (0, 1), (0, 5)])
def test_google_bigquery_executor_connection_pool_size(sdc_builder, sdc_executor, gcp, connection_pool_size,
                                                       number_of_threads):
    dataset_name = f'stf_{get_random_string(ascii_lowercase, 10)}'
    table_name = f'stf_{get_random_string(ascii_lowercase, 10)}'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    data_source = pipeline_builder.add_stage("Dev Data Generator")
    data_source.set_attributes(
        batch_size=2,
        delay_between_batches=1,
        fields_to_generate=[{"field": "data", "type": "STRING"}],
        number_of_threads=number_of_threads
    )

    bigquery_executor = pipeline_builder.add_stage(name=EXECUTOR_STAGE_NAME)
    bigquery_executor.set_attributes(
        connection_pool_size=connection_pool_size,
        sql_queries=[f"INSERT INTO {dataset_name}.{table_name} VALUES (\"${{record:value('/data')}}\")"]
    )

    wiretap = pipeline_builder.add_wiretap()

    data_source >> [bigquery_executor, wiretap.destination]

    pipeline = pipeline_builder.build().configure_for_environment(gcp)

    bigquery_client = gcp.bigquery_client
    dataset_ref = DatasetReference(gcp.project_id, dataset_name)
    try:
        # Create the table in BigQuery with the proper schema
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
