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
import pytest
import string

from collections import Counter
from couchbase.management.buckets import CreateBucketSettings
from streamsets.testframework.markers import couchbase, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

# reference: https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/datatypes.html#objects
# Couchbase Lookup Processor can only read from JSON documents ('Objects' in the above reference),
# hence we are testing that each JSON data type is converted to the correct SDC_Record data type when looked up
DATA_TYPES = [
    (None, 'null', 'STRING', None),
    (True, 'boolean', 'BOOLEAN', True),
    (120, 'integer number', 'INTEGER', 120),
    (20.1, 'decimal number', 'DOUBLE', 20.1),
    ('string', 'string', 'STRING', 'string'),
]


@couchbase
@pytest.mark.parametrize('input,test_name,expected_type,expected_value', DATA_TYPES, ids=[i[1] for i in DATA_TYPES])
def test_data_types_kv(sdc_builder, sdc_executor, couchbase, input, test_name, expected_type, expected_value):
    bucket_name = get_random_string(string.ascii_letters, 10).lower()
    key = 'mydocid'
    doc = {'id': key, 'data': input}
    raw_dict = dict(id=key)
    raw_data = json.dumps(raw_dict)
    cluster = couchbase.cluster

    try:
        # populate the database
        logger.info('Creating %s Couchbase bucket ...', bucket_name)
        couchbase.bucket_manager.create_bucket(CreateBucketSettings(name=bucket_name,
                                                                    bucket_type='couchbase',
                                                                    ram_quota_mb=256))
        couchbase.wait_for_healthy_bucket(bucket_name)

        bucket = cluster.bucket(bucket_name)
        bucket.upsert(key, doc)

        # build the pipeline
        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('Dev Raw Data Source')
        origin.set_attributes(data_format='JSON',
                              stop_after_first_batch=True,
                              raw_data=raw_data)

        lookup = builder.add_stage('Couchbase Lookup')
        lookup.set_attributes(authentication_mode='USER', bucket=bucket_name,
                              lookup_type='KV', document_key='${record:value("/id")}', sdc_field='/output',
                              missing_value_behavior='ERROR')

        wiretap = builder.add_wiretap()

        origin >> lookup >> wiretap.destination

        pipeline = builder.build().configure_for_environment(couchbase)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        output_records = wiretap.output_records
        assert len(output_records) == 1
        assert output_records[0].field['output']['data'].type == expected_type
        assert output_records[0].field['output']['data'] == expected_value
    finally:
        try:
            logger.info('Deleting %s Couchbase bucket ...', bucket_name)
            couchbase.bucket_manager.drop_bucket(bucket_name)
        except Exception as e:
            logger.error(f"Can't delete bucket: {e}")


@couchbase
@sdc_min_version('4.2.0')
@pytest.mark.parametrize('input,test_name,expected_type,expected_value', DATA_TYPES, ids=[i[1] for i in DATA_TYPES])
def test_data_types_query(sdc_builder, sdc_executor, couchbase, input, test_name, expected_type, expected_value):
    if input is None:
        pytest.skip('Lookup Processor queries deal with null values as errors rather than treating them as values')
    bucket_name = get_random_string(string.ascii_letters, 10).lower()
    key = 'mydocid'
    doc = {'id': key, 'data': input}
    raw_dict = dict(id=key)
    raw_data = json.dumps(raw_dict)
    query = f'SELECT data FROM {bucket_name} WHERE ' + 'id="${record:value("/id")}"'
    cluster = couchbase.cluster

    try:
        # populate the database
        logger.info('Creating %s Couchbase bucket ...', bucket_name)
        couchbase.bucket_manager.create_bucket(CreateBucketSettings(name=bucket_name,
                                                                    bucket_type='couchbase',
                                                                    ram_quota_mb=256))
        couchbase.wait_for_healthy_bucket(bucket_name)

        bucket = cluster.bucket(bucket_name)
        bucket.upsert(key, doc)
        cluster.query(f'CREATE PRIMARY INDEX ON `{bucket_name}`').execute()

        # build the pipeline
        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('Dev Raw Data Source')
        origin.set_attributes(data_format='JSON',
                              stop_after_first_batch=True,
                              raw_data=raw_data)

        lookup = builder.add_stage('Couchbase Lookup')
        lookup.set_attributes(authentication_mode='USER', bucket=bucket_name,
                              lookup_type='N1QL', n1ql_query=query,
                              n1ql_mappings=[dict(property='data', sdcField='/output')],
                              missing_value_behavior='ERROR')

        wiretap = builder.add_wiretap()

        origin >> lookup >> wiretap.destination

        pipeline = builder.build().configure_for_environment(couchbase)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        output_records = wiretap.output_records

        assert len(output_records) == 1
        assert output_records[0].field['output'].type == expected_type
        assert output_records[0].field['output'] == expected_value
    finally:
        try:
            logger.info('Deleting %s Couchbase bucket ...', bucket_name)
            couchbase.bucket_manager.drop_bucket(bucket_name)
        except Exception as e:
            logger.error(f"Can't delete bucket: {e}")


# Reference: https://docs.couchbase.com/server/current/manage/manage-buckets/create-bucket.html
# We haven't included the percent test since Couchbase itself has problems creating an index on buckets with %
OBJECT_NAMES = [
    ('maxsize', get_random_string(string.ascii_lowercase, 100)),
    ('lowercase', get_random_string(string.ascii_lowercase)),
    ('uppercase', get_random_string(string.ascii_uppercase)),
    ('hyphen', get_random_string(string.ascii_lowercase) + '-' + get_random_string(string.ascii_lowercase)),
    ('period', get_random_string(string.ascii_lowercase) + '.' + get_random_string(string.ascii_lowercase)),
    ('underscore', get_random_string(string.ascii_lowercase) + '_' + get_random_string(string.ascii_lowercase)),
    # ('percent', get_random_string(string.ascii_lowercase) + '%' + get_random_string(string.ascii_lowercase)),
    ('digits', get_random_string(string.digits))
]


@couchbase
@pytest.mark.parametrize('test_name,bucket_name', OBJECT_NAMES, ids=[i[0] for i in OBJECT_NAMES])
def test_object_names_bucket_kv(sdc_builder, sdc_executor, couchbase, test_name, bucket_name):
    document_key_field = 'mydocname'
    key = 'mydocid'
    doc = {"data": "hello", document_key_field: key}
    raw_dict = dict(id=key)
    raw_data = json.dumps(raw_dict)
    cluster = couchbase.cluster

    try:
        # populate the database
        logger.info('Creating %s Couchbase bucket ...', bucket_name)
        couchbase.bucket_manager.create_bucket(CreateBucketSettings(name=bucket_name,
                                                                    bucket_type='couchbase',
                                                                    ram_quota_mb=256))
        couchbase.wait_for_healthy_bucket(bucket_name)

        bucket = cluster.bucket(bucket_name.replace('%', '%25'))
        bucket.upsert(key, doc)

        # build the pipeline
        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('Dev Raw Data Source')
        origin.set_attributes(data_format='JSON',
                              stop_after_first_batch=True,
                              raw_data=raw_data)

        lookup = builder.add_stage('Couchbase Lookup')
        lookup.set_attributes(authentication_mode='USER', bucket=bucket_name,
                              lookup_type='KV', document_key='${record:value("/id")}', sdc_field='/output',
                              missing_value_behavior='ERROR')

        wiretap = builder.add_wiretap()

        origin >> lookup >> wiretap.destination

        pipeline = builder.build().configure_for_environment(couchbase)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        output_records = wiretap.output_records
        assert len(output_records) == 1, 'Number of returned records should equal the number of matching records stored'
        assert output_records[0].field['output'] == doc
    finally:
        try:
            logger.info('Deleting %s Couchbase bucket ...', bucket_name)
            couchbase.bucket_manager.drop_bucket(bucket_name)
        except Exception as e:
            logger.error(f"Can't delete bucket: {e}")


@couchbase
@sdc_min_version('4.2.0')
@pytest.mark.parametrize('test_name,bucket_name', OBJECT_NAMES, ids=[i[0] for i in OBJECT_NAMES])
def test_object_names_bucket_query(sdc_builder, sdc_executor, couchbase, test_name, bucket_name):
    document_key_field = 'mydocname'
    key = 'mydocid'
    doc = {"data": "hello", document_key_field: key}
    raw_dict = dict(id=key)
    raw_data = json.dumps(raw_dict)
    query = f'SELECT * FROM `{bucket_name}` WHERE {document_key_field}=' + '"${record:value("/id")}"'
    cluster = couchbase.cluster

    try:
        # populate the database
        logger.info('Creating %s Couchbase bucket ...', bucket_name)
        couchbase.bucket_manager.create_bucket(CreateBucketSettings(name=bucket_name,
                                                                    bucket_type='couchbase',
                                                                    ram_quota_mb=256))
        couchbase.wait_for_healthy_bucket(bucket_name)

        bucket = cluster.bucket(bucket_name)
        bucket.upsert(key, doc)
        cluster.query(f'CREATE PRIMARY INDEX ON `{bucket_name}`').execute()

        # build the pipeline
        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('Dev Raw Data Source')
        origin.set_attributes(data_format='JSON',
                              stop_after_first_batch=True,
                              raw_data=raw_data)

        lookup = builder.add_stage('Couchbase Lookup')
        lookup.set_attributes(authentication_mode='USER', bucket=bucket_name,
                              lookup_type='N1QL', n1ql_query=query,
                              n1ql_mappings=[dict(property=bucket_name, sdcField='/output')],
                              missing_value_behavior='ERROR')

        wiretap = builder.add_wiretap()

        origin >> lookup >> wiretap.destination

        pipeline = builder.build().configure_for_environment(couchbase)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        output_records = wiretap.output_records
        assert len(output_records) == 1, 'Number of returned records should equal the number of matching records stored'
        assert output_records[0].field['output'] == doc
    finally:
        try:
            logger.info('Deleting %s Couchbase bucket ...', bucket_name)
            couchbase.bucket_manager.drop_bucket(bucket_name)
        except Exception as e:
            logger.error(f"Can't delete bucket: {e}")


@couchbase
@pytest.mark.parametrize('batch_size', [1, 10])
def test_multiple_batches_kv(sdc_builder, sdc_executor, couchbase, batch_size):
    bucket_name = get_random_string(string.ascii_letters, 10).lower()
    docs = [{"id": "1", "data": 10},
            {"id": "2", "data": 20},
            {"id": "3", "data": 30}]
    batches = 3
    cluster = couchbase.cluster

    # populate the database
    logger.info('Creating %s Couchbase bucket ...', bucket_name)
    couchbase.bucket_manager.create_bucket(CreateBucketSettings(name=bucket_name,
                                                                bucket_type='couchbase',
                                                                ram_quota_mb=256))
    couchbase.wait_for_healthy_bucket(bucket_name)

    bucket = cluster.bucket(bucket_name)
    for doc in docs:
        bucket.upsert(doc["id"], doc)

    # build the pipeline
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Dev Data Generator')
    origin.fields_to_generate = [{
        "type": "LONG_SEQUENCE",
        "field": "seq"
    }]

    expression = builder.add_stage('Expression Evaluator')
    expression.field_expressions = [{
        'fieldToSet': '/lookup',
        'expression': '${record:value("/seq") % 3 + 1}'
    }]

    lookup = builder.add_stage('Couchbase Lookup')
    lookup.set_attributes(authentication_mode='USER', bucket=bucket_name,
                          lookup_type='KV', document_key='${record:value("/lookup")}', sdc_field='/output',
                          missing_value_behavior='PASS')

    wiretap = builder.add_wiretap()

    origin >> expression >> lookup >> wiretap.destination

    pipeline = builder.build().configure_for_environment(couchbase)
    sdc_executor.add_pipeline(pipeline)

    try:
        # run the pipeline
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(batches * batch_size)
        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        record_count = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        logger.info(f"Wrote {record_count} records")

        records = wiretap.output_records
        assert len(records) == record_count

        # Verify each record
        def sort_func(entry):
            return entry.field['seq'].value

        records.sort(key=sort_func)

        expected_number = 0
        for record in records:
            assert record.field['seq'] == expected_number
            assert record.field['lookup'] == expected_number % 3 + 1
            assert record.field['output']['data'] == (expected_number % 3 + 1) * 10

            expected_number = expected_number + 1
    finally:
        try:
            logger.info('Deleting %s Couchbase bucket ...', bucket_name)
            couchbase.bucket_manager.drop_bucket(bucket_name)
        except Exception as e:
            logger.error(f"Can't delete bucket: {e}")


@couchbase
@sdc_min_version('4.2.0')
@pytest.mark.parametrize('batch_size', [1, 10])
def test_multiple_batches_query(sdc_builder, sdc_executor, couchbase, batch_size):
    bucket_name = get_random_string(string.ascii_letters, 10).lower()
    docs = [{"id": "1", "data": 10},
            {"id": "2", "data": 20},
            {"id": "3", "data": 30}]
    batches = 3
    query = f'SELECT data FROM {bucket_name} WHERE ' + 'id="${record:value("/lookup")}"'
    cluster = couchbase.cluster

    # populate the database
    logger.info('Creating %s Couchbase bucket ...', bucket_name)
    couchbase.bucket_manager.create_bucket(CreateBucketSettings(name=bucket_name,
                                                                bucket_type='couchbase',
                                                                ram_quota_mb=256))
    couchbase.wait_for_healthy_bucket(bucket_name)

    bucket = cluster.bucket(bucket_name)
    for doc in docs:
        bucket.upsert(doc["id"], doc)
    cluster.query(f'CREATE PRIMARY INDEX ON `{bucket_name}`').execute()

    # build the pipeline
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Dev Data Generator')
    origin.fields_to_generate = [{
        "type": "LONG_SEQUENCE",
        "field": "seq"
    }]

    expression = builder.add_stage('Expression Evaluator')
    expression.field_expressions = [{
        'fieldToSet': '/lookup',
        'expression': '${record:value("/seq") % 3 + 1}'
    }]

    lookup = builder.add_stage('Couchbase Lookup')
    lookup.set_attributes(authentication_mode='USER', bucket=bucket_name,
                          lookup_type='N1QL', n1ql_query=query,
                          n1ql_mappings=[dict(property='data', sdcField='/output')],
                          missing_value_behavior='PASS')

    wiretap = builder.add_wiretap()

    origin >> expression >> lookup >> wiretap.destination

    pipeline = builder.build().configure_for_environment(couchbase)
    sdc_executor.add_pipeline(pipeline)

    try:
        # run the pipeline
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(batches * batch_size)
        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        record_count = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        logger.info(f"Wrote {record_count} records")

        records = wiretap.output_records
        assert len(records) == record_count

        # Verify each record
        def sort_func(entry):
            return entry.field['seq'].value

        records.sort(key=sort_func)

        expected_number = 0
        for record in records:
            assert record.field['seq'] == expected_number
            assert record.field['lookup'] == expected_number % 3 + 1
            assert record.field['output'] == (expected_number % 3 + 1) * 10

            expected_number = expected_number + 1
    finally:
        try:
            logger.info('Deleting %s Couchbase bucket ...', bucket_name)
            couchbase.bucket_manager.drop_bucket(bucket_name)
        except Exception as e:
            logger.error(f"Can't delete bucket: {e}")


@couchbase
def test_dataflow_events(sdc_builder, sdc_executor, couchbase):
    pytest.skip("No events supported in Couchbase Lookup Processor at this time.")


@couchbase
def test_data_format(sdc_builder, sdc_executor, couchbase):
    pytest.skip("Couchbase Lookup Processor doesn't deal with data formats")


LOOKUPS = [
    ('no matching, error', 'id2', [], [{'id': 'id2'}], 'ERROR'),
    ('no matching, pass', 'id2', [{'id': 'id2'}], [], 'PASS'),
    ('one matching', 'id1', [{'id': 'id1', 'output': {'id': 'id1', 'data': 'hello'}}], [], 'ERROR')
]


@couchbase
@pytest.mark.parametrize('test_name,input,expected_out,expected_error,missing_value_behavior',
                         LOOKUPS, ids=[i[0] for i in LOOKUPS])
def test_lookup_kv(sdc_builder, sdc_executor, couchbase,
                   test_name, input, expected_out, expected_error, missing_value_behavior):
    bucket_name = get_random_string(string.ascii_letters, 10).lower()
    doc = {'id': 'id1', 'data': 'hello'}
    raw_dict = dict(id=input)
    raw_data = json.dumps(raw_dict)
    cluster = couchbase.cluster

    try:
        # populate the database
        logger.info('Creating %s Couchbase bucket ...', bucket_name)
        couchbase.bucket_manager.create_bucket(CreateBucketSettings(name=bucket_name,
                                                                    bucket_type='couchbase',
                                                                    ram_quota_mb=256))
        couchbase.wait_for_healthy_bucket(bucket_name)

        bucket = cluster.bucket(bucket_name)
        bucket.upsert(doc['id'], doc)

        # build the pipeline
        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('Dev Raw Data Source')
        origin.set_attributes(data_format='JSON',
                              stop_after_first_batch=True,
                              raw_data=raw_data)

        lookup = builder.add_stage('Couchbase Lookup')
        lookup.set_attributes(authentication_mode='USER', bucket=bucket_name,
                              lookup_type='KV', document_key='${record:value("/id")}', sdc_field='/output',
                              missing_value_behavior=missing_value_behavior)

        wiretap = builder.add_wiretap()

        origin >> lookup >> wiretap.destination

        pipeline = builder.build().configure_for_environment(couchbase)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        output_records = wiretap.output_records
        error_records = wiretap.error_records

        assert len(output_records) == len(expected_out)
        assert len(error_records) == len(expected_error)
        if expected_out:
            assert output_records[0].field == expected_out[0]
        if expected_error:
            assert error_records[0].field == expected_error[0]
    finally:
        try:
            logger.info('Deleting %s Couchbase bucket ...', bucket_name)
            couchbase.bucket_manager.drop_bucket(bucket_name)
        except Exception as e:
            logger.error(f"Can't delete bucket: {e}")


QUERIES = [
    ('no matching, error', 'data="goodbye"', [], 'FIRST', 'ERROR'),
    ('no matching, pass', 'data="goodbye"', [], 'FIRST', 'PASS'),
    ('one matching', 'id="id1"', ['id1'], 'FIRST', 'PASS'),
    ('multi matching, first result', 'data="hello"', ['id1'], 'FIRST', 'PASS'),
    ('multi matching, multi results', 'data="hello"', ['id1', 'id2', 'id3'], 'MULTI', 'PASS')
]


@couchbase
@sdc_min_version('4.2.0')
@pytest.mark.parametrize('test_name,input,expected,multiple_value_behavior, missing_value_behavior',
                         QUERIES, ids=[i[0] for i in QUERIES])
def test_lookup_query(sdc_builder, sdc_executor, couchbase,
                      test_name, input, expected, multiple_value_behavior, missing_value_behavior):
    bucket_name = get_random_string(string.ascii_letters, 10).lower()
    docs = [{'id': 'id1', 'data': 'hello'},
            {'id': 'id2', 'data': 'hello'},
            {'id': 'id3', 'data': 'hello'}]
    raw_dict = dict(criteria=input)
    raw_data = json.dumps(raw_dict)
    query = f"SELECT id FROM {bucket_name} WHERE " + '${record:value("/criteria")}'
    cluster = couchbase.cluster

    try:
        # populate the database
        logger.info('Creating %s Couchbase bucket ...', bucket_name)
        couchbase.bucket_manager.create_bucket(CreateBucketSettings(name=bucket_name,
                                                                    bucket_type='couchbase',
                                                                    ram_quota_mb=256))
        couchbase.wait_for_healthy_bucket(bucket_name)

        bucket = cluster.bucket(bucket_name)
        for doc in docs:
            bucket.upsert(doc['id'], doc)
        cluster.query(f'CREATE PRIMARY INDEX ON `{bucket_name}`').execute()

        # build the pipeline
        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('Dev Raw Data Source')
        origin.set_attributes(data_format='JSON',
                              stop_after_first_batch=True,
                              raw_data=raw_data)

        lookup = builder.add_stage('Couchbase Lookup')
        lookup.set_attributes(authentication_mode='USER', bucket=bucket_name,
                              lookup_type='N1QL', n1ql_query=query,
                              n1ql_mappings=[dict(property='id', sdcField='/output')],
                              multiple_value_behavior=multiple_value_behavior,
                              missing_value_behavior=missing_value_behavior)

        wiretap = builder.add_wiretap()

        origin >> lookup >> wiretap.destination

        pipeline = builder.build().configure_for_environment(couchbase)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        output_records = wiretap.output_records
        error_records = wiretap.error_records

        print('output:', output_records)

        if missing_value_behavior == 'ERROR':
            # The input record should pass through to error records without an output field
            assert len(error_records) == 1
            assert 'output' not in error_records[0].field
        elif not expected:
            # The input record should pass through to output records without an output field
            assert len(output_records) == 1
            assert 'output' not in output_records[0].field
        else:
            assert len(output_records) == len(expected)
            # Check that the output records are as expected, allowing for reordering
            output_list = [record.field['output'] for record in output_records]
            assert Counter(output_list) == Counter(expected)
    finally:
        try:
            logger.info('Deleting %s Couchbase bucket ...', bucket_name)
            couchbase.bucket_manager.drop_bucket(bucket_name)
        except Exception as e:
            logger.error(f"Can't delete bucket: {e}")
