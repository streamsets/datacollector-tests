# Copyright 2023 StreamSets Inc.
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
from streamsets.testframework.markers import couchbase, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

pytestmark = [couchbase, sdc_min_version('4.2.0')]

SUPPORTED_LIBS = ['streamsets-datacollector-couchbase_2-lib']
STAGE_NAME = 'com_streamsets_pipeline_stage_processor_couchbase_CouchbaseDProcessor'

DEFAULT_SCOPE = '_default'
DEFAULT_COLLECTION = '_default'


@pytest.fixture(autouse=True, scope='module')
def init(couchbase):
    for lib in couchbase.sdc_stage_libs:
        if lib in SUPPORTED_LIBS:
            couchbase.pre_create_buckets()
            return
    pytest.skip(f'Couchbase Lookup test requires using libraries in {SUPPORTED_LIBS}')


LOOKUPS = [
    ('no matching, error', 'id2', [], [{'id': 'id2'}], 'ERROR'),
    ('no matching, pass', 'id2', [{'id': 'id2'}], [], 'PASS'),
    ('one matching', 'id1', [{'id': 'id1', 'output': {'id': 'id1', 'data': 'hello'}}], [], 'ERROR')
]


@pytest.mark.parametrize('test_name,input,expected_out,expected_error,missing_value_behavior',
                         LOOKUPS, ids=[i[0] for i in LOOKUPS])
def test_lookup_kv(sdc_builder, sdc_executor, couchbase,
                   test_name, input, expected_out, expected_error, missing_value_behavior):
    doc = {'id': 'id1', 'data': 'hello'}
    raw_dict = dict(id=input)
    raw_data = json.dumps(raw_dict)

    try:
        # populate the database
        bucket_name = couchbase.get_bucket()
        couchbase.insert_documents(doc['id'], doc, bucket_name)

        # build the pipeline
        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('Dev Raw Data Source')
        origin.set_attributes(data_format='JSON',
                              stop_after_first_batch=True,
                              raw_data=raw_data)

        lookup = builder.add_stage(name=STAGE_NAME)
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
        couchbase.cleanup_buckets()


QUERIES = [
    ('no matching, error', 'data="goodbye"', [], 'FIRST', 'ERROR'),
    ('no matching, pass', 'data="goodbye"', [], 'FIRST', 'PASS'),
    ('one matching', 'id="id1"', ['id1'], 'FIRST', 'PASS'),
    ('multi matching, first result', 'data="hello"', ['id1'], 'FIRST', 'PASS'),
    ('multi matching, multi results', 'data="hello"', ['id1', 'id2', 'id3'], 'MULTI', 'PASS')
]


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

    try:
        # populate the database
        bucket_name = couchbase.get_bucket()
        for doc in docs:
            couchbase.insert_documents(doc['id'], doc, bucket_name)

        # build the pipeline
        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('Dev Raw Data Source')
        origin.set_attributes(data_format='JSON',
                              stop_after_first_batch=True,
                              raw_data=raw_data)

        lookup = builder.add_stage(name=STAGE_NAME)
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
        couchbase.cleanup_buckets()
