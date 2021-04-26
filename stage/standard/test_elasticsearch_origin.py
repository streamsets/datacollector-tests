# Copyright 2020 StreamSets Inc.
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
from streamsets.testframework.markers import elasticsearch
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@elasticsearch
def test_data_types(sdc_builder, sdc_executor, elasticsearch):
    pytest.skip("""Even though elastic search have technically types, it still stores JSON that we read up as-is with our
        built-in JSON parser. We don't particularly respect the metadata types ElasticSearch things are there.
        """)


INDEX_NAMES = [
    ('max_size', get_random_string(string.ascii_lowercase, 255)),
    ('underscore', get_random_string(string.ascii_lowercase, 10) + '_' + get_random_string(string.ascii_lowercase, 10)),
    ('hyphen', get_random_string(string.ascii_lowercase, 10) + '-' + get_random_string(string.ascii_lowercase, 10)),
    ('plus', get_random_string(string.ascii_lowercase, 10) + '+' + get_random_string(string.ascii_lowercase, 10)),
    ('dot', get_random_string(string.ascii_lowercase, 10) + '.' + get_random_string(string.ascii_lowercase, 10)),
    ('numbers', get_random_string(string.ascii_lowercase, 10) + '1234567890'),
]


@elasticsearch
@pytest.mark.parametrize('name_category,index', INDEX_NAMES, ids=[i[0] for i in INDEX_NAMES])
def test_object_names(sdc_builder, sdc_executor, elasticsearch, name_category, index):
    doc_id = get_random_string(string.ascii_letters, 10)
    raw_str = 'Hello World!'
    raw = {'body': raw_str}

    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Elasticsearch', type='origin')
    origin.set_attributes(index=index, query="{'query': {'match_all': {}}}")
    wiretap = builder.add_wiretap()

    origin >> wiretap.destination
    es_origin_pipeline = builder.build().configure_for_environment(elasticsearch)
    sdc_executor.add_pipeline(es_origin_pipeline)

    try:
        # Put data to Elasticsearch
        elasticsearch.client.create_document(index, doc_id, raw)

        sdc_executor.start_pipeline(es_origin_pipeline).wait_for_finished()
        record = wiretap.output_records[0]

        assert record.field['_index'] == index
        assert record.field['_id'] == doc_id
        assert record.field['_source']['body'] == raw_str
    finally:
        elasticsearch.client.delete_index(index)


@elasticsearch
@pytest.mark.parametrize('incremental', [True, False])
def test_multiple_batches(sdc_builder, sdc_executor, elasticsearch, incremental):
    max_batch_size = 1000
    batches = 10
    index = get_random_string(string.ascii_lowercase, 10)

    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Elasticsearch', type='origin')
    origin.index = index
    origin.query = "{'query': {'match_all': {}}}"

    if incremental:
        origin.query = "{'sort': [{'number': {'order': 'asc'}}] , 'query': {'range': {'number': {'gt': ${OFFSET}}}}}"
        origin.incremental_mode = True
        origin.query_interval = "${0}"
        origin.offset_field = "number"
        origin.initial_offset = "${0}"

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination
    pipeline = builder.build().configure_for_environment(elasticsearch)

    sdc_executor.add_pipeline(pipeline)

    try:
        def generator():
            for i in range(1, max_batch_size * batches + 1):
                yield {
                    "_index": index,
                    "_type": "data",
                    "_source": {"number": i}
                }

        elasticsearch.client.bulk(generator())

        if incremental:
            sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(max_batch_size * batches)
        else:
            sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records

        # We should get the right number of records
        assert len(records) == max_batch_size * batches

        # Verify each record
        def sortFunc(entry):
            return entry.field['_source']['number'].value

        records.sort(key=sortFunc)

        expected_number = 1
        for record in records:
            assert record.field['_index'] == index
            assert record.field['_source']['number'].value == expected_number

            expected_number = expected_number + 1
    finally:
        # Clean up test data in ES
        elasticsearch.client.delete_index(index)


@elasticsearch
def test_dataflow_events(sdc_builder, sdc_executor, elasticsearch):
    pytest.skip("No events supported in ElasticSearch origin at this time.")


@elasticsearch
def test_data_format(sdc_builder, sdc_executor, elasticsearch, keep_data):
    pytest.skip("ElasticSearch Origin doesn't deal with data formats")


@elasticsearch
def test_resume_offset(sdc_builder, sdc_executor, elasticsearch):
    iterations = 3
    records_per_iteration = 10
    index = get_random_string(string.ascii_lowercase, 10)

    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Elasticsearch', type='origin')
    origin.index = index
    origin.query = "{'sort': [{'number': {'order': 'asc'}}], 'query': {'range': {'number': {'gt': ${OFFSET}}}}}"
    origin.incremental_mode = True
    origin.query_interval = "${0}"
    origin.offset_field = "number"
    origin.initial_offset = "${0}"

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination
    pipeline = builder.build().configure_for_environment(elasticsearch)

    sdc_executor.add_pipeline(pipeline)

    try:
        for iteration in range(0, iterations):
            logger.info(f"Iteration: {iteration}")
            wiretap.reset()

            def generator():
                for i in range(1, records_per_iteration + 1):
                    yield {
                        "_index": index,
                        "_type": "data",
                        "_source": {"number": iteration * records_per_iteration + i}
                    }

            elasticsearch.client.bulk(generator())
            sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(records_per_iteration)
            sdc_executor.stop_pipeline(pipeline)

            records = wiretap.output_records

            # We should get the right number of records
            assert len(records) == records_per_iteration

            def _sort_response(entry):
                return entry.field['_source']['number'].value

            records.sort(key=_sort_response)

            expected_number = iteration * records_per_iteration + 1
            for record in records:
                assert record.field['_index'] == index
                assert record.field['_source']['number'].value == expected_number

                expected_number = expected_number + 1
    finally:
        elasticsearch.client.delete_index(index)
