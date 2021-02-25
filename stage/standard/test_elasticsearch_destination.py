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
import json

import pytest
from streamsets.testframework.markers import elasticsearch, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


DATA_TYPES = [
    ('true', 'BOOLEAN', True),
    ('a', 'CHAR', 'a'),
#    ('a', 'BYTE', None), # Not supported today
    (120, 'SHORT', 120),
    (120, 'INTEGER', 120),
    (120, 'LONG', 120),
    (20.1, 'FLOAT', 20.1),
    (20.1, 'DOUBLE', 20.1),
    (20.1, 'DECIMAL', 20.1),
    ('2020-01-01 10:00:00', 'DATE', 1577872800000),
    ('2020-01-01 10:00:00', 'TIME', 1577872800000),
    ('2020-01-01 10:00:00', 'DATETIME', 1577872800000),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', '2020-01-01T10:00:00Z'),
    ('string', 'STRING', 'string'),
#     ('string', 'BYTE_ARRAY', 'string'), # Not supported today
]
@elasticsearch
@pytest.mark.parametrize('input,converter_type,expected', DATA_TYPES, ids=[i[1] for i in DATA_TYPES])
def test_data_types(sdc_builder, sdc_executor, elasticsearch, input, converter_type, expected):
    index = get_random_string(string.ascii_letters, 10).lower()
    mapping = get_random_string(string.ascii_letters, 10)
    doc_id = get_random_string(string.ascii_letters, 10)

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Raw Data Source')
    origin.data_format = 'JSON'
    origin.stop_after_first_batch = True
    origin.raw_data = json.dumps({"value": input })

    converter = builder.add_stage('Field Type Converter')
    converter.conversion_method = 'BY_FIELD'
    converter.field_type_converter_configs = [{
        'fields': ['/value'],
        'targetType': converter_type,
        'dataLocale': 'en,US',
        'dateFormat': 'YYYY_MM_DD_HH_MM_SS',
        'zonedDateTimeFormat': 'ISO_OFFSET_DATE_TIME',
        'scale': 2
    }]

    target = builder.add_stage('Elasticsearch', type='destination')
    target.default_operation='INDEX'
    target.document_id = doc_id
    target.index = index
    target.mapping = mapping

    origin >> converter >> target

    pipeline = builder.build().configure_for_environment(elasticsearch)
    pipeline.configuration["shouldRetry"] = False

    sdc_executor.add_pipeline(pipeline)

    try:
        elasticsearch.client.create_index(index)

        # Run pipeline and read from Elasticsearch to assert
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        responses = elasticsearch.client.search(index)

        assert len(responses) == 1
        response = responses[0]
        assert response['_index'] == index
        assert response['_id'] == doc_id
        assert response['_type'] == mapping
        assert response['_source'] == {'value': expected}
    finally:
        # Clean up test data in ES
        elasticsearch.client.delete_index(index)


INDEX_NAMES = [
    ('max_size', get_random_string(string.ascii_letters, 255).lower(), []),
    ('underscore', get_random_string(string.ascii_letters, 10).lower() + '_' + get_random_string(string.ascii_letters, 10).lower(), []),
    ('hyphen', get_random_string(string.ascii_letters, 10).lower() + '-' + get_random_string(string.ascii_letters, 10).lower(), []),
    ('plus', get_random_string(string.ascii_letters, 10).lower() + '+' + get_random_string(string.ascii_letters, 10).lower(), [5]),
    ('dot', get_random_string(string.ascii_letters, 10).lower() + '.' + get_random_string(string.ascii_letters, 10).lower(), []),
    ('numbers', get_random_string(string.ascii_letters, 10).lower() + '1234567890', []),
]
@elasticsearch
@pytest.mark.parametrize('name_category,index, skip_versions', INDEX_NAMES, ids=[i[0] for i in INDEX_NAMES])
def test_object_names(sdc_builder, sdc_executor, elasticsearch, name_category, index, skip_versions):
    if elasticsearch.major_version in skip_versions:
        pytest.skip(f"This combination is not supported for ElasticSearch {elasticsearch.major_version}.x")

    mapping = get_random_string(string.ascii_letters, 10)
    doc_id = get_random_string(string.ascii_letters, 10)

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Raw Data Source')
    origin.data_format = 'TEXT'
    origin.stop_after_first_batch = True
    origin.raw_data = 'Hi!'

    target = builder.add_stage('Elasticsearch', type='destination')
    target.default_operation='INDEX'
    target.document_id = doc_id
    target.index = index
    target.mapping = mapping

    origin >> target

    pipeline = builder.build().configure_for_environment(elasticsearch)
    pipeline.configuration["shouldRetry"] = False

    sdc_executor.add_pipeline(pipeline)

    try:
        elasticsearch.client.create_index(index)

        # Run pipeline and read from Elasticsearch to assert
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        responses = elasticsearch.client.search(index)

        assert len(responses) == 1
        response = responses[0]
        assert response['_index'] == index
        assert response['_id'] == doc_id
        assert response['_type'] == mapping
        assert response['_source'] == {'text': 'Hi!'}
    finally:
        # Clean up test data in ES
        elasticsearch.client.delete_index(index)


@elasticsearch
def test_multiple_batches(sdc_builder, sdc_executor, elasticsearch):
    batch_size = 10
    batches = 3

    index = get_random_string(string.ascii_letters, 10).lower()
    mapping = get_random_string(string.ascii_letters, 10)

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Dev Data Generator')
    origin.batch_size = batch_size
    origin.fields_to_generate = [{
        "type" : "LONG_SEQUENCE",
        "field" : "seq"
    }]

    target = builder.add_stage('Elasticsearch', type='destination')
    target.default_operation='INDEX'
    target.document_id = '${record:value("/seq")}'
    target.index = index
    target.mapping = mapping

    origin >> target
    pipeline = builder.build().configure_for_environment(elasticsearch)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Wait for at least 10 batches
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(batches * batch_size)
        sdc_executor.stop_pipeline(pipeline)

        # Now the pipeline will write some amount of records that will be larger, so we get precise count from metrics
        history = sdc_executor.get_pipeline_history(pipeline)
        records = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count

        logger.info(f"Wrote {records} records")

        responses = elasticsearch.client.search(index)
        assert len(responses) == records

        def _sort_response(entry):
            return entry['_source']['seq']

        responses.sort(key=_sort_response)

        for i in range(0, records):
            assert responses[i]['_index'] == index
            assert responses[i]['_id'] == str(i)
            assert responses[i]['_type'] == mapping
            assert responses[i]['_source'] == {'seq': i}

    finally:
        # Clean up test data in ES
        elasticsearch.client.delete_index(index)


@elasticsearch
def test_dataflow_events(sdc_builder, sdc_executor, elasticsearch):
    pytest.skip("No events supported in ElasticSearch destination at this time.")


@elasticsearch
def test_data_format(sdc_builder, sdc_executor, elasticsearch, keep_data):
    pytest.skip("ElasticSearch Destination doesn't deal with data formats")


@elasticsearch
def test_push_pull(sdc_builder, sdc_executor, elasticsearch):
    pytest.skip("We haven't re-implemented this test since Dev Data Generator (push) is art of test_multiple_batches and Dev Raw Data Source (pull) is part of test_data_types.")
