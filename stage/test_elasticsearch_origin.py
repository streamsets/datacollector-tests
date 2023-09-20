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
import logging
import string

import pytest
from streamsets.testframework.markers import elasticsearch, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

raw_str = 'Hello World!'

ELASTICSEARCH_VERSION_8 = 8


@elasticsearch
def test_elasticsearch_origin(sdc_builder, sdc_executor, elasticsearch):
    """Test for default configuration of Elasticsearch origin stage. Write data using Elasticsearch client, then the
    pipeline reads from a ElasticSearch cluster and we check if the data received is valid via wiretap.
    The pipeline would look like:

    Elasticsearch error pipeline:
        origin >> wiretap.destination
    """
    index = get_random_string(string.ascii_lowercase, 10)
    doc_id = get_random_string(string.ascii_letters, 10)
    raw_str = 'Hello World!'
    raw_json = {'body': raw_str}

    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Elasticsearch', type='origin')
    origin.set_attributes(index=index,
                          query="{'query': {'match_all': {}}}")
    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    es_origin_pipeline = builder.build(title='Test Elasticsearch Origin').configure_for_environment(elasticsearch)
    sdc_executor.add_pipeline(es_origin_pipeline)

    try:
        # Put data to Elasticsearch
        elasticsearch.client.create_document(index, doc_id, raw_json)

        sdc_executor.start_pipeline(es_origin_pipeline).wait_for_finished()

        record = wiretap.output_records[0]
        assert record.field['_index'] == index
        assert record.field['_id'] == doc_id
        assert record.field['_source']['body'] == raw_str
    finally:
        # Delete all the data from Elasticsearch
        elasticsearch.client.delete_index(index)


@elasticsearch
@pytest.mark.parametrize('delete_scroll', [True, False])
def test_elasticsearch_origin_cursor_expired(sdc_builder, sdc_executor, elasticsearch, delete_scroll):
    """Test for cursor expired option when the pipeline stops with Elasticsearch origin stage. Write multiple data using
    Elasticsearch client, read with the pipeline up to 1 input and check the received data. Restart the pipeline and
    check if the new received data is the same data in case the "Delete Scroll" option is selected, or is new if not.
    The pipeline would look like:

    Elasticsearch error pipeline:
        origin >> delay >> wiretap.destination
    """
    if elasticsearch.major_version < ELASTICSEARCH_VERSION_8:
        pytest.skip("The test only runs with ElasticSearch 8")
    index = get_random_string(string.ascii_lowercase, 10)
    number_records = 10
    batch_record_size = 1

    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Elasticsearch', type='origin')
    origin.set_attributes(index=index,
                          query="{'query': {'match_all': {}}}",
                          max_batch_size=batch_record_size,
                          delete_scroll_on_pipeline_stop=delete_scroll)

    delay = builder.add_stage('Delay')
    delay.set_attributes(delay_between_batches=5000)

    wiretap = builder.add_wiretap()

    origin >> delay >> wiretap.destination

    pipeline = builder.build(title='Test Elasticsearch Origin cursor expired').configure_for_environment(elasticsearch)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Put data to Elasticsearch
        def generator():
            for i in range(1, number_records+1):
                if elasticsearch.major_version < ELASTICSEARCH_VERSION_8:
                    yield {
                        "_index": index,
                        "_type": "data",
                        "_source": {'a': i, 'b': 'Hello World!'}
                    }
                else:
                    yield {
                        "_index": index,
                        "_source": {'a': i, 'b': 'Hello World!'}
                    }

        elasticsearch.client.bulk(generator())
        previous_responses = elasticsearch.client.search(index)
        assert len(previous_responses) == number_records

        # Start the pipeline
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        records = [rec.field for rec in wiretap.output_records]
        assert len(records) > 0     # Assert records is not empty
        sorted_records = sorted(records, key=lambda x: x['_source']['a'])
        for i in range(len(sorted_records)):
            record = sorted_records[i]
            expected_data = {'a': i+1, 'b': 'Hello World!'}
            assert record['_index'] == index
            assert record['_source'] == expected_data

        # Restart the pipeline
        wiretap.reset()
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        new_records = [rec.field for rec in wiretap.output_records]
        assert len(new_records) > 0     # Assert new_records is not empty
        sorted_new_records = sorted(new_records, key=lambda x: x['_source']['a'])
        if delete_scroll:
            assert sorted_records == sorted_new_records
        else:
            completed_sorted_records = sorted_records + sorted_new_records
            for i in range(len(completed_sorted_records)):
                record = completed_sorted_records[i]
                expected_data = {'a': i+1, 'b': 'Hello World!'}
                assert record['_index'] == index
                assert record['_source'] == expected_data

    finally:
        elasticsearch.client.delete_index(index)


@elasticsearch
@pytest.mark.parametrize('query_interval', ['${1 * SECONDS}', '${1 * HOURS}'])
def test_elasticsearch_origin_incremental_mode(sdc_builder, sdc_executor, elasticsearch, query_interval):
    """Test for read data in incremental mode for Elasticsearch origin stage. Write multiple data using Elasticsearch
    client with offset field selected in the stage, read all the data with wiretap stage with different query intervals
    and check the received data.
    The pipeline would look like:

    Elasticsearch error pipeline:
        origin >> wiretap.destination
    """
    index = get_random_string(string.ascii_lowercase, 10)
    number_records = 1000

    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Elasticsearch', type='origin')
    origin.set_attributes(index=index,
                          query="{'sort': [{'a': {'order': 'asc'}}], 'query': {'range': {'a': {'gt': ${OFFSET}}}}}",
                          incremental_mode=True,
                          query_interval=query_interval,
                          offset_field="a",
                          initial_offset="${0}")
    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build(title='Test Elasticsearch Origin in incremental mode').configure_for_environment(elasticsearch)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Put data to Elasticsearch
        def generator():
            for i in range(1, number_records+1):
                if elasticsearch.major_version < ELASTICSEARCH_VERSION_8:
                    yield {
                        "_index": index,
                        "_type": "data",
                        "_source": {'a': i, 'b': 'Hello World!'}
                    }
                else:
                    yield {
                        "_index": index,
                        "_source": {'a': i, 'b': 'Hello World!'}
                    }

        elasticsearch.client.bulk(generator())
        previous_responses = elasticsearch.client.search(index)
        assert len(previous_responses) == number_records

        # Start the pipeline
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', number_records)
        sdc_executor.stop_pipeline(pipeline)

        records = [rec.field for rec in wiretap.output_records]
        assert len(records) == number_records
        sorted_records = sorted(records, key=lambda x: x['_source']['a'])
        for i in range(number_records):
            record = sorted_records[i]
            expected_data = {'a': i+1, 'b': 'Hello World!'}
            assert record['_index'] == index
            assert record['_source'] == expected_data

    finally:
        elasticsearch.client.delete_index(index)


@elasticsearch
@pytest.mark.parametrize('number_of_slices', [2, 25])
def test_elasticsearch_origin_parallelism(sdc_builder, sdc_executor, elasticsearch, number_of_slices):
    """Test formultiple slices in incremental mode for Elasticsearch origin stage. Write multiple data using
    Elasticsearch client and read all the data with different number of slices and check the received data.
    The pipeline would look like:

    Elasticsearch error pipeline:
        origin >> wiretap.destination
    """
    index = get_random_string(string.ascii_lowercase, 10)
    number_records = 1000

    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Elasticsearch', type='origin')
    origin.set_attributes(index=index,
                          query="{'sort': [{'a': {'order': 'asc'}}], 'query': {'range': {'a': {'gt': ${OFFSET}}}}}",
                          incremental_mode=True,
                          offset_field="a",
                          initial_offset="${0}",
                          number_of_slices=number_of_slices
                          )

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build(title='Test Elasticsearch Origin using parallelism').configure_for_environment(elasticsearch)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Put data to Elasticsearch
        def generator():
            for i in range(1, number_records+1):
                if elasticsearch.major_version < ELASTICSEARCH_VERSION_8:
                    yield {
                        "_index": index,
                        "_type": "data",
                        "_source": {'a': i, 'b': 'Hello World!'}
                    }
                else:
                    yield {
                        "_index": index,
                        "_source": {'a': i, 'b': 'Hello World!'}
                    }

        elasticsearch.client.bulk(generator())
        previous_responses = elasticsearch.client.search(index)
        assert len(previous_responses) == number_records

        # Start the pipeline
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', number_records)
        sdc_executor.stop_pipeline(pipeline)

        records = [rec.field for rec in wiretap.output_records]
        assert len(records) == number_records
        sorted_records = sorted(records, key=lambda x: x['_source']['a'])
        for i in range(number_records):
            record = sorted_records[i]
            expected_data = {'a': i+1, 'b': 'Hello World!'}
            assert record['_index'] == index
            assert record['_source'] == expected_data

    finally:
        elasticsearch.client.delete_index(index)


# SDC-11233: Elasticsearch origin does not properly upgrade single-threaded offsets
@elasticsearch
def test_offset_upgrade(sdc_builder, sdc_executor, elasticsearch):
    """Ensure that when upgrading from older offset format (that can be generated by either SCH or by upgrading
       pre-multithreaded pipeline) we properly upgrade the offset and the pipeline will not re-read everything
       from the source.
    """
    es_index = get_random_string(string.ascii_letters, 10).lower()
    es_doc_id = get_random_string(string.ascii_letters, 10)
    raw = {'body': raw_str}

    builder = sdc_builder.get_pipeline_builder()
    es_origin = builder.add_stage('Elasticsearch', type='origin')
    es_origin.set_attributes(index=es_index, query="{'query': {'match_all': {}}}")
    wiretap = builder.add_wiretap()

    es_origin >> wiretap.destination
    pipeline = builder.build().configure_for_environment(elasticsearch)
    sdc_executor.add_pipeline(pipeline)

    # We hard code offset to be pre-migration to multi-threaded origin and thus forcing the origin to upgrade it
    offset = {
        'offsets': {
            '$com.streamsets.datacollector.pollsource.offset$': None,
        },
        'version': 2
    }
    sdc_executor.api_client.update_pipeline_committed_offsets(pipeline.id, body=offset)

    try:
        # Put data to Elasticsearch
        elasticsearch.client.create_document(es_index, es_doc_id, raw)

        # Run pipeline and assert
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        # no need to stop pipeline - as ES origin shuts off once data is read from Elasticsearch

        assert len(wiretap.output_records) == 1
        # assert ES meta
        assert wiretap.output_records[0].field['_index'] == es_index
        assert wiretap.output_records[0].field['_id'] == es_doc_id
        # assert ES data
        assert wiretap.output_records[0].field['_source']['body'] == raw_str

        # Now let's validate that the offset doesn't have the poll key any more
        offset = sdc_executor.api_client.get_pipeline_committed_offsets(pipeline.id).response.json()
        assert offset is not None
        assert '$com.streamsets.datacollector.pollsource.offset$' not in offset['offsets']
    finally:
        # Clean up test data in ES
        elasticsearch.client.delete_index(es_index)


@elasticsearch
def test_index_and_template_with_plus_get_encoded(sdc_builder, sdc_executor, elasticsearch):
    """
    We want to test that we can search in documents of document types containing the '+' character
    belonging to indices containing the '+' character.
    So we will create an index with the '+' character and a document type with the '+' character.
    Then we will search for fields in all in the index. If the '+' character is accepted
    then we will get exactly the same document we created before since the newly created index
    contains only it. The pipeline is as follows:

    Elasticsearch >> Trash

    This same test should pass for all Elasticsearch versions.
    This test will fail for SDC version less than 3.19.0 which is expected.
    """
    if elasticsearch.major_version >= ELASTICSEARCH_VERSION_8:
        pytest.skip("doc types are not available in ES 8.x+")

    doc_id = get_random_string(string.ascii_lowercase)
    doc_index = f'{doc_id}id+x'
    doc_type = f'{doc_id}tp+y'
    doc = {"data": "DATA"}

    builder = sdc_builder.get_pipeline_builder()

    es = builder.add_stage('Elasticsearch', type='origin').set_attributes(index=doc_index,
                                                                          query="{'query': {'match_all': {}}}",
                                                                          mapping=doc_type)

    elasticsearch.client.client.create(index=doc_index, doc_type=doc_type, id=doc_id, body=doc)

    wiretap = builder.add_wiretap()
    pipeline_finisher = builder.add_stage('Pipeline Finisher Executor')
    es >> [wiretap.destination, pipeline_finisher]

    pipeline = builder.build().configure_for_environment(elasticsearch)
    pipeline.configuration["shouldRetry"] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_data = [record.field for record in wiretap.output_records]

        assert len(output_data) == 1
        assert output_data[0]['_index'] == doc_index
        assert output_data[0]['_id'] == doc_id
        assert output_data[0]['_source'] == doc
        assert output_data[0]['_type'] == doc_type
    finally:
        elasticsearch.client.delete_index(doc_index)
