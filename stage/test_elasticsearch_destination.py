# Copyright 2017 StreamSets Inc.
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
import datetime

import pytest
from streamsets.testframework.markers import elasticsearch, sdc_min_version
from streamsets.testframework.utils import get_random_string
from streamsets.sdk.exceptions import RunError, RunningError, StartError

logger = logging.getLogger(__name__)

raw_str = 'Hello World!'

ELASTICSEARCH_VERSION_8 = 8


@elasticsearch
def test_elasticsearch_destination(sdc_builder, sdc_executor, elasticsearch):
    """Test for default configuration of Elasticsearch destination stage. Write data in Elasticsearch with the pipeline
    and check with ElasticSearch client if the data is correctly wrote.
    The pipeline would look like:

    Elasticsearch error pipeline:
        dev_raw_data_source >> target
    """
    # Test static
    index = get_random_string(string.ascii_letters, 10).lower()
    mapping = get_random_string(string.ascii_letters, 10)
    doc_id = get_random_string(string.ascii_letters, 10)

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Raw Data Source').set_attributes(
        raw_data=raw_str,
        data_format='TEXT',
        stop_after_first_batch=True
    )

    target = builder.add_stage('Elasticsearch', type='destination').set_attributes(
        default_operation='INDEX',
        document_id=doc_id,
        index=index,
        mapping=mapping
    )

    origin >> target

    pipeline = builder.build(title='Test ElasticSearch Destination').configure_for_environment(elasticsearch)
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
        assert response['_source'] == {'text': raw_str}
        if elasticsearch.major_version < ELASTICSEARCH_VERSION_8:
            assert response['_type'] == mapping

    finally:
        # Clean up test data in ES
        elasticsearch.client.delete_index(index)


@elasticsearch
@sdc_min_version('3.0.0.0')
def test_elasticsearch_pipeline_errors(sdc_builder, sdc_executor, elasticsearch):
    """Test for a pipeline's error records being pumped to Elasticsearch. We do so by making a Dev Raw Data source
    target to Error stage which would send records to the pipeline configured Elasticsearch error records handling.
    We then assert the error records what we find in Elasticsearch. The pipeline would look like:

    Elasticsearch error pipeline:
        dev_raw_data_source >> error_target
    """
    # Test static
    es_index = get_random_string(string.ascii_letters, 10).lower()  # Elasticsearch indexes must be lower case
    es_mapping = get_random_string(string.ascii_letters, 10)
    es_doc_id = get_random_string(string.ascii_letters, 10)

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    elastic_to_error = builder.add_error_stage('Write to Elasticsearch')
    elastic_to_error.set_attributes(document_id=es_doc_id, index=es_index, mapping=es_mapping)
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  stop_after_first_batch=True,
                                                                                  raw_data=raw_str)

    error_target = builder.add_stage('To Error')

    dev_raw_data_source >> error_target
    es_error_pipeline = builder.build().configure_for_environment(elasticsearch)
    sdc_executor.add_pipeline(es_error_pipeline)

    try:
        elasticsearch.client.create_index(es_index)

        # Run pipeline and read from Elasticsearch to assert
        sdc_executor.start_pipeline(es_error_pipeline).wait_for_finished()

        responses = elasticsearch.client.search(es_index)
        assert len(responses) == 1

        response = responses[0]
        assert response['_index'] == es_index
        assert response['_id'] == es_doc_id
        assert response['_source'] == {'text': raw_str}
        if elasticsearch.major_version < ELASTICSEARCH_VERSION_8:
            assert response['_type'] == es_mapping
    finally:
        # Clean up test data in ES
        elasticsearch.client.delete_index(es_index)


@elasticsearch
@pytest.mark.parametrize('on_record_error', ['DISCARD', 'STOP_PIPELINE', 'TO_ERROR'])
def test_elasticsearch_destination_on_record_error(sdc_builder, sdc_executor, elasticsearch, on_record_error):
    """Test for incorrect configuration of Elasticsearch destination stage with different ways to handle the error.
    Try to write data in Elasticsearch with wrong index, then check the history to get if the pipeline get the correct
    metrics.
    The pipeline would look like:

    Elasticsearch error pipeline:
        dev_raw_data_source >> error_target
    """
    # Test static
    index = get_random_string(string.ascii_letters, 10).lower()
    mapping = get_random_string(string.ascii_letters, 10)
    doc_id = get_random_string(string.ascii_letters, 10)

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(raw_data=raw_str,
                                                                                  data_format='TEXT',
                                                                                  stop_after_first_batch=True)

    target = builder.add_stage('Elasticsearch', type='destination').set_attributes(
        default_operation='INDEX',
        document_id=doc_id,
        index='NO',
        mapping=mapping,
        on_record_error=on_record_error
    )

    dev_raw_data_source >> target

    pipeline = builder.build(title='Test ElasticSearch Destination error handler').configure_for_environment(elasticsearch)
    pipeline.configuration["shouldRetry"] = False

    sdc_executor.add_pipeline(pipeline)

    try:
        elasticsearch.client.create_index(index)

        # Run pipeline and read from Elasticsearch to assert
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        if on_record_error == 'DISCARD':
            history = sdc_executor.get_pipeline_history(pipeline)
            assert history.latest.metrics.counter('stage.Elasticsearch_01.outputRecords.counter').count == 1
            assert history.latest.metrics.counter('stage.Elasticsearch_01.errorRecords.counter').count == 0
        elif on_record_error == 'STOP_PIPELINE':
            assert False, "The pipeline should have arisen an exception after the error"
        else:
            history = sdc_executor.get_pipeline_history(pipeline)
            assert history.latest.metrics.counter('stage.Elasticsearch_01.outputRecords.counter').count == 0
            assert history.latest.metrics.counter('stage.Elasticsearch_01.errorRecords.counter').count == 1

    except (RunError, RunningError) as e:
        if on_record_error == 'STOP_PIPELINE':
            response = sdc_executor.get_pipeline_status(pipeline).response.json()
            status = response.get('status')
            logger.info('Pipeline status %s ...', status)
            assert 'ELASTICSEARCH_17' in e.message
        else:
            raise e

    finally:
        # Clean up test data in Elasticsearch
        elasticsearch.client.delete_index(index)


@elasticsearch
def test_elasticsearch_destination_time_driver_now(sdc_builder, sdc_executor, elasticsearch):
    """Test for default configuration of Elasticsearch destination stage using as time basis the time evaluator.
    Write data in Elasticsearch with the pipeline and check with ElasticSearch client if the data is correctly wrote.
    The pipeline would look like:

    Elasticsearch error pipeline:
        dev_raw_data_source >> target
    """
    if elasticsearch.major_version < ELASTICSEARCH_VERSION_8:
        pytest.skip("The test only runs with ElasticSearch 8")

    # Test static
    index = datetime.date.today().year
    index_template = '${YYYY()}'
    mapping = '${record:value(\'/type\')}'
    doc_id = get_random_string(string.ascii_letters, 10)

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Raw Data Source').set_attributes(
        raw_data=raw_str,
        data_format='TEXT',
        stop_after_first_batch=True
    )

    target = builder.add_stage('Elasticsearch', type='destination').set_attributes(
        default_operation='INDEX',
        document_id=doc_id,
        index=index_template,
        mapping=mapping,
        time_basis="${time:now()}"
    )

    origin >> target

    pipeline = builder.build(title='Test ElasticSearch Destination time driver now').configure_for_environment(elasticsearch)
    pipeline.configuration["shouldRetry"] = False

    sdc_executor.add_pipeline(pipeline)

    try:
        elasticsearch.client.create_index(index)

        # Run pipeline and read from Elasticsearch to assert
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        responses = elasticsearch.client.search(index)
        assert len(responses) == 1
        response = responses[0]
        assert response['_index'] == str(index)
        assert response['_id'] == doc_id
        assert response['_source'] == {'text': raw_str}

    finally:
        # Clean up test data in ES
        elasticsearch.client.delete_index(index)


@elasticsearch
def test_elasticsearch_destination_upsert(sdc_builder, sdc_executor, elasticsearch):
    """Test for doing upsert operation with Elasticsearch destination stage. Write data using Elasticsearch client and
    update the new data using the pipeline and check with ElasticSearch client if the data is correctly updated.
    The pipeline would look like:

    Elasticsearch error pipeline:
        dev_raw_data_source >> target
    """
    if elasticsearch.major_version < ELASTICSEARCH_VERSION_8:
        pytest.skip("The test only runs with ElasticSearch 8")

    # Test static
    index = get_random_string(string.ascii_letters, 10).lower()
    mapping = get_random_string(string.ascii_letters, 10)
    doc_id = get_random_string(string.ascii_letters, 10)

    # Data input
    old_data = 'Old'
    new_data = 'New'

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Raw Data Source').set_attributes(
        raw_data=new_data,
        data_format='TEXT',
        stop_after_first_batch=True
    )

    target = builder.add_stage('Elasticsearch', type='destination').set_attributes(
        default_operation='INDEX',
        document_id=doc_id,
        index=index,
        mapping=mapping,
        time_basis="${time:now()}"
    )

    origin >> target

    pipeline = builder.build(title='Test ElasticSearch Destination update operation').configure_for_environment(elasticsearch)
    pipeline.configuration["shouldRetry"] = False

    sdc_executor.add_pipeline(pipeline)

    try:
        # Put data to Elasticsearch
        elasticsearch.client.create_document(index, doc_id, {'text': old_data})
        previous_responses = elasticsearch.client.search(index)
        assert previous_responses[0]['_source'] == {'text': old_data}

        # Run pipeline and read from Elasticsearch to assert
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        responses = elasticsearch.client.search(index)
        assert len(responses) == 1
        response = responses[0]
        assert response['_index'] == index
        assert response['_id'] == doc_id
        assert response['_source'] == {'text': new_data}
        if elasticsearch.major_version < ELASTICSEARCH_VERSION_8:
            assert response['_type'] == mapping
    finally:
        # Clean up test data in ES
        elasticsearch.client.delete_index(index)


@elasticsearch
def test_elasticsearch_destination_merge(sdc_builder, sdc_executor, elasticsearch):
    """Test for doing merge operation with Elasticsearch destination stage. Write data using Elasticsearch client and
    merge the new data of the record using the pipeline and check with ElasticSearch client if the data is correctly
    merged.
    The pipeline would look like:

    Elasticsearch error pipeline:
        dev_raw_data_source >> target
    """
    if elasticsearch.major_version < ELASTICSEARCH_VERSION_8:
        pytest.skip('ElasticSearch Merge operation only runs against ElasticSearch version < 8.')

    # Data input
    old_data = {'a': 'Old', 'nested': {'one': 'uno', 'two': 'dos', 'three': 'tres'}, 'existing': 'Not touched'}
    new_data = {'a': 'New', 'nested': {'one': 'uno', 'two': 'due', 'four': 'quattour'},
                'new': 'fresh', 'existing': 'Not touched'}
    expected_data = {'a': 'New', 'nested': {'one': 'uno', 'two': 'due', 'three': 'tres', 'four': 'quattour'},
                     'new': 'fresh', 'existing': 'Not touched'}

    # Test static
    index = get_random_string(string.ascii_letters, 10).lower()
    mapping = get_random_string(string.ascii_letters, 10)
    doc_id = get_random_string(string.ascii_letters, 10)

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Raw Data Source').set_attributes(
        raw_data=json.dumps(new_data),
        data_format='JSON',
        stop_after_first_batch=True
    )

    target = builder.add_stage('Elasticsearch', type='destination').set_attributes(
        default_operation='MERGE',
        document_id=doc_id,
        index=index,
        mapping=mapping
    )

    origin >> target

    pipeline = builder.build(title='Test ElasticSearch Destination merge operation').configure_for_environment(elasticsearch)
    pipeline.configuration["shouldRetry"] = False

    sdc_executor.add_pipeline(pipeline)

    try:
        # Put data to Elasticsearch
        elasticsearch.client.create_document(index, doc_id, old_data)
        previous_responses = elasticsearch.client.search(index)
        assert previous_responses[0]['_source'] == old_data

        # Run pipeline and read from Elasticsearch to assert
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        responses = elasticsearch.client.search(index)
        assert len(responses) == 1
        response = responses[0]
        assert response['_index'] == index
        assert response['_id'] == doc_id
        assert response['_source'] == expected_data

    finally:
        # Clean up test data in ES
        elasticsearch.client.delete_index(index)


@elasticsearch
def test_elasticsearch_destination_additional_properties(sdc_builder, sdc_executor, elasticsearch):
    """Test for default configuration of Elasticsearch destination stage using additional properties. Write date in a
    human-readable format in Elasticsearch with the additional property "human" as false, and check with ElasticSearch
    client if the data is correctly wrote as a timestamp.
    The pipeline would look like:

    Elasticsearch error pipeline:
        origin >> converter >> target
    """
    # Test static
    data_input = '2020-01-01'
    index = get_random_string(string.ascii_letters, 10).lower()
    mapping = get_random_string(string.ascii_letters, 10)
    doc_id = get_random_string(string.ascii_letters, 10)
    additional_properties = '{"human":false}'

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Raw Data Source').set_attributes(
        raw_data=json.dumps({"value": data_input}),
        data_format='JSON',
        stop_after_first_batch=True
    )

    converter = builder.add_stage('Field Type Converter')
    converter.conversion_method = 'BY_FIELD'
    converter.field_type_converter_configs = [{
        'fields': ['/value'],
        'targetType': 'DATE',
        'dataLocale': 'en,US',
        'dateFormat': 'YYYY_MM_DD',
        'zonedDateTimeFormat': 'ISO_OFFSET_DATE_TIME',
        'scale': 2
    }]

    target = builder.add_stage('Elasticsearch', type='destination').set_attributes(
        default_operation='INDEX',
        document_id=doc_id,
        index=index,
        mapping=mapping,
        additional_properties=additional_properties
    )

    origin >> converter >> target

    pipeline = builder.build(title='Test ElasticSearch Destination Additional Properties').configure_for_environment(elasticsearch)
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
        assert response['_source'] == {'value': 1577836800000}
        if elasticsearch.major_version < ELASTICSEARCH_VERSION_8:
            assert response['_type'] == mapping

    finally:
        # Clean up test data in ES
        elasticsearch.client.delete_index(index)


@sdc_min_version('3.17.0')
@elasticsearch
@pytest.mark.parametrize('join_credentials', [True, False])
def test_elasticsearch_credentials_format(sdc_builder, sdc_executor, elasticsearch, join_credentials):
    """
    Elasticsearch target pipeline where specifies two different formats for the credential values.
    First, it checks if the previous format "username:password" is also valid and then update the pipeline with the new
    format, user name and password into two different fields, and checks again.
        dev_raw_data_source >> es_target
    """
    # Test static
    es_index = get_random_string(string.ascii_letters, 10).lower()  # Elasticsearch indexes must be lower case
    es_mapping = get_random_string(string.ascii_letters, 10)
    es_doc_id = get_random_string(string.ascii_letters, 10)

    if join_credentials:
        username = elasticsearch.username + ':' + elasticsearch.password
        password = ''
    else:
        username = elasticsearch.username
        password = elasticsearch.password

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                  stop_after_first_batch=True,
                                                                                  raw_data=raw_str)
    es_target = builder.add_stage('Elasticsearch', type='destination')
    es_target.set_attributes(default_operation='INDEX', document_id=es_doc_id, index=es_index,
                             use_security=True, user_name=username, password=password, mapping=es_mapping)

    dev_raw_data_source >> es_target
    es_target_pipeline = builder.build().configure_for_environment(elasticsearch)
    es_target_pipeline.configuration["shouldRetry"] = False

    sdc_executor.add_pipeline(es_target_pipeline)

    try:
        elasticsearch.client.create_index(es_index)

        # Run pipeline and read credential values from Elasticsearch to assert
        sdc_executor.start_pipeline(es_target_pipeline).wait_for_finished()

        # Since we are upsert on the same index, map, doc - there should only be one document (index 0)
        response = elasticsearch.client.search(es_index)
        assert len(response) == 1
        assert response[0]['_index'] == es_index
        assert response[0]['_id'] == es_doc_id
        assert response[0]['_source'] == {'text': raw_str}
        if elasticsearch.major_version < ELASTICSEARCH_VERSION_8:
            assert response[0]['_type'] == es_mapping
    finally:
        # Clean up test data in ES
        elasticsearch.client.delete_index(es_index)
