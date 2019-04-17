# Copyright 2018 StreamSets Inc.
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

import pytest
from streamsets.testframework.utils import get_random_string
from streamsets.testframework.markers import solr, sdc_min_version

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@solr
def test_solr_write_records_apache(sdc_builder, sdc_executor, solr):
    """A reusable function to test Dev Raw Data Source to Solr target pipeline.
    Since the same doc is ingested, we can skip multiple writes by using deduplicator. The Pipeline looks like:

    dev_raw_data_source >> record_deduplicator >> solr_target
                                               >> to_error
    """
    client = solr.client
    field_name_1 = get_random_string(string.ascii_letters, 10)

    field_name_2 = get_random_string(string.ascii_letters, 10)
    field_val_1 = get_random_string(string.ascii_letters, 10)
    field_val_2 = get_random_string(string.ascii_letters, 10)
    json_fields_map = [{'field': '/id', 'solrFieldName': field_name_1},
                       {'field': '/title', 'solrFieldName': field_name_2}]
    json_str = json.dumps(dict(id=field_val_1, title=field_val_2))

    # build Solr target pipeline.
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=json_str)

    record_deduplicator = builder.add_stage('Record Deduplicator')
    to_error = builder.add_stage('To Error')

    solr_target = builder.add_stage('Solr', type='destination')
    solr_target.set_attributes(instance_type='SINGLE_NODE',
                               record_indexing_mode='RECORD',
                               fields=json_fields_map)

    dev_raw_data_source >> record_deduplicator >> solr_target
    record_deduplicator >> to_error

    solr_dest_pipeline = builder.build(title='Solr Target pipeline').configure_for_environment(solr)
    solr_dest_pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(solr_dest_pipeline)

    # assert data ingested into Solr.
    try:
        sdc_executor.start_pipeline(solr_dest_pipeline).wait_for_pipeline_batch_count(1)
        sdc_executor.stop_pipeline(solr_dest_pipeline)

        query = f'{{!term f={field_name_1}}}{field_val_1}'
        results = client.search(q=query)
        assert len(results) > 0
        assert results.docs[0][field_name_2][0] == field_val_2
    finally:
        # cleanup the documents created by the test.
        client.delete(id=field_name_1)
        client.delete(id=field_name_2)

        # cleanup the fields created by the test.
        _delete_schema_field(client, field_name_1)
        _delete_schema_field(client, field_name_2)


@solr
@sdc_min_version('3.8.0')
def test_solr_write_fields_automatically_mapped_apache(sdc_builder, sdc_executor, solr):
    """Test documents are correctly stored in Solr when mappings to solr schema fields are directly taken from the
    record. Pipeline looks like:

    dev_raw_data_source >> solr_target
    """
    client = solr.client

    field_val_1 = get_random_string(string.ascii_letters, 10)
    field_val_2 = get_random_string(string.ascii_letters, 10)
    json_str = json.dumps(dict(id=field_val_1, title=field_val_2))

    # Build Solr target pipeline.
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=json_str)
    solr_target = builder.add_stage('Solr', type='destination')
    solr_target.set_attributes(instance_type='SINGLE_NODE',
                               record_indexing_mode='RECORD',
                               map_fields_automatically=True,
                               field_path_for_data='/',
                               ignore_optional_fields=True)

    dev_raw_data_source >> solr_target

    solr_dest_pipeline = builder.build(title='Solr Target pipeline').configure_for_environment(solr)
    solr_dest_pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(solr_dest_pipeline)

    # Assert data ingested into Solr.
    try:
        sdc_executor.start_pipeline(solr_dest_pipeline).wait_for_pipeline_batch_count(1)
        sdc_executor.stop_pipeline(solr_dest_pipeline)

        query = f'{{!term f=id}}{field_val_1}'
        results = client.search(q=query)
        assert len(results) > 0
        assert results.docs[0]['title'][0] == field_val_2
    finally:
        # Cleanup fields created by the test.
        client.delete(id=field_val_1)


@solr
@sdc_min_version('3.8.0')
@pytest.mark.parametrize('map_fields_automatically', [True, False])
@pytest.mark.parametrize('ignore_optional_fields', [True, False])
def test_solr_required_fields(sdc_builder, sdc_executor, solr, map_fields_automatically, ignore_optional_fields):
    """Test required fields validation in Solr stage. When a record lacks any required field, SDC must discard it and
    produce a record error (when "Missing Fields" is "To Error"). This must be done irrespective of other stage
    parameters (e.g. "Ignore Optional Fields" or "Map Fields Automatically").

    Pipeline: dev_raw_data_source >> solr_target

    """
    # Prepare data for 2 documents. We include all the optional fields declared in the Solr schema to avoid an error
    # when `ignore_optional_fields == False`.
    id1 = f'stf_test_id_{get_random_string(string.ascii_letters)}'
    title1 = f'stf_test_title_{get_random_string(string.ascii_letters)}'
    title2 = f'stf_test_title_{get_random_string(string.ascii_letters)}'
    raw_data = [{'id': id1, 'title': title1, '_root_': None, '_version_': 0, '_text_': ''},
                {'title': title2, '_root_': None, '_version_': 0, '_text_': ''}]  # Omit the required 'id' field.

    builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source.
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=''.join(json.dumps(d) for d in raw_data))

    if map_fields_automatically:
        field_mapping = []  # Not used.
    else:
        field_mapping = [{'field': '/id', 'solrFieldName': 'id'},
                         {'field': '/title', 'solrFieldName': 'title'},
                         {'field': '/_root_', 'solrFieldName': '_root_'},
                         {'field': '/_version_', 'solrFieldName': '_version_'},
                         {'field': '/_text_', 'solrFieldName': '_text_'}]

    # Solr target.
    solr_target = builder.add_stage('Solr', type='destination')
    solr_target.set_attributes(instance_type='SINGLE_NODE',
                               record_indexing_mode='RECORD',
                               map_fields_automatically=map_fields_automatically,
                               fields=field_mapping,
                               field_path_for_data='/',
                               ignore_optional_fields=ignore_optional_fields,
                               missing_fields='TO_ERROR')

    # Build pipeline.
    dev_raw_data_source >> solr_target
    pipeline = builder.build(title='Solr Target pipeline').configure_for_environment(solr)
    sdc_executor.add_pipeline(pipeline)

    try:
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        # Test that the first record was indexed.
        res = solr.client.search(f'id:{id1}')
        assert len(res) == 1
        assert res.docs[0]['title'] == [title1]

        # Solr stage must produce a SOLR_06 or SOLR_07 error for the second record.
        stage = snapshot[solr_target.instance_name]
        assert len(stage.error_records) == 1
        assert stage.error_records[0].field['title'] == title2
        assert stage.error_records[0].header['errorCode'] == 'SOLR_07' if map_fields_automatically else 'SOLR_06'

        # Check also that missing fields are listed in the error message.
        assert 'id' in stage.error_records[0].header['errorMessage']

    finally:
        # Cleanup documents created by the test.
        solr.client.delete(id=id1)


@solr
@sdc_min_version('3.8.0')
@pytest.mark.parametrize('map_fields_automatically', [True, False])
@pytest.mark.parametrize('ignore_optional_fields', [True, False])
def test_solr_optional_fields(sdc_builder, sdc_executor, solr, map_fields_automatically, ignore_optional_fields):
    """Test optional fields validation in Solr stage. When "Ignore Optional Fields" is disabled, Solr stage must discard
    and send to error (when "Missing Fields" is "To Error") those records with missing optional fields. When the ignore
    option is enabled, records are sent to Solr irrespective of having or not missing optional fields.

    Pipeline: dev_raw_data_source >> solr_target

    """
    id1 = f'stf_test_id_{get_random_string(string.ascii_letters)}'
    id2 = f'stf_test_id_{get_random_string(string.ascii_letters)}'
    title1 = f'stf_test_title_{get_random_string(string.ascii_letters)}'
    raw_data = [{'id': id1, 'title': title1, '_root_': None, '_version_': 0, '_text_': ''},
                {'id': id2, '_root_': None}]  # Omit 'title', '_version_' and '_text_' optional fields.

    builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source.
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=''.join(json.dumps(d) for d in raw_data))

    if map_fields_automatically:
        field_mapping = []  # Not used.
    else:
        field_mapping = [{'field': '/id', 'solrFieldName': 'id'},
                         {'field': '/title', 'solrFieldName': 'title'},
                         {'field': '/_root_', 'solrFieldName': '_root_'},
                         {'field': '/_version_', 'solrFieldName': '_version_'},
                         {'field': '/_text_', 'solrFieldName': '_text_'}]

    # Solr target.
    solr_target = builder.add_stage('Solr', type='destination')
    solr_target.set_attributes(instance_type='SINGLE_NODE',
                               record_indexing_mode='RECORD',
                               map_fields_automatically=map_fields_automatically,
                               fields=field_mapping,
                               field_path_for_data='/',
                               ignore_optional_fields=ignore_optional_fields,
                               missing_fields='TO_ERROR')

    # Build pipeline.
    dev_raw_data_source >> solr_target
    pipeline = builder.build(title='Solr Target pipeline').configure_for_environment(solr)
    sdc_executor.add_pipeline(pipeline)

    try:
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        # Test that the first record was indexed.
        res = solr.client.search(f'id:{id1}')
        assert len(res) == 1
        assert res.docs[0]['title'] == [title1]
        assert '_root_' not in res.docs[0]

        # Test pipeline behavior with the second record.
        if ignore_optional_fields:
            # When ignoring optional fields this record must be indexed in Solr.
            res = solr.client.search(f'id:{id2}')
            assert len(res) == 1
            assert 'title' not in res.docs[0]
            assert '_root_' not in res.docs[0]
        else:
            # Otherwise, Solr stage must produce a SOLR_06 or SOLR_8 error for that record.
            stage = snapshot[solr_target.instance_name]
            assert len(stage.error_records) == 1
            assert stage.error_records[0].field['id'] == id2
            assert stage.error_records[0].header['errorCode'] == 'SOLR_08' if map_fields_automatically else 'SOLR_06'

            # Check also that missing fields are listed in the error message.
            assert 'title' in stage.error_records[0].header['errorMessage']
            assert '_version_' in stage.error_records[0].header['errorMessage']
            assert '_text_' in stage.error_records[0].header['errorMessage']

    finally:
        # Cleanup documents created by the test.
        solr.client.delete(id=id1)
        solr.client.delete(id=id2)


@solr
@sdc_min_version('3.9.0')
@pytest.mark.parametrize('map_fields_automatically', [True, False])
def test_solr_autogenerated_fields(sdc_builder, sdc_executor, solr, map_fields_automatically):
    """Test that Solr validation allows records to omit Solr autogenerated fields and check that they are properly
    indexed. We test it against a Solr schema which has the document 'id' as an autogenerated field.

    Pipeline:
        dev_raw_data_source >> solr_target

    """
    id1 = f'stf_test_id_{get_random_string(string.ascii_letters)}'
    title1 = f'stf_test_title_{get_random_string(string.ascii_letters)}'
    title2 = f'stf_test_title_{get_random_string(string.ascii_letters)}'
    raw_data = [{'id': id1, 'title': title1},
                {'title': title2}]  # 2nd record has no id.

    builder = sdc_builder.get_pipeline_builder()

    # Dev raw data source.
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=''.join(json.dumps(d) for d in raw_data),
                                       stop_after_first_batch=True)

    if map_fields_automatically:
        field_mapping = []  # Not used.
    else:
        field_mapping = [{'field': '/id', 'solrFieldName': 'id'},
                         {'field': '/title', 'solrFieldName': 'title'}]

    # Solr target.
    solr_target = builder.add_stage('Solr', type='destination')
    solr_target.set_attributes(instance_type='SINGLE_NODE',
                               record_indexing_mode='RECORD',
                               map_fields_automatically=map_fields_automatically,
                               fields=field_mapping,
                               field_path_for_data='/',
                               ignore_optional_fields=True,
                               auto_generated_fields=['id'])

    # Build pipeline.
    dev_raw_data_source >> solr_target
    pipeline = builder.build(title='Solr Target pipeline').configure_for_environment(solr)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Test that the first record was indexed.
        res = solr.client.search(f'id:{id1}')
        assert len(res) == 1
        assert res.docs[0]['title'] == [title1]

        # Test that the second record was indexed with an auto-generated id.
        res = solr.client.search(f'title:{title2}')
        assert len(res) == 1
        assert 'id' in res.docs[0] and len(res.docs[0]['id']) > 0

    finally:
        # Cleanup fields created by the test.
        solr.client.delete(id=id1)
        solr.client.delete(f'title:{title2}')


def _delete_schema_field(solr, field_name):
    """Delete a field from the Solr schema.

    Args:
        solr: (:obj:`pysolr.Solr`) a Solr client connection.
        field_name: (:obj:`str`) name of the field to remove.

    """
    body = '{{"delete-field" : {{"name": "{}"}} }}'.format(field_name)
    solr._send_request('POST', 'schema', body=body)
