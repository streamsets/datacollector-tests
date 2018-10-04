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
from streamsets.testframework.markers import cluster
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

pytestmark = cluster('cdh')


def test_solr_write_records_cdh(sdc_builder, sdc_executor, cluster):
    """Solr basic write records test case.
    dev_raw_data_source >> solr_target
    """

    # Mandatory to have an id of the document for CDH Solr schemaless.
    field_name_1 = cluster.solr.default_field_name
    field_name_2 = get_random_string(string.ascii_letters, 10)
    field_val_1 = get_random_string(string.ascii_letters, 10)
    field_val_2 = get_random_string(string.ascii_letters, 10)

    json_str = json.dumps(dict(id=field_val_1, title=field_val_2))

    json_fields_map = [{'field': '/id', 'solrFieldName': field_name_1},
                       {'field': '/title', 'solrFieldName': field_name_2}]

    # build Solr target pipeline.
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON', raw_data=json_str)

    solr_target = builder.add_stage('Solr', type='destination')
    solr_target.set_attributes(fields=json_fields_map)

    dev_raw_data_source >> solr_target

    pipeline = builder.build(title='Solr Target pipeline').configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    # assert data ingested into Solr.
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)
        sdc_executor.stop_pipeline(pipeline)

        query = f'{{!term f={field_name_1}}}{field_val_1}'
        resp = cluster.solr.client.query(cluster.solr.collection_name, {'q': query})
        assert resp.num_found > 0
        assert resp.data['response']['docs'][0][field_name_2][0] == field_val_2
    finally:
        # Delete Solr document created in the test.
        cluster.solr.client.delete_doc_by_id(cluster.solr.collection_name, field_val_1)


def test_solr_write_records_on_error_discard(sdc_builder, sdc_executor, cluster):
    """Solr write records on error discard test case.
    dev_raw_data_source >> solr_target
    """

    # Mandatory to have an id of the document for CDH Solr schemaless.
    field_name_1 = cluster.solr.default_field_name
    field_name_2 = get_random_string(string.ascii_letters, 10)
    field_val_1 = get_random_string(string.ascii_letters, 10)
    field_val_2 = get_random_string(string.ascii_letters, 10)

    json_str = json.dumps(dict(id=field_val_1, title=field_val_2))

    json_fields_map = [{'field': '/not_id', 'solrFieldName': field_name_1},
                       {'field': '/title', 'solrFieldName': field_name_2}]

    # build Solr target pipeline.
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON', raw_data=json_str)

    solr_target = builder.add_stage('Solr', type='destination')
    solr_target.set_attributes(fields=json_fields_map,
                               on_record_error='DISCARD')

    dev_raw_data_source >> solr_target

    pipeline = builder.build(title='Solr Target pipeline').configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    # assert data ingested into Solr.
    try:
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        query = f'{{!term f={field_name_1}}}{field_val_1}'
        resp = cluster.solr.client.query(cluster.solr.collection_name, {'q': query})
        assert 0 == resp.num_found

        stage = snapshot[solr_target.instance_name]
        assert 0 == len(stage.error_records)
    finally:
        # Delete Solr document created in the test.
        cluster.solr.client.delete_doc_by_id(cluster.solr.collection_name, field_val_1)


def test_solr_write_records_on_error_to_error(sdc_builder, sdc_executor, cluster):
    """Solr write records on error to error test case.
    dev_raw_data_source >> solr_target
    """

    # Mandatory to have an id of the document for CDH Solr schemaless.
    field_name_1 = cluster.solr.default_field_name
    field_name_2 = get_random_string(string.ascii_letters, 10)
    field_val_1 = get_random_string(string.ascii_letters, 10)
    field_val_2 = get_random_string(string.ascii_letters, 10)

    json_str = json.dumps(dict(id=field_val_1, title=field_val_2))

    json_fields_map = [{'field': '/not_id', 'solrFieldName': field_name_1},
                       {'field': '/title', 'solrFieldName': field_name_2}]

    # build Solr target pipeline.
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON', raw_data=json_str)

    solr_target = builder.add_stage('Solr', type='destination')
    solr_target.set_attributes(on_record_error='TO_ERROR',
                               fields=json_fields_map)

    dev_raw_data_source >> solr_target

    pipeline = builder.build(title='Solr Target pipeline').configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    # assert data ingested into Solr.
    try:
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        query = f'{{!term f={field_name_1}}}{field_val_1}'
        resp = cluster.solr.client.query(cluster.solr.collection_name, {'q': query})
        assert resp.num_found == 0

        stage = snapshot[solr_target.instance_name]
        assert 1 == len(stage.error_records)
        assert 'SOLR_06' == stage.error_records[0].header['errorCode']
    finally:
        # Delete Solr document created in the test.
        cluster.solr.client.delete_doc_by_id(cluster.solr.collection_name, field_val_1)


def test_solr_write_records_indexing_error_to_error(sdc_builder, sdc_executor, cluster):
    """Solr write records indexing error to error test case.
    dev_raw_data_source >> solr_target
    """

    # Mandatory to have an id of the document for CDH Solr schemaless.
    field_name_1 = cluster.solr.default_field_name
    field_name_2 = get_random_string(string.ascii_letters, 10)
    field_val_1 = get_random_string(string.ascii_letters, 10)
    field_val_2 = get_random_string(string.ascii_letters, 10)

    data = [{'id': field_val_1, "name": field_val_2}, {'not_id': field_val_1, "name": field_val_2}]

    json_str = json.dumps(data)

    json_fields_map = [{'field': '/id', 'solrFieldName': field_name_1},
                       {'field': '/title', 'solrFieldName': field_name_2}]

    # build Solr target pipeline.
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON', raw_data=json_str)

    solr_target = builder.add_stage('Solr', type='destination')
    solr_target.set_attributes(on_record_error='TO_ERROR',
                               fields=json_fields_map)

    dev_raw_data_source >> solr_target

    pipeline = builder.build(title='Solr Target pipeline').configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    # assert data ingested into Solr.
    try:
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        query = f'{{!term f={field_name_2}}}{field_val_2}'
        resp = cluster.solr.client.query(cluster.solr.collection_name, {'q': query})
        assert resp.num_found == 0

        stage = snapshot[solr_target.instance_name]
        assert 1 == len(stage.error_records)
        assert 'SOLR_06' == stage.error_records[0].header['errorCode']
    finally:
        # Delete Solr document created in the test.
        cluster.solr.client.delete_doc_by_id(cluster.solr.collection_name, field_val_1)


def test_solr_write_records_error_stop_pipeline(sdc_builder, sdc_executor, cluster):
    """Solr write records error stop pipeline test case.
    dev_raw_data_source >> solr_target
    """

    # Mandatory to have an id of the document for CDH Solr schemaless.
    field_name_1 = cluster.solr.default_field_name
    field_name_2 = get_random_string(string.ascii_letters, 10)
    field_val_1 = get_random_string(string.ascii_letters, 10)
    field_val_2 = get_random_string(string.ascii_letters, 10)

    data = {'not_id': field_name_1, "name": field_val_2}

    json_str = json.dumps(data)

    json_fields_map = [{'field': '/id', 'solrFieldName': field_name_1},
                       {'field': '/title', 'solrFieldName': field_name_2}]

    # build Solr target pipeline.
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON', raw_data=json_str)

    solr_target = builder.add_stage('Solr', type='destination')
    solr_target.set_attributes(on_record_error='STOP_PIPELINE',
                               fields=json_fields_map)

    dev_raw_data_source >> solr_target

    pipeline = builder.build(title='Solr Target pipeline').configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    # assert data ingested into Solr.
    try:
        with pytest.raises(Exception):
            sdc_executor.start_pipeline(pipeline)
            sdc_executor.stop_pipeline(pipeline)

        status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
        assert 'RUN_ERROR' == status
    finally:
        # Delete Solr document created in the test.
        cluster.solr.client.delete_doc_by_id(cluster.solr.collection_name, field_val_1)


def test_solr_write_record_empty_stop_pipeline(sdc_builder, sdc_executor, cluster):
    """Solr write record empty stop pipeline test case.
    dev_raw_data_source >> solr_target
    """

    # Mandatory to have an id of the document for CDH Solr schemaless.
    field_name_1 = None
    field_name_2 = get_random_string(string.ascii_letters, 10)
    field_val_1 = get_random_string(string.ascii_letters, 10)
    field_val_2 = get_random_string(string.ascii_letters, 10)

    data = {'id': field_name_1, "name": field_val_2}

    json_str = json.dumps(data)

    json_fields_map = [{'field': '/id', 'solrFieldName': field_val_1},
                       {'field': '/title', 'solrFieldName': field_name_2}]

    # build Solr target pipeline.
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON', raw_data=json_str)

    solr_target = builder.add_stage('Solr', type='destination')
    solr_target.set_attributes(on_record_error='STOP_PIPELINE',
                               fields=json_fields_map)

    dev_raw_data_source >> solr_target

    pipeline = builder.build(title='Solr Target pipeline').configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    # assert data ingested into Solr.
    try:
        with pytest.raises(Exception):
            sdc_executor.start_pipeline(pipeline)
            sdc_executor.stop_pipeline(pipeline)

        status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
        assert 'RUN_ERROR' == status
    finally:
        # Delete Solr document created in the test.
        cluster.solr.client.delete_doc_by_id(cluster.solr.collection_name, field_val_1)


def test_solr_write_record_empty_to_error(sdc_builder, sdc_executor, cluster):
    """Solr write record empty to error test case.
    dev_raw_data_source >> solr_target
    """

    # Mandatory to have an id of the document for CDH Solr schemaless.
    field_name_1 = None
    field_name_2 = get_random_string(string.ascii_letters, 10)
    field_val_1 = get_random_string(string.ascii_letters, 10)
    field_val_2 = get_random_string(string.ascii_letters, 10)

    data = {'id': field_name_1, "name": field_val_2}

    json_str = json.dumps(data)

    json_fields_map = [{'field': '/id', 'solrFieldName': field_val_1},
                       {'field': '/title', 'solrFieldName': field_name_2}]

    # build Solr target pipeline.
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON', raw_data=json_str)

    solr_target = builder.add_stage('Solr', type='destination')
    solr_target.set_attributes(on_record_error='TO_ERROR',
                               fields=json_fields_map)

    dev_raw_data_source >> solr_target

    pipeline = builder.build(title='Solr Target pipeline').configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    # assert data ingested into Solr.
    try:
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        query = f'{{!term f={field_name_1}}}{field_val_1}'
        resp = cluster.solr.client.query(cluster.solr.collection_name, {'q': query})
        assert resp.num_found == 0

        stage = snapshot[solr_target.instance_name]
        assert 1 == len(stage.error_records)
        assert 'SOLR_06' == stage.error_records[0].header['errorCode']
    finally:
        # Delete Solr document created in the test.
        cluster.solr.client.delete_doc_by_id(cluster.solr.collection_name, field_val_1)


def test_solr_write_record_empty_discard(sdc_builder, sdc_executor, cluster):
    """Solr write record empty dicard test case.
    dev_raw_data_source >> solr_target
    """

    # Mandatory to have an id of the document for CDH Solr schemaless.
    field_name_1 = None
    field_name_2 = get_random_string(string.ascii_letters, 10)
    field_val_1 = get_random_string(string.ascii_letters, 10)
    field_val_2 = get_random_string(string.ascii_letters, 10)

    data = {'id': field_name_1, "name": field_val_2}

    json_str = json.dumps(data)

    json_fields_map = [{'field': '/id', 'solrFieldName': field_val_1},
                       {'field': '/title', 'solrFieldName': field_name_2}]

    # build Solr target pipeline.
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON', raw_data=json_str)

    solr_target = builder.add_stage('Solr', type='destination')
    solr_target.set_attributes(on_record_error='DISCARD',
                               fields=json_fields_map)

    dev_raw_data_source >> solr_target

    pipeline = builder.build(title='Solr Target pipeline').configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    # assert data ingested into Solr.
    try:
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        query = f'{{!term f={field_name_1}}}{field_val_1}'
        resp = cluster.solr.client.query(cluster.solr.collection_name, {'q': query})
        assert 0 == resp.num_found

        stage = snapshot[solr_target.instance_name]
        assert 0 == len(stage.error_records)
    finally:
        # Delete Solr document created in the test.
        cluster.solr.client.delete_doc_by_id(cluster.solr.collection_name, field_val_1)


def test_solr_test_validations_null_url(sdc_builder, sdc_executor, cluster):
    """Solr basic validations null url.
    dev_raw_data_source >> solr_target
    """

    # Mandatory to have an id of the document for CDH Solr schemaless.
    field_name_1 = cluster.solr.default_field_name
    field_name_2 = get_random_string(string.ascii_letters, 10)
    field_val_1 = get_random_string(string.ascii_letters, 10)
    field_val_2 = get_random_string(string.ascii_letters, 10)

    json_str = json.dumps(dict(id=field_val_1, title=field_val_2))

    json_fields_map = [{'field': 'id', 'solrFieldName': field_name_1},
                       {'field': 'title', 'solrFieldName': field_name_2}]

    # build Solr target pipeline.
    builder = sdc_builder.get_pipeline_builder()

    pipeline = builder.build(title='Solr Target pipeline').configure_for_environment(cluster)

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON', raw_data=json_str)

    solr_target = builder.add_stage('Solr', type='destination')
    solr_target.set_attributes(solr_uri=None,
                               fields=json_fields_map)

    dev_raw_data_source >> solr_target

    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    # assert data ingested into Solr.
    try:
        with pytest.raises(Exception):
            sdc_executor.start_pipeline(pipeline)
            sdc_executor.stop_pipeline(pipeline)

        issues = sdc_executor.api_client.export_pipeline(pipeline.id)['pipelineConfig']['issues']

        assert 'VALIDATION_0007 - Configuration value is required' in issues['stageIssues']['Solr_01'][0]['message']

    finally:
        # Delete Solr document created in the test.
        cluster.solr.client.delete_doc_by_id(cluster.solr.collection_name, field_val_1)


def test_solr_test_validations_empty_fields(sdc_builder, sdc_executor, cluster):
    """Solr basic validations empty fields.
    dev_raw_data_source >> solr_target
    """

    # Mandatory to have an id of the document for CDH Solr schemaless.
    field_val_1 = get_random_string(string.ascii_letters, 10)
    field_val_2 = get_random_string(string.ascii_letters, 10)

    json_str = json.dumps(dict(id=field_val_1, title=field_val_2))

    json_fields_map = []

    # build Solr target pipeline.
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON', raw_data=json_str)

    solr_target = builder.add_stage('Solr', type='destination')
    solr_target.set_attributes(fields=json_fields_map)

    dev_raw_data_source >> solr_target

    pipeline = builder.build(title='Solr Target pipeline').configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    # assert data ingested into Solr.
    try:
        with pytest.raises(Exception) as e:
            sdc_executor.start_pipeline(pipeline)
            sdc_executor.stop_pipeline(pipeline)

        assert 'SOLR_02' in e.value.message

    finally:
        # Delete Solr document created in the test.
        cluster.solr.client.delete_doc_by_id(cluster.solr.collection_name, field_val_1)


def test_solr_test_validations_invalid_url(sdc_builder, sdc_executor, cluster):
    """Solr basic validations invalid url.
    dev_raw_data_source >> solr_target
    """

    # Mandatory to have an id of the document for CDH Solr schemaless.
    field_name_1 = cluster.solr.default_field_name
    field_name_2 = get_random_string(string.ascii_letters, 10)
    field_val_1 = get_random_string(string.ascii_letters, 10)
    field_val_2 = get_random_string(string.ascii_letters, 10)

    json_str = json.dumps(dict(id=field_val_1, title=field_val_2))

    json_fields_map = [{'field': '/id', 'solrFieldName': field_name_1},
                       {'field': '/title', 'solrFieldName': field_name_2}]

    # build Solr target pipeline.
    builder = sdc_builder.get_pipeline_builder()

    pipeline = builder.build(title='Solr Target pipeline').configure_for_environment(cluster)

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON', raw_data=json_str)

    solr_target = builder.add_stage('Solr', type='destination')
    solr_target.set_attributes(fields=json_fields_map,
                               solr_uri='I am invalid')

    dev_raw_data_source >> solr_target

    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    # assert data ingested into Solr.
    try:
        with pytest.raises(Exception) as e:
            sdc_executor.start_pipeline(pipeline)
            sdc_executor.stop_pipeline(pipeline)

        assert 'SOLR_03' in e.value.message

    finally:
        # Delete Solr document created in the test.
        cluster.solr.client.delete_doc_by_id(cluster.solr.collection_name, field_val_1)
