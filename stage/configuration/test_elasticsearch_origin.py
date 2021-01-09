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
import os
import string

import pytest
from streamsets.sdk.exceptions import ValidationError
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import elasticsearch, sdc_min_version
from streamsets.testframework.utils import get_random_string, Version

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'AWSSIGV4', 'use_security': True}])
def test_access_key_id(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_additional_http_params(sdc_builder, sdc_executor):
    pass


@stub
def test_cluster_http_uris(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'delete_scroll_on_pipeline_stop': False},
                                              {'delete_scroll_on_pipeline_stop': True}])
def test_delete_scroll_on_pipeline_stop(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'detect_additional_nodes_in_cluster': False},
                                              {'detect_additional_nodes_in_cluster': True}])
def test_detect_additional_nodes_in_cluster(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'AWSSIGV4', 'region': 'OTHER', 'use_security': True}])
def test_endpoint(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'incremental_mode': False}, {'incremental_mode': True}])
def test_incremental_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@elasticsearch
@pytest.mark.parametrize('valid_index', [True, False])
def test_index(sdc_builder, sdc_executor, elasticsearch, valid_index):
    """
    To test the index configuration we create a pipeline as follows:

    Elasticsearch >> Wiretap

    Then we add a document to an index. When a valid index name is used we expect to find the document in the index.
    When an invalid index name is used we expect an error to happen.
    """

    index = get_random_string(string.ascii_lowercase)
    origin_index = index if valid_index else get_random_string(string.ascii_lowercase)
    doc_id = get_random_string(string.ascii_lowercase)

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Elasticsearch', type='origin')
    origin.query = '{"query": {"match_all": {}}}'
    origin.index = origin_index

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(elasticsearch)

    sdc_executor.add_pipeline(pipeline)

    elasticsearch.client.create_document(index=index, id=doc_id, body={
        "number": 1
    })

    try:
        if valid_index:
            sdc_executor.start_pipeline(pipeline).wait_for_finished()

            assert len(wiretap.output_records) == 1

            record = wiretap.output_records[0]
            assert record.field['_id'] == doc_id
            assert record.field['_index'] == index
            assert record.field['_source'] == {"number": 1}
        else:
            with pytest.raises(ValidationError) as e:
                sdc_executor.validate_pipeline(pipeline)

            assert e.value.issues['issueCount'] == 1
            assert e.value.issues['stageIssues'][origin.instance_name][0]['message'].find('ELASTICSEARCH_45') != -1

    finally:
        elasticsearch.client.delete_index(index)


@stub
@pytest.mark.parametrize('stage_attributes', [{'incremental_mode': True}])
def test_initial_offset(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_mapping(sdc_builder, sdc_executor):
    pass


@stub
def test_max_batch_size(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'AWSSIGV4', 'use_security': True},
                                              {'mode': 'BASIC', 'use_security': True}])
def test_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_number_of_slices(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'incremental_mode': True}])
def test_offset_field(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@elasticsearch
@pytest.mark.parametrize('test_data', [
    {'error': True, 'query': '{"query": {"matchAll": {}}}', 'error_code': 'ELASTICSEARCH_41'},
    {'error': True, 'query': 'INVALID_JSON', 'error_code': 'ELASTICSEARCH_34'},
    {'error': False, 'query': '{"query": {"match_all": {}}, "sort": ["number"]}', 'error_code': None}])
def test_query(sdc_builder, sdc_executor, elasticsearch, test_data):
    """
    We will test a valid query containing an additional field apart from the query field.
    In the validate API, only the query field is allowed, though in the search API other fields may be used,
    e.g. the sort field. Here we want to test that if a valid query contains additional fields the origin doesn't break.

    We also want to test that the validation fails if a query is invalid or a query JSON is not a valid object itself.

    The pipeline is as follows:

    Elasticsearch >> Wiretap

    """

    index = get_random_string(string.ascii_lowercase)
    doc_count = 10

    def generator():
        for i in range(0, doc_count):
            yield {
                "_index": index,
                "_type": "data",
                "_source": {"number": i + 1}
            }

    elasticsearch.client.bulk(generator())

    try:
        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('Elasticsearch', type='origin')
        origin.index = index
        origin.query = test_data['query']

        wiretap = builder.add_wiretap()

        origin >> wiretap.destination

        pipeline = builder.build().configure_for_environment(elasticsearch)

        sdc_executor.add_pipeline(pipeline)

        if test_data['error']:
            with pytest.raises(ValidationError) as e:
                sdc_executor.validate_pipeline(pipeline)

            assert e.value.issues['issueCount'] == 1
            assert e.value.issues['stageIssues'][origin.instance_name][0]['message'].find(test_data['error_code']) != -1

        else:
            sdc_executor.start_pipeline(pipeline).wait_for_finished()

            assert len(wiretap.output_records) == doc_count

            for i in range(0, doc_count):
                record = wiretap.output_records[i]
                assert record.field['_index'] == index
                assert record.field['_source'] == {"number": i + 1}

    finally:
        elasticsearch.client.delete_index(index)


@stub
@pytest.mark.parametrize('stage_attributes', [{'incremental_mode': True}])
def test_query_interval(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'AWSSIGV4', 'region': 'AP_NORTHEAST_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'AP_NORTHEAST_2', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'AP_NORTHEAST_3', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'AP_SOUTHEAST_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'AP_SOUTHEAST_2', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'AP_SOUTH_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'CA_CENTRAL_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'CN_NORTHWEST_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'CN_NORTH_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'EU_CENTRAL_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'EU_WEST_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'EU_WEST_2', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'EU_WEST_3', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'OTHER', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'SA_EAST_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'US_EAST_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'US_EAST_2', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'US_GOV_WEST_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'US_WEST_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'US_WEST_2', 'use_security': True}])
def test_region(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_scroll_timeout(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'AWSSIGV4', 'use_security': True}])
def test_secret_access_key(sdc_builder, sdc_executor, stage_attributes):
    pass


@elasticsearch
@pytest.mark.parametrize('stage_attributes', [
    # True for with_valid_username or with_valid_password means set a valid value.
    # False for with_valid_username or with_valid_password means set an invalid value.
    # None for with_valid_username or with_valid_password means set blank value
    # If None is used for one of the config param, the opposite config param can be either valid or invalid,
    # it doesn't matter, the pipeline will fail before sending any requests
    {'with_valid_password': False, 'with_valid_username': False, 'error_code': 'ELASTICSEARCH_09'},
    {'with_valid_password': False, 'with_valid_username': True, 'error_code': 'ELASTICSEARCH_09'},
    {'with_valid_password': True, 'with_valid_username': False, 'error_code': 'ELASTICSEARCH_09'},
    {'with_valid_password': None, 'with_valid_username': True, 'error_code': 'ELASTICSEARCH_39'},
    {'with_valid_password': True, 'with_valid_username': None, 'error_code': 'ELASTICSEARCH_20'},
    {'with_valid_password': None, 'with_valid_username': None, 'error_code': 'ELASTICSEARCH_20'},
    {'with_valid_password': True, 'with_valid_username': True, 'error_code': None},
])
@sdc_min_version('3.17.0')  # The way the test validates various combinations isn't compatible with pre-3.17 pipelines
def test_security_username_and_password(sdc_builder, sdc_executor, elasticsearch, stage_attributes):
    """
    To test the username and password configurations we create a pipeline as follows:

    Elasticsearch >> Wiretap

    Then we check different combinations of valid/invalid/empty username/password configuration values.
    We expect no errors when the username and password are not empty and are valid.
    We verify that an appropriate error happens when an invalid/empty username and/or password are set.
    """

    if stage_attributes['with_valid_password'] is None:
        password = ''
    elif stage_attributes['with_valid_password']:
        password = elasticsearch.password
    else:
        password = get_random_string()

    if stage_attributes['with_valid_username'] is None:
        username = ''
    elif stage_attributes['with_valid_username']:
        username = elasticsearch.username
    else:
        username = get_random_string()

    index = get_random_string(string.ascii_lowercase)
    doc_id = get_random_string(string.ascii_lowercase)

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Elasticsearch', type='origin')
    origin.query = '{"query": {"match_all": {}}}'
    origin.index = index

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(elasticsearch)

    configured_origin = pipeline.stages.get(label=origin.label)

    if Version(sdc_builder.version) < Version('3.17.0'):
        configured_origin.configuration['conf.securityConfig.securityUser'] = f'{username}:{password}'
    else:
        configured_origin.user_name = username
        configured_origin.password = password

    sdc_executor.add_pipeline(pipeline)

    if stage_attributes['error_code'] is None:
        elasticsearch.client.create_document(index=index, id=doc_id, body={"number": 1})

        try:
            sdc_executor.start_pipeline(pipeline).wait_for_finished()

            assert len(wiretap.output_records) == 1

            record = wiretap.output_records[0]
            assert record.field['_index'] == index
            assert record.field['_id'] == doc_id
            assert record.field['_source'] == {"number": 1}

        finally:
            elasticsearch.client.delete_index(index)

    else:
        with pytest.raises(ValidationError) as e:
            sdc_executor.validate_pipeline(pipeline)

        assert e.value.issues['issueCount'] == 1
        assert e.value.issues['stageIssues'][origin.instance_name][0]['message'].find(
            stage_attributes['error_code']) != -1


@elasticsearch
@pytest.mark.parametrize('stage_attributes', [
    {'password': None, 'filename': 'keystore.jks', 'error_code': ['ELASTICSEARCH_12', 'ELASTICSEARCH_12']},
    {'password': 'changeme', 'filename': 'empty.jks', 'error_code': ['ELASTICSEARCH_12', 'ELASTICSEARCH_12']},
    # This configuration is correct and no error should happen.
    # But we do not have an Elasticsearch environment with SSL/TLS enabled
    # As a result the server will ignore the requested HTTPs protocol
    # and will respond as if we requested the HTTP protocol.
    # In this case SDC raises an error since we expect an SSL/TLS response from the server.
    # The test case is here because we have this issue https://issues.streamsets.com/browse/STF-1374
    # When completed error_code should be set to None
    {'password': 'changeme', 'filename': 'keystore.jks', 'error_code': [None, 'ELASTICSEARCH_43']}
])
def test_ssl_truststore_password(sdc_builder, sdc_executor, stage_attributes, elasticsearch):
    """
    To test the ssl truststore password configuration we create a pipeline as follows:

    Elasticsearch >> Wiretap

    Then we copy a keystore file from the STF to SDC container and configure the Elasticsearch origin to use this file.
    If the file is a valid JKS file and the password is correct we search for a document in the index we have put before.
    If the file is an invalid or the password doesn't match we expect an appropriate error to happen.
    """

    error_code_mode = 1 if Version(sdc_executor.version) >= Version('3.21.0') else 0
    keystore_filename = get_random_string()
    files_directory = os.path.join('/tmp', get_random_string())
    keystore_file_path = f'{files_directory}/{keystore_filename}'
    sdc_executor.execute_shell(f'mkdir -pv {files_directory}')

    try:
        # FIXME: This is just to copy a keystore file form the STF container to the SDC container.
        # We read a binary file, then we convert each byte to a hex string representation, e.g 127 becomes 7f, 64 -> 40
        # and then we insert \x before every two characters, e.g. \x7f\x40...
        # This representation of the binary file will then be used by the printf command internally used by
        # sdc_executor.write_file to write to a file in the SDC container.
        with open(os.path.join(os.path.dirname(__file__), '..', '..', 'resources', 'elasticsearch', stage_attributes['filename']), 'rb') as f:
            contents = f.read().hex()
            contents = '\\x' + '\\x'.join(contents[i:i + 2] for i in range(0, len(contents), 2))
            sdc_executor.write_file(keystore_file_path, contents)

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('Elasticsearch', type='origin')
        origin.index = get_random_string(string.ascii_lowercase)
        origin.query = '{"query": {"match_all": {}}}'
        if Version(sdc_builder.version) >= Version('3.21.0'):
            origin.enable_ssl = True
        origin.ssl_truststore_path = keystore_file_path
        origin.ssl_truststore_password = get_random_string() if stage_attributes['password'] is None else stage_attributes['password']

        wiretap = builder.add_wiretap()

        origin >> wiretap.destination

        pipeline = builder.build().configure_for_environment(elasticsearch)

        sdc_executor.add_pipeline(pipeline)

        if stage_attributes['error_code'][error_code_mode] is None:
            doc_id = get_random_string(string.ascii_lowercase)

            elasticsearch.client.create_document(index=origin.index, id=doc_id, body={"number": 1})

            try:
                sdc_executor.start_pipeline(pipeline).wait_for_finished()

                assert len(wiretap.output_records) == 1

                record = wiretap.output_records[0]
                assert record.field['_id'] == doc_id
                assert record.field['_index'] == origin.index
                assert record.field['_source'] == {"number": 1}

            finally:
                elasticsearch.client.delete_index(origin.index)

        else:
            with pytest.raises(ValidationError) as e:
                sdc_executor.validate_pipeline(pipeline)

            assert e.value.issues['issueCount'] == 1
            assert e.value.issues['stageIssues'][origin.instance_name][0]['message'].find(
                stage_attributes['error_code'][error_code_mode]) != -1

    finally:
        sdc_executor.execute_shell(f'rm -frv {files_directory}')


@elasticsearch
@pytest.mark.parametrize('stage_attributes', [
    # valid_file_path is True - use a path to a real file
    # valid_file_path is False - use a path to an not existing file
    # valid_file_path is None - use an empty string for the path, this will be resolved to a working folder,
    # which is eventually an invalid trust store file path

    {'valid_file_path': False, 'password': 'changeme', 'error_codes': [['ELASTICSEARCH_11'], ['ELASTICSEARCH_11'], ['ELASTICSEARCH_11']]},
    {'valid_file_path': None, 'password': 'changeme', 'error_codes': [None, None, ['ELASTICSEARCH_12']]},
    {'valid_file_path': True, 'password': '', 'error_codes': [['ELASTICSEARCH_10'], ['ELASTICSEARCH_10'], ['ELASTICSEARCH_10']]},
    {'valid_file_path': False, 'password': '', 'error_codes': [['ELASTICSEARCH_10', 'ELASTICSEARCH_11'], ['ELASTICSEARCH_10', 'ELASTICSEARCH_11'], ['ELASTICSEARCH_10', 'ELASTICSEARCH_11']]},
    {'valid_file_path': None, 'password': '', 'error_codes': [None, None, ['ELASTICSEARCH_10']]},
    # This configuration is correct and no error should happen.
    # But we do not have an Elasticsearch environment with SSL/TLS enabled
    # As a result the server will ignore the requested HTTPs protocol
    # and will respond as if we requested the HTTP protocol.
    # In this case SDC raises an error since we expect an SSL/TLS response from the server.
    # The test case is here because we have this issue https://issues.streamsets.com/browse/STF-1374
    # When completed error_codes should be set to None
    {'valid_file_path': True, 'password': 'changeme', 'error_codes': [None, ['ELASTICSEARCH_43'], ['ELASTICSEARCH_43']]},
])
def test_ssl_truststore_path(sdc_builder, sdc_executor, stage_attributes, elasticsearch):
    """
    To test the ssl truststore path configuration we create a pipeline as follows:

    Elasticsearch >> Wiretap

    Then we copy a keystore file from the STF to SDC container.
    If the ssl_truststore_path configuration points to an existing file the pipeline should succeed
    and we should find a document we have put before in an index.
    Otherwise an appropriate error should happen.
    """

    error_codes_mode = 0 if Version(sdc_executor.version) < Version('3.21.0') \
        else (1 if Version(sdc_builder.version) < Version('3.21.0') else 2)
    keystore_filename = get_random_string()
    files_directory = os.path.join('/tmp', get_random_string())
    keystore_file_path = f'{files_directory}/{keystore_filename}'

    sdc_executor.execute_shell(f'mkdir -pv {files_directory}')

    try:
        # FIXME: This is just to copy a keystore file form the STF container to the SDC container.
        # We read a binary file, then we convert each byte to a hex string representation, e.g 127 becomes 7f, 64 -> 40
        # and then we insert \x before every two characters, e.g. \x7f\x40...
        # This representation of the binary file will then be used by the printf command internally used by
        # sdc_executor.write_file to write to a file in the SDC container.
        with open(os.path.join(os.path.dirname(__file__), '..', '..', 'resources', 'elasticsearch', 'keystore.jks'), 'rb') as f:
            contents = f.read().hex()
            contents = '\\x' + '\\x'.join(contents[i:i + 2] for i in range(0, len(contents), 2))
            sdc_executor.write_file(keystore_file_path, contents)

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('Elasticsearch', type='origin')
        origin.index = get_random_string(string.ascii_lowercase)
        origin.query = '{"query": {"match_all": {}}}'
        origin.ssl_truststore_path = '' if stage_attributes['valid_file_path'] is None \
            else (keystore_file_path if stage_attributes['valid_file_path'] else get_random_string())
        origin.ssl_truststore_password = stage_attributes['password']
        if Version(sdc_builder.version) >= Version('3.21.0'):
            origin.enable_ssl = True

        wiretap = builder.add_wiretap()

        origin >> wiretap.destination

        pipeline = builder.build().configure_for_environment(elasticsearch)

        sdc_executor.add_pipeline(pipeline)

        if not stage_attributes['error_codes'][error_codes_mode] is None:
            with pytest.raises(ValidationError) as e:
                sdc_executor.validate_pipeline(pipeline)

            assert e.value.issues['issueCount'] == len(stage_attributes['error_codes'][error_codes_mode])
            for i in range(0, len(stage_attributes['error_codes'][error_codes_mode])):
                assert e.value.issues['stageIssues'][origin.instance_name][i]['message'].find(stage_attributes['error_codes'][error_codes_mode][i]) != -1

        else:
            doc_id = get_random_string(string.ascii_lowercase)

            elasticsearch.client.create_document(index=origin.index, id=doc_id, body={"number": 1})

            try:
                sdc_executor.start_pipeline(pipeline).wait_for_finished()

                assert len(wiretap.output_records) == 1

                record = wiretap.output_records[0]
                assert record.field['_id'] == doc_id
                assert record.field['_index'] == origin.index
                assert record.field['_source'] == {"number": 1}

            finally:
                elasticsearch.client.delete_index(origin.index)

    finally:
        sdc_executor.execute_shell(f'rm -frv {files_directory}')


@elasticsearch
@pytest.mark.parametrize('stage_attributes', [{'use_security': False}, {'use_security': True}])
def test_use_security(sdc_builder, sdc_executor, elasticsearch, stage_attributes):
    """
    To test the use security configuration we create a pipeline as follows:

    Elasticsearch >> Wiretap

    Since the Elasticsearch server requires using a username with a password
    an error should happen if the use security property is false.
    Otherwise we should succeed to find a document we have put before to an index.
    """

    index = get_random_string(string.ascii_lowercase)
    doc_id = get_random_string(string.ascii_lowercase)

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Elasticsearch', type='origin')
    origin.query = '{"query": {"match_all": {}}}'
    origin.index = index

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(elasticsearch)

    configured_origin = pipeline.stages.get(label=origin.label)
    configured_origin.use_security = stage_attributes['use_security']
    if not stage_attributes['use_security']:
        if Version(sdc_builder.version) < Version('3.17.0'):
            configured_origin.configuration['conf.securityConfig.securityUser'] = f':'
        else:
            configured_origin.user_name = ''
            configured_origin.password = ''

    sdc_executor.add_pipeline(pipeline)

    elasticsearch.client.create_document(index=index, id=doc_id, body={"number": 1})

    try:
        if stage_attributes['use_security']:
            sdc_executor.start_pipeline(pipeline).wait_for_finished()

            assert len(wiretap.output_records) == 1

            record = wiretap.output_records[0]
            assert record.field['_id'] == doc_id
            assert record.field['_index'] == index
            assert record.field['_source'] == {"number": 1}

        else:
            with pytest.raises(ValidationError) as e:
                sdc_executor.validate_pipeline(pipeline)

            assert e.value.issues['issueCount'] == 1
            assert e.value.issues['stageIssues'][origin.instance_name][0]['message'].find('ELASTICSEARCH_47') != -1

    finally:
        elasticsearch.client.delete_index(index)
