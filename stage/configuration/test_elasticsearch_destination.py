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

import json
import logging
import string

import pytest
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import elasticsearch, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

ELASTICSEARCH_VERSION_8 = 8


@pytest.fixture(scope='function')
def test_data():
    yield [{
        "text": "Record1",
        "index": get_random_string(string.ascii_letters, 10).lower(),
        "mapping": get_random_string(string.ascii_letters, 10).lower(),
        "doc_id": get_random_string(string.ascii_letters, 10).lower(),
        "shard": get_random_string(string.ascii_letters, 10).lower(),
    }, {
        "text": "Record2",
        "index": get_random_string(string.ascii_letters, 10).lower(),
        "mapping": get_random_string(string.ascii_letters, 10).lower(),
        "doc_id": get_random_string(string.ascii_letters, 10).lower(),
        "shard": get_random_string(string.ascii_letters, 10).lower(),
    }, {
        "text": "Record3",
        "index": get_random_string(string.ascii_letters, 10).lower(),
        "mapping": get_random_string(string.ascii_letters, 10).lower(),
        "doc_id": get_random_string(string.ascii_letters, 10).lower(),
        "shard": get_random_string(string.ascii_letters, 10).lower(),
    }, {
        "text": "Record4",
        "index": get_random_string(string.ascii_letters, 10).lower(),
        "mapping": get_random_string(string.ascii_letters, 10).lower(),
        "doc_id": get_random_string(string.ascii_letters, 10).lower(),
        "shard": None
    }]


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'AWSSIGV4', 'use_security': True}])
def test_access_key_id(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_additional_http_params(sdc_builder, sdc_executor):
    pass


@elasticsearch
@sdc_min_version('3.7.0')
def test_additional_properties(sdc_builder, sdc_executor, elasticsearch, test_data):
    index = get_random_string(string.ascii_letters, 10).lower()
    mapping = get_random_string(string.ascii_letters, 10).lower()

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('Dev Raw Data Source')
    source.data_format = 'JSON'
    source.stop_after_first_batch = True
    source.raw_data = '\n'.join(json.dumps(rec) for rec in test_data)

    target = builder.add_stage('Elasticsearch', type='destination')
    target.default_operation = 'INDEX'
    target.index = index
    # Main config change for this test
    target.additional_properties = '{\"routing\":${record:value(\'/shard\')}}'

    if elasticsearch.major_version < ELASTICSEARCH_VERSION_8:
        target.mapping = mapping

    source >> target
    pipeline = builder.build().configure_for_environment(elasticsearch)

    sdc_executor.add_pipeline(pipeline)

    try:
        elasticsearch.client.create_index(index)

        # Run pipeline with additional properties
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        hits = searchAndSort(elasticsearch, index)
        assert len(hits) == 4

        for i in range(4):
            assert hits[i]['_index'] == index
            assert hits[i]['_source']['text'] == test_data[i]['text']
            if elasticsearch.major_version < ELASTICSEARCH_VERSION_8:
                assert hits[i]['_type'] == mapping

            # First three records have the value "shard" filled whereas the last record (id=3) does not and thus that
            # piece of metadata should never made it ElasticSearch.
            if i == 3:
                assert '_routing' not in hits[i]
            else:
                assert hits[i]['_routing'] == test_data[i]['shard']
    finally:
        # Clean up test data in ES
        elasticsearch.client.delete_index(index)


@stub
def test_cluster_http_uris(sdc_builder, sdc_executor):
    pass


@stub
def test_data_charset(sdc_builder, sdc_executor):
    pass


@stub
def test_data_time_zone(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'default_operation': 'CREATE'},
                                              {'default_operation': 'DELETE'},
                                              {'default_operation': 'INDEX'},
                                              {'default_operation': 'MERGE'},
                                              {'default_operation': 'UPDATE'}])
def test_default_operation(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'detect_additional_nodes_in_cluster': False},
                                              {'detect_additional_nodes_in_cluster': True}])
def test_detect_additional_nodes_in_cluster(sdc_builder, sdc_executor, stage_attributes):
    pass


@elasticsearch
def test_document_id(sdc_builder, sdc_executor, elasticsearch, test_data):
    index = get_random_string(string.ascii_letters, 10).lower()
    mapping = get_random_string(string.ascii_letters, 10).lower()

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('Dev Raw Data Source')
    source.data_format = 'JSON'
    source.stop_after_first_batch = True
    source.raw_data = '\n'.join(json.dumps(rec) for rec in test_data)

    target = builder.add_stage('Elasticsearch', type='destination')
    target.default_operation = 'INDEX'
    target.index = index
    # Main config change for this test
    target.document_id = '${record:value(\'/doc_id\')}'

    if elasticsearch.major_version < ELASTICSEARCH_VERSION_8:
        target.mapping = mapping

    source >> target
    pipeline = builder.build().configure_for_environment(elasticsearch)

    sdc_executor.add_pipeline(pipeline)

    try:
        elasticsearch.client.create_index(index)

        # Run pipeline with additional properties
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        hits = searchAndSort(elasticsearch, index)
        assert len(hits) == 4

        for i in range(4):
            assert hits[i]['_index'] == index
            assert hits[i]['_id'] == test_data[i]['doc_id']
            assert hits[i]['_source']['text'] == test_data[i]['text']
            if elasticsearch.major_version < ELASTICSEARCH_VERSION_8:
                assert hits[i]['_type'] == mapping
    finally:
        # Clean up test data in ES
        elasticsearch.client.delete_index(index)


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'AWSSIGV4', 'region': 'OTHER', 'use_security': True}])
def test_endpoint(sdc_builder, sdc_executor, stage_attributes):
    pass


@elasticsearch
def test_index(sdc_builder, sdc_executor, elasticsearch, test_data):
    mapping = get_random_string(string.ascii_letters, 10).lower()

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('Dev Raw Data Source')
    source.data_format = 'JSON'
    source.stop_after_first_batch = True
    source.raw_data = '\n'.join(json.dumps(rec) for rec in test_data)

    target = builder.add_stage('Elasticsearch', type='destination')
    target.default_operation = 'INDEX'
    # For this test, we set index to EL
    target.index = '${record:value(\'/index\')}'
    if elasticsearch.major_version < ELASTICSEARCH_VERSION_8:
        target.mapping = mapping

    source >> target
    pipeline = builder.build().configure_for_environment(elasticsearch)

    sdc_executor.add_pipeline(pipeline)

    try:
        for entry in test_data:
            elasticsearch.client.create_index(entry['index'])

        # Run pipeline with additional properties
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        hits = searchAndSort(elasticsearch, ','.join(entry['index'] for entry in test_data))
        assert len(hits) == 4

        for i in range(4):
            assert hits[i]['_index'] == test_data[i]['index']
            assert hits[i]['_source']['text'] == test_data[i]['text']
            if elasticsearch.major_version < ELASTICSEARCH_VERSION_8:
                assert hits[i]['_type'] == mapping
    finally:
        # Clean up test data in ES
        for entry in test_data:
            elasticsearch.client.delete_index(entry['index'])


# Mappings were removed in 6 and thus ELs are only supported in 5 and totally removed in 8
# https://www.elastic.co/guide/en/elasticsearch/reference/6.0/removal-of-types.html
@elasticsearch
def test_mapping(sdc_builder, sdc_executor, elasticsearch, test_data):
    mapping = get_random_string(string.ascii_letters, 10).lower()
    index = get_random_string(string.ascii_letters, 10).lower()

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('Dev Raw Data Source')
    source.data_format = 'JSON'
    source.stop_after_first_batch = True
    source.raw_data = '\n'.join(json.dumps(rec) for rec in test_data)

    target = builder.add_stage('Elasticsearch', type='destination')
    target.default_operation = 'INDEX'
    target.index = index

    # For this test, we set mapping to EL

    if elasticsearch.major_version == 5:
        target.mapping = '${record:value(\'/mapping\')}'
    elif elasticsearch.major_version < ELASTICSEARCH_VERSION_8:
        target.mapping = mapping

    source >> target
    pipeline = builder.build().configure_for_environment(elasticsearch)

    sdc_executor.add_pipeline(pipeline)

    try:
        elasticsearch.client.create_index(index)

        # Run pipeline with additional properties
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        hits = searchAndSort(elasticsearch, index)
        assert len(hits) == 4

        for i in range(4):
            assert hits[i]['_index'] == index
            assert hits[i]['_source']['text'] == test_data[i]['text']

            if elasticsearch.major_version == 5:
                assert hits[i]['_type'] == test_data[i]['mapping']
            elif elasticsearch.major_version < ELASTICSEARCH_VERSION_8:
                assert hits[i]['_type'] == mapping
    finally:
        elasticsearch.client.delete_index(index)


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'AWSSIGV4', 'use_security': True},
                                              {'mode': 'BASIC', 'use_security': True}])
def test_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_parent_id(sdc_builder, sdc_executor):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
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
def test_required_fields(sdc_builder, sdc_executor):
    pass


@elasticsearch
def test_routing(sdc_builder, sdc_executor, elasticsearch, test_data):
    index = get_random_string(string.ascii_letters, 10).lower()
    mapping = get_random_string(string.ascii_letters, 10).lower()

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('Dev Raw Data Source')
    source.data_format = 'JSON'
    source.stop_after_first_batch = True
    source.raw_data = '\n'.join(json.dumps(rec) for rec in test_data)

    target = builder.add_stage('Elasticsearch', type='destination')
    target.default_operation = 'INDEX'
    target.index = index
    # We send dynamic routing based on the records itself, this is the main change in this test
    target.routing = '${record:value(\'/shard\')}'

    if elasticsearch.major_version < ELASTICSEARCH_VERSION_8:
        target.mapping = mapping

    source >> target
    pipeline = builder.build().configure_for_environment(elasticsearch)

    sdc_executor.add_pipeline(pipeline)

    try:
        elasticsearch.client.create_index(index)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        hits = searchAndSort(elasticsearch, index)
        assert len(hits) == 4

        for i in range(4):
            assert hits[i]['_index'] == index
            assert hits[i]['_source']['text'] == test_data[i]['text']
            if elasticsearch.major_version < ELASTICSEARCH_VERSION_8:
                assert hits[i]['_type'] == mapping

            # First three records have the value "shard" filled whereas the last record (id=3) does not and thus that
            # piece of metadata should never made it ElasticSearch.
            if i == 3:
                assert '_routing' not in hits[i]
            else:
                assert hits[0]['_routing'] == test_data[0]['shard']
    finally:
        # Clean up test data in ES
        elasticsearch.client.delete_index(index)


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'AWSSIGV4', 'use_security': True}])
def test_secret_access_key(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'BASIC', 'use_security': True}])
def test_security_username_and_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_security': True}])
def test_ssl_truststore_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_security': True}])
def test_ssl_truststore_path(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_time_basis(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'unsupported_operation_handling': 'DISCARD'},
                                              {'unsupported_operation_handling': 'SEND_TO_ERROR'},
                                              {'unsupported_operation_handling': 'USE_DEFAULT'}])
def test_unsupported_operation_handling(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_security': False}, {'use_security': True}])
def test_use_security(sdc_builder, sdc_executor, stage_attributes):
    pass


def searchAndSort(elasticsearch, index):
    def _sort_response(entry):
        return entry['_source']['text']

    hits = elasticsearch.client.search(index)
    hits.sort(key=_sort_response)
    return hits
