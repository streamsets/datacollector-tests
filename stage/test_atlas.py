# Copyright 2019 StreamSets Inc.
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
from streamsets.sdk.utils import wait_for_condition
from streamsets.testframework.environments.hortonworks import AmbariCluster
from streamsets.testframework.markers import cluster, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def atlas_check(cluster):
    if isinstance(cluster, AmbariCluster) and not hasattr(cluster, 'atlas'):
        pytest.skip('Atlas tests require Atlas to be installed on the cluster')


@cluster('hdp')
@sdc_min_version('3.2.0')
def test_basic_local_fs(sdc_builder, sdc_executor, cluster):
    """A simple basic test to see if Atlas records governance data. We do so by wiring following pipeline stages
    which have metadata support and then query Atlas to see if the entity has expected data. Pipeline looks like:

        dev_data_generator >> local_fs
        dev_data_generator >= pipeline_finisher
    """
    tmp_directory = '/tmp/out/{}'.format(get_random_string(string.ascii_letters, 10))
    pipeline_title = 'Atlas basic test pipeline {}'.format(get_random_string(string.ascii_letters, 10))

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(batch_size=100, delay_between_batches=10,
                                      fields_to_generate=[{'field': 'text', 'type': 'STRING'}])

    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='TEXT', directory_template=tmp_directory)

    # Only when pipeline finishes, SDC publishes the metadata to Atlas
    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')

    dev_data_generator >> local_fs
    dev_data_generator >= pipeline_finisher

    pipeline = pipeline_builder.build(pipeline_title).configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    client = cluster.atlas.client

    query_data = {
        'typeName': 'streamsets_datastore',
        'entityFilters': {'condition': 'AND',
                          'criterion': [{'attributeName': 'description',
                                         'operator': 'eq', 'attributeValue': pipeline.id}]}}
    def condition():
        search_results = client.search_basic.create(data=query_data)
        return len(search_results.entities.to_dict()) > 0
    # need to wait for sometime before Atlas gets metadata
    wait_for_condition(condition=condition, timeout=30)

    # assert 'streamsets_datastore' entities (stages)
    search_results = client.search_basic.create(data=query_data)
    ds_entities = search_results.entities.to_dict()
    assert ds_entities[0]['attributes']['owner'] == 'admin'

    entity = client.entity_guid(ds_entities[0]['guid'])
    assert entity.entity['attributes']['provider'].startswith('StreamSets')

    # assert specific LocalFS entity attribute
    query_data = {
        'typeName': 'streamsets_datastore',
        'entityFilters': {'condition': 'AND',
                          'criterion': [{'attributeName': 'stageName',
                                         'operator': 'eq', 'attributeValue': 'LocalFS_01'},
                                        {'attributeName': 'description',
                                         'operator': 'eq', 'attributeValue': pipeline.id}]}}
    search_results = client.search_basic.create(data=query_data)
    local_fs_entity = client.entity_guid(search_results.entities.to_dict()[0]['guid'])
    assert local_fs_entity.entity['attributes']['storeDescription'].startswith(tmp_directory)

    # assert 'streamsets_process' entity (pipeline)
    query_data = {
        'typeName': 'streamsets_process',
        'entityFilters': {'condition': 'AND',
                          'criterion': [{'attributeName': 'name',
                                         'operator': 'startsWith', 'attributeValue': pipeline_title}]}}
    search_results = client.search_basic.create(data=query_data)
    pipeline_entity = search_results.entities.to_dict()[0]
    pipeline_entity['attributes']['owner'] == 'admin'

    entity = client.entity_guid(pipeline_entity['guid'])
    assert entity.entity['attributes']['provider'].startswith('StreamSets')
    assert entity.entity['attributes']['processName'] == pipeline_title

    client.entity_bulk.delete(guid=([entity['guid'] for entity in ds_entities] + [pipeline_entity['guid']]))
