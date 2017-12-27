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

"""The tests in this module are basic reusable functions which can be called by different relevant environments.
The test follow a pattern of creating pipelines with :py:obj:`testframework.sdc_models.PipelineBuilder` in one version
of SDC and then importing and running them in another.
"""

import json
import logging
import string

from testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def basic_solr_target(distribution, sdc_builder, sdc_executor, environment):
    """A reusable function to test Dev Raw Data Source to Solr target pipeline.
    Since the same doc is ingested, we can skip multiple writes by using deduplicator. The Pipeline looks like:

        dev_raw_data_source >> record_deduplicator >> solr_target
                                                   >> to_error
    """
    if distribution == 'cdh':  # in this case, environment would be CDH Cluster (ClouderaManagerCluster)
        client = environment.solr.client
        field_name_1 = 'id'  # mandatory to have an id of the document for CDH Solr schemaless
        # 'sample_collection' schemaless collection has to be pre-created in CDH Solr.
        # clusterdock CDH image has this already built in. Some useful links around this:
        # https://www.cloudera.com/documentation/enterprise/5-10-x/topics/search_validate_deploy_solr_rest_api.html
        # https://www.cloudera.com/documentation/enterprise/5-10-x/topics/search_solrctl_managing_solr.html#concept_l3y_txb_mt
        # https://www.cloudera.com/documentation/enterprise/5-10-x/topics/search_faq.html#faq_search_general_schemalesserror
        solr_collection_name = 'sample_collection'
    elif distribution == 'apache':  # in this case, environment would be SolrInstance
        client = environment.client
        field_name_1 = get_random_string(string.ascii_letters, 10)
        solr_collection_name = environment.core_name  # single instance of Solr, collection will be same as core

    field_name_2 = get_random_string(string.ascii_letters, 10)
    field_val_1 = get_random_string(string.ascii_letters, 10)
    field_val_2 = get_random_string(string.ascii_letters, 10)
    json_fields_map = [{'field': '/id', 'solrFieldName': field_name_1},
                       {'field': '/title', 'solrFieldName': field_name_2}]
    json_str = json.dumps(dict(id=field_val_1, title=field_val_2))
    solr_url = client.host.rstrip('/')  # In single node mode this will be in a URL format
    sdc_solr_uri = ''.join([solr_url, '/', solr_collection_name])

    # build Solr target pipeline
    builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON',
                                                                                  raw_data=json_str)

    record_deduplicator = builder.add_stage('Record Deduplicator')
    to_error = builder.add_stage('To Error')

    solr_target = builder.add_stage('Solr', type='destination')
    solr_target.set_attributes(instance_type='SINGLE_NODE', indexing_mode='RECORD', fields=json_fields_map,
                               solr_uri=sdc_solr_uri)

    dev_raw_data_source >> record_deduplicator >> solr_target
    record_deduplicator >> to_error

    solr_dest_pipeline = builder.build(title='Solr Target pipeline').configure_for_environment(environment)
    sdc_executor.add_pipeline(solr_dest_pipeline)

    # assert data ingested into Solr
    try:
        sdc_executor.start_pipeline(solr_dest_pipeline).wait_for_pipeline_batch_count(1)
        sdc_executor.stop_pipeline(solr_dest_pipeline)

        query = f'{{!term f={field_name_1}}}{field_val_1}'
        resp = client.query(solr_collection_name, {'q': query})
        assert resp.num_found > 0
        assert resp.data['response']['docs'][0][field_name_2][0] == field_val_2
    finally:
        if distribution == 'cdh':
            # delete field data. Delete field, schema and collection are not supported by CDH 5.x Solr 4.x version.
            client.delete_doc_by_id(solr_collection_name, field_val_1)
        elif distribution == 'apache':
            # cleanup the fields created by the test
            schema = client.schema
            schema.delete_field(solr_collection_name, field_name_1)
            schema.delete_field(solr_collection_name, field_name_2)
