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

from streamsets.testframework.utils import get_random_string
from streamsets.testframework.markers import solr

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
        # cleanup the fields created by the test.
        client.delete(id=field_name_1)
        client.delete(id=field_name_2)
