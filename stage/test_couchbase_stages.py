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

from streamsets.sdk.utils import Version
from streamsets.testframework.markers import couchbase, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@couchbase
@sdc_min_version('3.4.0')
def test_couchbase_destination(sdc_builder, sdc_executor, couchbase):
    """
    Send simple JSON text into Couchbase destination from Dev Raw Data Source and assert Couchbase has received it.

    The pipeline looks like:
        dev_raw_data_source >> couchbase_destination
    """
    couchbase_host = f'{couchbase.hostname}:{couchbase.port}'
    bucket_name = get_random_string(string.ascii_letters, 10)
    document_key_field = 'mydocname'
    raw_dict = dict(f1='abc', f2='xyz', f3='lmn')
    raw_dict[document_key_field] = 'mydocid'
    raw_data = json.dumps(raw_dict)

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
    couchbase_destination = builder.add_stage('Couchbase', type='destination')
    if Version(sdc_builder.version) < Version('3.9.0'):
        couchbase_destination.set_attributes(database_version='VERSION5', unique_document_key_field=document_key_field,
                                             bucket=bucket_name, couchbase_user_name=couchbase.username,
                                             couchbase_user_password=couchbase.password, url=couchbase_host)
    else:
        couchbase_destination.set_attributes(authentication_mode='USER', document_key="${record:value('/" + document_key_field + "')}",
                                             bucket=bucket_name, user_name=couchbase.username,
                                             password=couchbase.password, node_list=couchbase_host)

    dev_raw_data_source >> couchbase_destination
    pipeline = builder.build(title='Couchbase Destination pipeline').configure_for_environment(couchbase)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating %s Couchbase bucket ...', bucket_name)
        couchbase.admin.bucket_create(name=bucket_name, bucket_type='couchbase', ram_quota=256)
        couchbase.wait_for_healthy_bucket(bucket_name)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        bucket = couchbase.cluster.open_bucket(bucket_name)
        doc_value = bucket.get(raw_dict[document_key_field]).value
        assert doc_value == raw_dict
    finally:
        logger.info('Deleting %s Couchbase bucket ...', bucket_name)
        couchbase.admin.bucket_delete(bucket_name)
