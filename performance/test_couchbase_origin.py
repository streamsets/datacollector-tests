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
import pytest

from streamsets.testframework.markers import couchbase, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

pytestmark = [couchbase, sdc_min_version('5.8.0')]

SUPPORTED_LIBS = ['streamsets-datacollector-couchbase_3-lib']
STAGE_NAME = 'com_streamsets_pipeline_stage_origin_couchbase_CouchbaseDSource'

DEFAULT_SCOPE = '_default'
DEFAULT_COLLECTION = '_default'

FLAGS_HEADER = 'couchbase.flags'
EXPIRATION_HEADER = 'couchbase.expiration'
SCOPE_HEADER = 'couchbase.scope'
COLLECTION_HEADER = 'couchbase.collection'
BUCKET_HEADER = 'couchbase.bucket'
DOCUMENT_ID_HEADER = 'couchbase.document.id'
KEYSPACE_HEADER = 'couchbase.keyspace'
CAS_HEADER = 'couchbase.cas'
TYPE_HEADER = 'couchbase.type'

DEFAULT_DOCUMENT_IDS = ['doc_1', 'doc_2', 'doc_3']
DEFAULT_DOCUMENTS = [{'id': 1, 'name': 'Roger Federer'}, {'id': 2, 'name': 'Rafael Nadal'},
                     {'id': 3, 'name': 'Carlos Alcaraz'}]


@pytest.fixture(autouse=True, scope='module')
def init(couchbase):
    for lib in couchbase.sdc_stage_libs:
        if lib in SUPPORTED_LIBS:
            couchbase.pre_create_buckets()
            return
    pytest.skip(f'Couchbase Origin test requires using libraries in {SUPPORTED_LIBS}')


def get_bucket_config(bucket, scope=DEFAULT_SCOPE, collection=DEFAULT_COLLECTION, where=None, order_by='META().id'):
    return {
        'bucket': bucket,
        'scope': scope,
        'collection': collection,
        'where': where if where is not None else [],
        'order': order_by
    }


@pytest.mark.parametrize('num_threads', [1, 5, 10])
@pytest.mark.parametrize('num_buckets, num_records, batch_size', [(1, 100_000, 1000), (3, 500_000, 5000)])
def test_big_amounts_of_records(sdc_builder, sdc_executor, couchbase, num_threads, num_buckets, num_records,
                                batch_size):
    """ Performance test for Couchbase Origin. The pipeline looks like:

    couchbase_origin >> trash
    """
    try:
        bucket_names = []
        for _ in range(0, num_buckets):
            bucket_name = couchbase.get_bucket()
            bucket_document_ids = []
            bucket_documents = []
            for _ in range(0, num_records):
                bucket_document_ids.append(get_random_string())
                bucket_documents.append({'id': f'id_{get_random_string()}', 'name': f'name_{get_random_string()}'})
            couchbase.insert_documents(bucket_document_ids, bucket_documents, bucket_name)
            bucket_names.append(bucket_name)

        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        couchbase_origin = builder.add_stage(name=STAGE_NAME)
        couchbase_origin.set_attributes(buckets=[get_bucket_config(bucket_name) for bucket_name in bucket_names],
                                        number_of_threads=num_threads,
                                        max_batch_size_in_records=batch_size)
        trash = builder.add_stage('Trash')

        couchbase_origin >> trash

        pipeline = builder.build().configure_for_environment(couchbase)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.benchmark_pipeline(pipeline, record_count=num_records)
    finally:
        couchbase.cleanup_buckets()
