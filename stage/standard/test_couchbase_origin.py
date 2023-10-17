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

import datetime
import logging
import string
import time

import pytest

from streamsets.testframework.markers import couchbase, sdc_min_version
from streamsets.testframework.utils import get_random_string
from couchbase.management.buckets import CreateBucketSettings
from couchbase.management.collections import CollectionSpec

logger = logging.getLogger(__name__)

pytestmark = [couchbase, sdc_min_version('5.8.0')]

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

# Reference: https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/datatypes.html
# Couchbase doesn't really care about types, as it holds JSONs. The deserializer used is provided by Couchbase,
# so as long as the data type is being held by them, we can read it. In order to create a document with any value,
# the value must be serializable (so no date, byte, etc)
# Therefore, adding some common cases with NULL and empty values.
DATA_TYPES = [
    # inserted value                type
    (True,                         'BOOLEAN'),
    (123412341,                    'LONG'),
    (12312.1231,                   'DOUBLE'),
    (12312,                        'LONG'),
    ('2020-02-03',                 'DATE'),
    ('10:02:06',                   'TIME'),
    ('2020-02-03 11:12:13',        'DATETIME'),
    ('2020-01-01T10:00:00+00:12',  'ZONED_DATETIME'),
    ('string',                     'STRING'),
    (None,                         'NULL'),
    ('',                           'EMPTY'),
]

COMPLEX_TYPES = [
    # inserted value                type
    (['list_item_1', 'list_item_2'], 'LIST'),
    ({'object_key_1': 'object_value_1', 'object_key_2': 'object_value_2'}, 'OBJECT'),
    ({'object_key_1': ['list_item_1', 'list_item_2'], 'object_key_2': 'object_value_2'}, 'LIST_OF_OBJECTS_WITH_LISTS'),
]

# Reference: https://docs.couchbase.com/server/current/manage/manage-buckets/create-bucket.html
COUCHBASE_BUCKET_NAMES = [
    ('maxsize', lambda: get_random_string(string.ascii_lowercase, 100)),
    ('lowercase', lambda: get_random_string(string.ascii_lowercase)),
    ('uppercase', lambda: get_random_string(string.ascii_uppercase)),
    ('hyphen', lambda: get_random_string(string.ascii_lowercase) + '-' + get_random_string(string.ascii_lowercase)),
    ('period', lambda: get_random_string(string.ascii_lowercase) + '.' + get_random_string(string.ascii_lowercase)),
    ('underscore', lambda: get_random_string(string.ascii_lowercase) + '_' + get_random_string(string.ascii_lowercase)),
    ('percent', lambda: get_random_string(string.ascii_lowercase) + '%' + get_random_string(string.ascii_lowercase)),
    ('digits', lambda: get_random_string(string.digits))
]

COUCHBASE_SCOPE_AND_COLLECTION_NAMES = [
    ('maxsize', lambda: get_random_string(string.ascii_lowercase, 100)),
    ('lowercase', lambda: get_random_string(string.ascii_lowercase)),
    ('uppercase', lambda: get_random_string(string.ascii_uppercase)),
    ('hyphen', lambda: get_random_string(string.ascii_lowercase) + '-' + get_random_string(string.ascii_lowercase)),
    ('underscore', lambda: get_random_string(string.ascii_lowercase) + '_' + get_random_string(string.ascii_lowercase)),
    ('percent', lambda: get_random_string(string.ascii_lowercase) + '%' + get_random_string(string.ascii_lowercase)),
    ('digits', lambda: get_random_string(string.digits))
]


def get_bucket_config(bucket, scope=DEFAULT_SCOPE, collection=DEFAULT_COLLECTION, where=None, order_by='META().id'):
    return {
        'bucket': bucket,
        'scope': scope,
        'collection': collection,
        'where': where if where is not None else [],
        'order': order_by
    }


def create_bucket(couchbase, bucket_name, scope_name=DEFAULT_SCOPE, collection_name=DEFAULT_COLLECTION,
                  ram_quota_mb=128, create_primary_index=True):
    logger.info(f'Creating {bucket_name} Couchbase bucket...')
    couchbase.bucket_manager.create_bucket(CreateBucketSettings(name=bucket_name,
                                                                bucket_type='couchbase',
                                                                ram_quota_mb=ram_quota_mb))
    couchbase.wait_for_healthy_bucket(bucket_name)
    bucket = couchbase.cluster.bucket(bucket_name)
    if scope_name != DEFAULT_SCOPE:
        logger.info(f'Creating {scope_name} scope in {bucket_name} Couchbase bucket...')
        bucket.collections().create_scope(scope_name)
    if collection_name != DEFAULT_COLLECTION:
        logger.info(
            f'Creating {collection_name} collection in {scope_name} scope in {bucket_name} Couchbase bucket...')
        bucket.collections().create_collection(
            CollectionSpec(collection_name=collection_name, scope_name=scope_name))
    if create_primary_index:
        logger.info(
            f'Creating PRIMARY INDEX on `{bucket_name}`.`{scope_name}`.`{collection_name}` Couchbase bucket ...')
        couchbase.cluster.query(f'CREATE PRIMARY INDEX ON `{bucket_name}`.`{scope_name}`.`{collection_name}`').execute()
    return bucket


def insert_documents(bucket, document_ids, documents, scope=DEFAULT_SCOPE, collection=DEFAULT_COLLECTION):
    logger.info(f'Inserting {len(documents)} documents into Couchbase {bucket.name}.')
    for document_key, document in zip(document_ids, documents):
        bucket.scope(scope).collection(collection).insert(document_key, document)


@pytest.mark.parametrize('data_types', [
    pytest.param(DATA_TYPES, id="DATA_TYPES"),
    pytest.param(COMPLEX_TYPES, id="COMPLEX_TYPES")
])
def test_data_types(sdc_builder, sdc_executor, couchbase, data_types):
    """ Test for Couchbase Origin data types. The pipeline looks like:

    couchbase_origin >> wiretap
    """
    bucket_name = get_random_string(string.ascii_letters, 10)
    document_ids = [get_random_string()]
    documents = [{data_type[1]: data_type[0] for data_type in data_types}]
    try:
        bucket = create_bucket(couchbase, bucket_name)
        insert_documents(bucket, document_ids, documents)

        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        couchbase_origin = builder.add_stage(name=STAGE_NAME)
        couchbase_origin.set_attributes(buckets=[get_bucket_config(bucket_name)])

        wiretap = builder.add_wiretap()
        finisher = builder.add_stage('Pipeline Finisher Executor')
        finisher.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

        couchbase_origin >> wiretap.destination
        couchbase_origin >= finisher

        pipeline = builder.build().configure_for_environment(couchbase)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        assert 1 == len(records), f'1 records should have been processed but {len(records)} were found'
        assert records[0].header.values[FLAGS_HEADER]
        assert records[0].header.values[EXPIRATION_HEADER] == '0'
        assert records[0].header.values[SCOPE_HEADER] == DEFAULT_SCOPE
        assert records[0].header.values[COLLECTION_HEADER] == DEFAULT_COLLECTION
        assert records[0].header.values[BUCKET_HEADER] == bucket_name
        assert records[0].header.values[DOCUMENT_ID_HEADER] == document_ids[0]
        assert records[0].header.values[
                   KEYSPACE_HEADER] == f'default:{bucket_name}.{DEFAULT_SCOPE}.{DEFAULT_COLLECTION}'
        assert records[0].header.values[CAS_HEADER]
        assert records[0].header.values[TYPE_HEADER] == 'json'

        for record_key, document_key in zip(records[0].field.keys(), documents[0].keys()):
            assert record_key == document_key
            assert records[0].field[record_key] == documents[0][document_key]
    finally:
        logger.info('Deleting %s Couchbase bucket ...', bucket_name)
        couchbase.bucket_manager.drop_bucket(bucket_name)


@pytest.mark.parametrize('test_name, couchbase_name_generator', COUCHBASE_BUCKET_NAMES,
                         ids=[i[0] for i in COUCHBASE_BUCKET_NAMES])
def test_bucket_names(sdc_builder, sdc_executor, couchbase, test_name, couchbase_name_generator):
    """ Test for Couchbase Origin bucket names. The pipeline looks like:

    couchbase_origin >> wiretap
    """
    document_ids = DEFAULT_DOCUMENT_IDS
    documents = DEFAULT_DOCUMENTS
    entity_name = couchbase_name_generator()
    try:
        bucket = create_bucket(couchbase, entity_name, scope_name=entity_name, collection_name=entity_name)
        insert_documents(bucket, document_ids, documents, scope=entity_name, collection=entity_name)

        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        couchbase_origin = builder.add_stage(name=STAGE_NAME)
        couchbase_origin.set_attributes(
            buckets=[get_bucket_config(entity_name, scope=entity_name, collection=entity_name)])

        wiretap = builder.add_wiretap()
        finisher = builder.add_stage('Pipeline Finisher Executor')
        finisher.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

        couchbase_origin >> wiretap.destination
        couchbase_origin >= finisher

        pipeline = builder.build().configure_for_environment(couchbase)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        assert len(documents) == len(records), \
            f'{len(documents)} records should have been processed but {len(records)} were found'
        for record, document_id, document in zip(records, document_ids, documents):
            assert record.header.values[FLAGS_HEADER]
            assert record.header.values[EXPIRATION_HEADER] == '0'
            assert record.header.values[SCOPE_HEADER] == entity_name
            assert record.header.values[COLLECTION_HEADER] == entity_name
            assert record.header.values[BUCKET_HEADER] == entity_name
            assert record.header.values[DOCUMENT_ID_HEADER] == document_id
            assert record.header.values[
                       KEYSPACE_HEADER] == f'default:{entity_name}.{entity_name}.{entity_name}'
            assert record.header.values[CAS_HEADER]
            assert record.header.values[TYPE_HEADER] == 'json'

            for record_key, document_key in zip(record.field.keys(), document.keys()):
                assert record_key == document_key
                assert record.field[record_key] == document[document_key]
    finally:
        logger.info('Deleting %s Couchbase bucket ...', entity_name)
        couchbase.bucket_manager.drop_bucket(entity_name)


@pytest.mark.parametrize('test_name, couchbase_name_generator', COUCHBASE_SCOPE_AND_COLLECTION_NAMES,
                         ids=[i[0] for i in COUCHBASE_SCOPE_AND_COLLECTION_NAMES])
def test_scope_and_collection_names(sdc_builder, sdc_executor, couchbase, test_name, couchbase_name_generator):
    """ Test for Couchbase Origin scope and collection names. The pipeline looks like:

    couchbase_origin >> wiretap
    """
    bucket_name = get_random_string(string.ascii_letters, 10)
    document_ids = DEFAULT_DOCUMENT_IDS
    documents = DEFAULT_DOCUMENTS
    entity_name = couchbase_name_generator()
    try:
        bucket = create_bucket(couchbase, bucket_name, scope_name=entity_name, collection_name=entity_name)
        insert_documents(bucket, document_ids, documents, scope=entity_name, collection=entity_name)

        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        couchbase_origin = builder.add_stage(name=STAGE_NAME)
        couchbase_origin.set_attributes(
            buckets=[get_bucket_config(bucket_name, scope=entity_name, collection=entity_name)])

        wiretap = builder.add_wiretap()
        finisher = builder.add_stage('Pipeline Finisher Executor')
        finisher.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

        couchbase_origin >> wiretap.destination
        couchbase_origin >= finisher

        pipeline = builder.build().configure_for_environment(couchbase)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        assert len(documents) == len(records), \
            f'{len(documents)} records should have been processed but {len(records)} were found'
        for record, document_id, document in zip(records, document_ids, documents):
            assert record.header.values[FLAGS_HEADER]
            assert record.header.values[EXPIRATION_HEADER] == '0'
            assert record.header.values[SCOPE_HEADER] == entity_name
            assert record.header.values[COLLECTION_HEADER] == entity_name
            assert record.header.values[BUCKET_HEADER] == bucket_name
            assert record.header.values[DOCUMENT_ID_HEADER] == document_id
            assert record.header.values[
                       KEYSPACE_HEADER] == f'default:{bucket_name}.{entity_name}.{entity_name}'
            assert record.header.values[CAS_HEADER]
            assert record.header.values[TYPE_HEADER] == 'json'

            for record_key, document_key in zip(record.field.keys(), document.keys()):
                assert record_key == document_key
                assert record.field[record_key] == document[document_key]
    finally:
        logger.info('Deleting %s Couchbase bucket ...', bucket_name)
        couchbase.bucket_manager.drop_bucket(bucket_name)


@pytest.mark.parametrize('batch_size', [1, 2])
def test_multiple_batches(sdc_builder, sdc_executor, couchbase, batch_size):
    """ Test for Couchbase Origin batch size. The pipeline looks like:

    couchbase_origin >> trash
    """
    bucket_name = get_random_string(string.ascii_letters, 10)
    document_ids = ['doc_1', 'doc_2', 'doc_3', 'doc_4']
    documents = [{'id': 1, 'name': 'Roger Federer'}, {'id': 2, 'name': 'Rafael Nadal'},
                 {'id': 3, 'name': 'Carlos Alcaraz'}, {'id': 4, 'name': 'Joaquin Bo'}]
    batch_count = len(documents) / batch_size + 1  # 1 event batch is generated
    try:
        bucket = create_bucket(couchbase, bucket_name)
        insert_documents(bucket, document_ids, documents)

        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        couchbase_origin = builder.add_stage(name=STAGE_NAME)
        couchbase_origin.set_attributes(buckets=[get_bucket_config(bucket_name)],
                                        max_batch_size_in_records=batch_size)

        trash = builder.add_stage('Trash')

        couchbase_origin >> trash

        pipeline = builder.build().configure_for_environment(couchbase)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(documents))
        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchCount.counter').count == batch_count
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == len(documents)
    finally:
        logger.info('Deleting %s Couchbase bucket ...', bucket_name)
        couchbase.bucket_manager.drop_bucket(bucket_name)


def test_empty_bucket(sdc_builder, sdc_executor, couchbase):
    """ Test for Couchbase Origin batch size. The pipeline looks like:

    couchbase_origin >> trash
    """
    bucket_name = get_random_string(string.ascii_letters, 10)
    try:
        bucket = create_bucket(couchbase, bucket_name)

        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        couchbase_origin = builder.add_stage(name=STAGE_NAME)
        couchbase_origin.set_attributes(buckets=[get_bucket_config(bucket_name)])

        trash = builder.add_stage('Trash')

        couchbase_origin >> trash

        pipeline = builder.build().configure_for_environment(couchbase)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        time.sleep(15)
        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('pipeline.batchCount.counter').count == 0
        assert history.latest.metrics.counter('pipeline.batchInputRecords.counter').count == 0
        assert history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count == 0
    finally:
        logger.info('Deleting %s Couchbase bucket ...', bucket_name)
        couchbase.bucket_manager.drop_bucket(bucket_name)


def test_dataflow_events(sdc_builder, sdc_executor, couchbase):
    pytest.skip('Already tested in stage/test_couchbase_origin.py::test_events')


def test_data_formats(sdc_builder, sdc_executor, couchbase):
    pytest.skip('Couchbase Origin does not support data formats')


def test_multiple_threads(sdc_builder, sdc_executor, snowflake):
    pytest.skip("Already tested in  stage/test_couchbase_origin.py::test_big_amounts_of_records")
