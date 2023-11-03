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
import string
import pytest

from streamsets.testframework.markers import couchbase, sdc_min_version
from streamsets.testframework.utils import get_random_string
from couchbase.management.buckets import CreateBucketSettings
from couchbase.management.collections import CollectionSpec

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


@pytest.fixture(autouse=True)
def library_check(couchbase):
    for lib in couchbase.sdc_stage_libs:
        if lib in SUPPORTED_LIBS:
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
    collection = bucket.scope(scope).collection(collection)
    for document_key, document in zip(document_ids, documents):
        collection.insert(document_key, document)


def test_basic(sdc_builder, sdc_executor, couchbase):
    """ Basic test for Couchbase Origin. We create some data in Couchbase, and then read it and assert it is correct.
    We also assert that headers are there, with the expected values. The pipeline looks like:

    couchbase_origin >> wiretap
    """
    bucket_name = get_random_string(string.ascii_letters, 10)
    document_ids = DEFAULT_DOCUMENT_IDS
    documents = DEFAULT_DOCUMENTS
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
        assert len(documents) == len(records), \
            f'{len(documents)} records should have been processed but {len(records)} were found'
        for record, document_id, document in zip(records, document_ids, documents):
            assert record.header.values[FLAGS_HEADER]
            assert record.header.values[EXPIRATION_HEADER] == '0'
            assert record.header.values[SCOPE_HEADER] == DEFAULT_SCOPE
            assert record.header.values[COLLECTION_HEADER] == DEFAULT_COLLECTION
            assert record.header.values[BUCKET_HEADER] == bucket_name
            assert record.header.values[DOCUMENT_ID_HEADER] == document_id
            assert record.header.values[
                       KEYSPACE_HEADER] == f'default:{bucket_name}.{DEFAULT_SCOPE}.{DEFAULT_COLLECTION}'
            assert record.header.values[CAS_HEADER]
            assert record.header.values[TYPE_HEADER] == 'json'

            for record_key, document_key in zip(record.field.keys(), document.keys()):
                assert record_key == document_key
                assert record.field[record_key] == document[document_key]
    finally:
        logger.info('Deleting %s Couchbase bucket ...', bucket_name)
        couchbase.bucket_manager.drop_bucket(bucket_name)


@pytest.mark.parametrize('order_by_column', ['id', 'name'])
def test_order_by_clause(sdc_builder, sdc_executor, couchbase, order_by_column):
    """ Basic test for Couchbase Origin using ORDER BY clause, ordering by non-default metadata id.
    The pipeline looks like:

    couchbase_origin >> wiretap
    """
    def sort_documents(doc):
        return doc[order_by_column]

    bucket_name = get_random_string(string.ascii_letters, 10)
    document_ids = DEFAULT_DOCUMENT_IDS
    documents = DEFAULT_DOCUMENTS
    documents.sort(key=sort_documents)
    try:
        bucket = create_bucket(couchbase, bucket_name)
        insert_documents(bucket, document_ids, documents)

        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        couchbase_origin = builder.add_stage(name=STAGE_NAME)
        couchbase_origin.set_attributes(
            buckets=[get_bucket_config(bucket_name, order_by=f'DOCUMENT.{order_by_column}')])

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
        for record, document in zip(records, documents):
            for record_key, document_key in zip(record.field.keys(), document.keys()):
                assert record_key == document_key
                assert record.field[record_key] == document[document_key]
    finally:
        logger.info('Deleting %s Couchbase bucket ...', bucket_name)
        couchbase.bucket_manager.drop_bucket(bucket_name)


@pytest.mark.parametrize('where, expected_records', [
    (["id > 2"], [DEFAULT_DOCUMENTS[2]]),
    (["name = 'Roger Federer'"], [DEFAULT_DOCUMENTS[0]]),
    (["name != 'Joaquin'"], DEFAULT_DOCUMENTS),
    (["name != 'Joaquin'", "id > 2"], [DEFAULT_DOCUMENTS[2]])
])
def test_where_clause(sdc_builder, sdc_executor, couchbase, where, expected_records):
    """ Basic test for Couchbase Origin using WHERE clause.
    The pipeline looks like:

    couchbase_origin >> wiretap
    """
    bucket_name = get_random_string(string.ascii_letters, 10)
    document_ids = DEFAULT_DOCUMENT_IDS
    documents = DEFAULT_DOCUMENTS
    try:
        bucket = create_bucket(couchbase, bucket_name)
        insert_documents(bucket, document_ids, documents)

        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        couchbase_origin = builder.add_stage(name=STAGE_NAME)
        couchbase_origin.set_attributes(
            buckets=[get_bucket_config(bucket_name, where=where)])

        wiretap = builder.add_wiretap()
        finisher = builder.add_stage('Pipeline Finisher Executor')
        finisher.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

        couchbase_origin >> wiretap.destination
        couchbase_origin >= finisher

        pipeline = builder.build().configure_for_environment(couchbase)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        assert len(expected_records) == len(records), \
            f'{len(expected_records)} records should have been processed but {len(records)} were found'
        for record, document in zip(records, expected_records):
            for record_key, document_key in zip(record.field.keys(), document.keys()):
                assert record_key == document_key
                assert record.field[record_key] == document[document_key]
    finally:
        logger.info('Deleting %s Couchbase bucket ...', bucket_name)
        couchbase.bucket_manager.drop_bucket(bucket_name)


@pytest.mark.parametrize('num_threads', [1, 5, 10])
@pytest.mark.parametrize('num_buckets, num_records, batch_size', [(1, 10000, 1000), (4, 20000, 3333)])
def test_big_amounts_of_records(sdc_builder, sdc_executor, couchbase, num_threads, num_buckets, num_records,
                                batch_size):
    """ Test for Couchbase Origin with loads of documents, using multiple buckets and multiple threads.
    The pipeline looks like:

    couchbase_origin >> wiretap
    """
    # we need them sorted as multithreading doesn't care about output order
    sort_column = 'id'

    def sort_documents(doc):
        return doc[sort_column]

    def sort_records(rec):
        return rec.field[sort_column]

    bucket_names = [get_random_string(string.ascii_letters, 10) for _ in range(0, num_buckets)]
    try:
        documents = []
        for bucket_name in bucket_names:
            bucket = create_bucket(couchbase, bucket_name)
            bucket_document_ids = []
            bucket_documents = []
            for _ in range(0, num_records):
                bucket_document_ids.append(get_random_string())
                bucket_documents.append({'id': f'id_{get_random_string()}', 'name': f'name_{get_random_string()}'})

            insert_documents(bucket, bucket_document_ids, bucket_documents)
            documents.extend(bucket_documents)
        documents.sort(key=sort_documents)

        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        couchbase_origin = builder.add_stage(name=STAGE_NAME)
        couchbase_origin.set_attributes(buckets=[get_bucket_config(bucket_name) for bucket_name in bucket_names],
                                        number_of_threads=num_threads,
                                        max_batch_size_in_records=batch_size)

        wiretap = builder.add_wiretap()
        finisher = builder.add_stage('Pipeline Finisher Executor')
        finisher.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

        couchbase_origin >> wiretap.destination
        couchbase_origin >= finisher

        pipeline = builder.build().configure_for_environment(couchbase)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        records.sort(key=sort_records)
        assert len(documents) == len(records), \
            f'{len(documents)} records should have been processed but {len(records)} were found'
        for record, document in zip(records, documents):
            for record_key, document_key in zip(record.field.keys(), document.keys()):
                assert record_key == document_key
                assert record.field[record_key] == document[document_key]
    finally:
        logger.info('Deleting %s Couchbase bucket ...', bucket_names)
        [couchbase.bucket_manager.drop_bucket(bucket_name) for bucket_name in bucket_names]


def test_empty_document(sdc_builder, sdc_executor, couchbase):
    """ Basic test for Couchbase Origin to read empty documents. The pipeline looks like:

    couchbase_origin >> wiretap
    """
    bucket_name = get_random_string(string.ascii_letters, 10)
    document_ids = [get_random_string()]
    documents = [{}]
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
        assert records[0].field == {}
    finally:
        logger.info('Deleting %s Couchbase bucket ...', bucket_name)
        couchbase.bucket_manager.drop_bucket(bucket_name)


@pytest.mark.parametrize('num_threads', [1, 3])
@pytest.mark.parametrize('num_buckets', [1, 3])
def test_events(sdc_builder, sdc_executor, couchbase, num_threads, num_buckets):
    """ Test for Couchbase Origin events generation. 2 events can be generated, no-more-data and no-more-bucket-data.
    The pipeline looks like:

    couchbase_origin >> trash
    couchbase_origin >= wiretap.destination
    """
    bucket_names = [get_random_string(string.ascii_letters, 10) for _ in range(0, num_buckets)]
    document_ids = DEFAULT_DOCUMENT_IDS
    documents = DEFAULT_DOCUMENTS
    try:
        for bucket_name in bucket_names:
            bucket = create_bucket(couchbase, bucket_name)
            insert_documents(bucket, document_ids, documents)

        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        couchbase_origin = builder.add_stage(name=STAGE_NAME)
        couchbase_origin.set_attributes(buckets=[get_bucket_config(bucket_name) for bucket_name in bucket_names],
                                        number_of_threads=num_threads)
        trash = builder.add_stage('Trash')
        wiretap = builder.add_wiretap()

        couchbase_origin >> trash
        couchbase_origin >= wiretap.destination

        pipeline = builder.build().configure_for_environment(couchbase)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', num_buckets * len(documents))
        sdc_executor.stop_pipeline(pipeline)

        event_records = wiretap.output_records
        # we will get num_buckets no-more-bucket-data and 1 no-more-data
        assert num_buckets + 1 == len(event_records), \
            f'{num_buckets + 1} events should have been created but {len(event_records)} were found'

        # we cannot ensure the no-more-bucket-data events order, but we know the no-more-data is last
        bucket_names_in_events = []
        for i in range(0, num_buckets):
            assert event_records[i].header.values['sdc.event.type'] == 'no-more-bucket-data'
            assert event_records[i].field['documents'] == len(documents)
            assert event_records[i].field['scope'] == DEFAULT_SCOPE
            assert event_records[i].field['collection'] == DEFAULT_COLLECTION
            bucket_names_in_events.append(event_records[i].field['bucket'])
        # assert every bucket had an event, any order
        assert sorted(bucket_names_in_events) == sorted(bucket_names)
        # and that we also have the no-more-data
        assert event_records[num_buckets].header.values['sdc.event.type'] == 'no-more-data'
        assert event_records[num_buckets].field['record-count'] == num_buckets * len(documents)
    finally:
        logger.info('Deleting %s Couchbase bucket ...', bucket_names)
        [couchbase.bucket_manager.drop_bucket(bucket_name) for bucket_name in bucket_names]


def test_multiple_no_more_data(sdc_builder, sdc_executor, couchbase):
    """ Test for Couchbase Origin. We need to make sure the stage is waiting until more data is retrieved,
    and generate proper events.
    The pipeline looks like:

    couchbase_origin >> records_wiretap.destination
    couchbase_origin >= events_wiretap.destination
    """
    bucket_name = get_random_string(string.ascii_letters, 10)
    try:
        bucket = create_bucket(couchbase, bucket_name)
        document_ids = ['doc_1', 'doc_2', 'doc_3']
        documents_1 = [{'id': 1, 'name': 'Roger'}, {'id': 2, 'name': 'Rafael'}, {'id': 3, 'name': 'Carlos'}]
        insert_documents(bucket, document_ids, documents_1)

        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        couchbase_origin = builder.add_stage(name=STAGE_NAME)
        couchbase_origin.set_attributes(buckets=[get_bucket_config(bucket_name)])
        records_wiretap = builder.add_wiretap()
        events_wiretap = builder.add_wiretap()

        couchbase_origin >> records_wiretap.destination
        couchbase_origin >= events_wiretap.destination

        pipeline = builder.build().configure_for_environment(couchbase)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(documents_1))

        document_ids = ['doc_4', 'doc_5', 'doc_6']
        documents_2 = [{'id': 4, 'name': 'Joaquin'}, {'id': 5, 'name': 'Bo'}, {'id': 6, 'name': 'Diaz'}]
        insert_documents(bucket, document_ids, documents_2)

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(documents_1) + len(documents_2))
        sdc_executor.stop_pipeline(pipeline)

        event_records = events_wiretap.output_records
        assert 4 == len(event_records), f'4 events should have been created but {len(event_records)} were found'
        assert event_records[0].header.values['sdc.event.type'] == 'no-more-bucket-data'
        assert event_records[1].header.values['sdc.event.type'] == 'no-more-data'
        assert event_records[2].header.values['sdc.event.type'] == 'no-more-bucket-data'
        assert event_records[3].header.values['sdc.event.type'] == 'no-more-data'

        expected_documents = documents_1 + documents_2
        records = records_wiretap.output_records
        assert len(expected_documents) == len(records), \
            f'{len(expected_documents)} records should have been processed but {len(records)} were found'
        for record, document in zip(records, expected_documents):
            for record_key, document_key in zip(record.field.keys(), document.keys()):
                assert record_key == document_key
                assert record.field[record_key] == document[document_key]
    finally:
        logger.info('Deleting %s Couchbase bucket ...', bucket_name)
        couchbase.bucket_manager.drop_bucket(bucket_name)
