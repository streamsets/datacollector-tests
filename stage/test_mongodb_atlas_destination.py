# Copyright 2022 StreamSets Inc.
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
import json
from bson import json_util
from string import ascii_letters

import pytest
from streamsets.testframework.markers import mongodb, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


MONGODB_ATLAS_DESTINATION = 'com_streamsets_pipeline_stage_destination_mongodb_atlas_MongoDBAtlasDTarget'
pytestmark = [mongodb, sdc_min_version('5.2.0')]


@pytest.mark.parametrize('operation,sdc_operation,input_data,expected_records', [
    ('INSERT', '1', {"id": 0, "f1": {"f2": "a"}, "f3": "b"}, [{"id": 0, "f1": {"f2": "a"}, "f3": "b"}]),
    ('DELETE', '2', {"id": 1, "f3": "b"}, [{"id": 0, "f1": {"f2": "a"}}]),
    ('UPDATE', '3', {"id": 2, "f3": "c"}, [{"id": 0, "f1": {"f2": "a"}}, {"id": 1, "f3": "b"}]),
    ('UPSERT', '4', {"id": 2, "f3": "c"}, [{"id": 0, "f1": {"f2": "a"}}, {"id": 1, "f3": "b"}, {"id": 2, "f3": "c"}]),
    ('REPLACE', '7', {"id": 0, "f1": {"f2": "b"}, "f3": "c"}, [{"id": 0, "f1": {"f2": "b"}, "f3": "c"}]),
])
def test_mongodb_atlas_destination(sdc_builder, sdc_executor, mongodb, operation, sdc_operation, input_data,
                                   expected_records):
    """
    Send simple text into MongoDB Atlas destination from Dev Raw Data Source and confirm that MongoDB Atlas correctly
        received them using PyMongo.

    The pipeline looks like:
        dev_raw_data_source >> record_deduplicator >> expression_evaluator >> mongodb_atlas_destination
                               record_deduplicator >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    if operation == 'UPSERT':
        upsert = True
    else:
        upsert = False

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=json.dumps(input_data), stop_after_first_batch=True)

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': sdc_operation}]

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=get_random_string(ascii_letters, 5),
                                             collection=get_random_string(ascii_letters, 10),
                                             unique_keys=[{'collectionPath': 'f3', 'recordPath': ''}],
                                             upsert=upsert)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_destination.tls_mode = 'NONE'
        mongodb_atlas_destination.authentication_method = 'NONE'

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')

    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> record_deduplicator >> expression_evaluator >> mongodb_atlas_destination
    record_deduplicator >> trash

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        if operation != "INSERT":
            if operation in ('DELETE', 'UPDATE', 'UPSERT'):
                input_data_1 = {"id": 0, "f1": {"f2": "a"}}
                input_data_2 = {"id": 1, "f3": "b"}
            else:
                input_data_1 = {"id": 0, "f3": "c"}

            # Create document in MongoDB Atlas using PyMongo.
            # First a database is created. Then a collection is created inside that database.
            # Then document is created in that collection.
            logger.info('Adding document into %s collection using PyMongo...', mongodb_atlas_destination.collection)
            mongodb_database = mongodb.engine[mongodb_atlas_destination.database]
            mongodb_collection = mongodb_database[mongodb_atlas_destination.collection]
            inserted_doc = mongodb_collection.insert_one(input_data_1)
            assert inserted_doc is not None
            if operation not in ('REPLACE'):
                inserted_doc = mongodb_collection.insert_one(input_data_2)
                assert inserted_doc is not None


        # Data is generated in dev_raw_data_source and sent to MongoDB Atlas using pipeline.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify data is received correctly using PyMongo.
        # While reading data, we specify MongoDB Atlas database and the collection inside it.
        logger.info('Verifying docs received with PyMongo...')
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][
            mongodb_atlas_destination.collection].find()]
        assert len(mongodb_documents) == len(expected_records)
        mongodb_documents_without_id = [_delete_id(doc) for doc in mongodb_documents]
        assert sorted(mongodb_documents_without_id, key=lambda x: x["id"]) == expected_records

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(mongodb_atlas_destination.database)


@pytest.mark.parametrize('with_record_path', [True, False])
@pytest.mark.parametrize('upsert', [True, False])
def test_mongodb_atlas_destination_update_record_path(sdc_builder, sdc_executor, mongodb, with_record_path, upsert):
    """
    Ensure that an update on a document with and without record path works.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    insert_record = {"f1": {"f2": "a"}, "f3": "c"}
    if with_record_path:
        pipeline_title = 'Using Record Path - '
        record = {"f1": {"g2": "a"}, "f3": "b"}
        unique_keys = [{'collectionPath': '/f1/f2', 'recordPath': 'f1.g2'}]
        expected_record = {'f1': {'f2': 'a', 'g2': 'a'}, 'f3': 'b'}
    else:
        pipeline_title = 'Only Collection Path - '
        record = {"f1": {"f2": "a"}, "f3": "b"}
        unique_keys = [{'collectionPath': '/f1/f2', 'recordPath': ''}]
        expected_record = {"f1": {"f2": "a"}, "f3": "b"}

    if upsert:
        pipeline_title = pipeline_title + 'Upsert'
        operation = '4'
    else:
        pipeline_title = pipeline_title + 'Update'
        operation = '3'

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=json.dumps(record))

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': operation}]

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=get_random_string(ascii_letters, 5),
                                             collection=get_random_string(ascii_letters, 10),
                                             unique_keys=unique_keys)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_destination.tls_mode = 'NONE'
        mongodb_atlas_destination.authentication_method = 'NONE'

    dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination

    pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(mongodb)

    try:
        if not upsert:
            # Create document in MongoDB Atlas using PyMongo.
            # First a database is created. Then a collection is created inside that database.
            # Then document is created in that collection.
            logger.info('Adding document into %s collection using PyMongo...', mongodb_atlas_destination.collection)
            mongodb_database = mongodb.engine[mongodb_atlas_destination.database]
            mongodb_collection = mongodb_database[mongodb_atlas_destination.collection]
            inserted_doc = mongodb_collection.insert_one(insert_record)
            assert inserted_doc is not None

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(1)
        sdc_executor.stop_pipeline(pipeline)

        logger.info('Verifying docs updated with PyMongo...')
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][
            mongodb_atlas_destination.collection].find()]
        assert len(mongodb_documents) == 1
        assert _delete_id(mongodb_documents[0]) == expected_record

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(mongodb_atlas_destination.database)


@pytest.mark.parametrize('object', [
    'COLLECTION',
    'DATABASE'
])
def test_mongodb_atlas_destination_collection_database_expression(sdc_builder, sdc_executor, mongodb, object):
    """
    Insert 1 simple document in MongoDB Atlas with database and collection as expression and confirm it works in
        MongoDB Atlas.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    prefix = get_random_string(ascii_letters, 4)
    if object == 'COLLECTION':
        database = get_random_string(ascii_letters, 5)
        collection = prefix + '${record:value(\'/name\')}'
        pipeline_title = 'Collection using Expression'
    else:
        collection = get_random_string(ascii_letters, 5)
        database = prefix + '${record:value(\'/name\')}'
        pipeline_title = 'Database using Expression'

    record = {'data': 'a', 'name': 'test'}

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=json.dumps(record), stop_after_first_batch=True)

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': '1'}]

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=database,
                                             collection=collection)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_destination.tls_mode = 'NONE'
        mongodb_atlas_destination.authentication_method = 'NONE'

    dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination

    pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(mongodb)

    try:
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        logger.info('Get the name of the collection or database from MongoDB...')
        if object == 'COLLECTION':
            for coll in mongodb.engine[mongodb_atlas_destination.database].list_collections():
                collection = coll.get('name')
                if collection.startswith(prefix):
                    assert 'test' in collection
                    break
        else:
            for database in mongodb.engine.list_database_names():
                if database.startswith(prefix):
                    assert 'test' in database
                    break

        logger.info('Verifying docs updated with PyMongo...')
        mongodb_documents = [doc for doc in mongodb.engine[database][collection].find()]
        assert _delete_id(mongodb_documents[0]) == record

    finally:
        logger.info('Dropping %s database...', database)
        mongodb.engine.drop_database(database)


@pytest.mark.parametrize('prefix', ['.', '$', '/'])
def test_mongodb_atlas_destination_reserved_field_names(sdc_builder, sdc_executor, mongodb, prefix):
    """
    Insert 1 simple document in MongoDB Atlas with reserved field names like periods (.), dollar digns ($) and
    slash (/) and confirm it works in MongoDB Atlas.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    if not mongodb.atlas:
        pytest.skip("MongoDB database didn't accept write field names with prefixes periods "
                    "and dollar signs with reserved words")

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    database = get_random_string(ascii_letters, 5)
    collection = get_random_string(ascii_letters, 10)

    record = {f"{prefix}db": "a", f"{prefix}inc": "b", f"{prefix}ref": "c"}
    pipeline_title = f'Reserved field {prefix}'

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=json.dumps(record), stop_after_first_batch=True)

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': '1'}]

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=database,
                                             collection=collection)

    dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination

    pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(mongodb)

    try:
        # Start pipeline and verify the documents using PyMongo.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        logger.info('Verifying docs updated with PyMongo...')
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][
            mongodb_atlas_destination.collection].find()]
        assert _delete_id(mongodb_documents[0]) == record

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(mongodb_atlas_destination.database)


@sdc_min_version('5.5.0')
@pytest.mark.parametrize('nested', [True, False])
@pytest.mark.parametrize('element_exist', [True, False])
def test_mongodb_atlas_destination_update_map(sdc_builder, sdc_executor, mongodb, nested, element_exist):
    """
    Ensure that an update a nested document with map structure is correctly executed. 
    The expected performance:
        - If the map exist and have all the nested unique keys, it will replace the value 
        of the specific record for the new one.
        - If the map exist but only had document unique key, it will replace the document 
        for the record.
        - If the map or the document didn't exist, it will do nothing.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    database_name = get_random_string(ascii_letters, 5)
    collection_name = get_random_string(ascii_letters, 10)

    pipeline_title = 'Update - Map Structure - '
    operation = '3'
    upsert = False
    
    if nested:
        pipeline_title = pipeline_title + 'Nested UK - '
        record = {"id": 0, "f1": {"g1": 1, "g2": {"h1": 2, "h2": "Updated"}}}
        unique_keys = [{'collectionPath': 'id', 'recordPath': ''},
                {'collectionPath': 'f1.g1', 'recordPath': ''},
                {'collectionPath': 'f1.g2.h1', 'recordPath': ''}]
        if element_exist:
            pipeline_title = pipeline_title + 'Element exist'
            origin_record = {"id": 0, "f1": {"g1": 1, "g2": {"h1": 2, "h2": "toBeUpdated"}, "f2": "NoUpdate"}}
            expected_records = [{"id": 0, "f1": {"g1": 1, "g2": {"h1": 2, "h2": "Updated"}, "f2": "NoUpdate"}}]
        else:
            pipeline_title = pipeline_title + 'Element no exist'
            origin_record = {"id": 0, "f1": {"g1": 2, "g2": {"h1": 2, "h2": "toBeUpdated"}, "f2": "NoUpdate"}}
            expected_records = [{"id": 0, "f1": {"g1": 2, "g2": {"h1": 2, "h2": "toBeUpdated"}, "f2": "NoUpdate"}}]
    else:
        pipeline_title = pipeline_title + 'Non-Nested UK - '
        record = {"id": 0, "f1": {"g1": 1, "g2": "Updated"}}
        unique_keys = [{'collectionPath': 'id', 'recordPath': ''}]
        if element_exist:
            pipeline_title = pipeline_title + 'Element exist'
            origin_record = {"id": 0, "f1": {"g1": 1, "g2": "toBeUpdated"}, "f2": "NoUpdate"}
            expected_records = [{"id": 0, "f1": {"g1": 1, "g2": "Updated"}, "f2": "NoUpdate"}]
        else:
            pipeline_title = pipeline_title + 'Element no exist'
            origin_record = {"id": 1, "f1": {"g1": 2, "g2": "toBeUpdated"}, "f2": "NoUpdate"}
            expected_records = [{"id": 1, "f1": {"g1": 2, "g2": "toBeUpdated"}, "f2": "NoUpdate"}]

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=json_util.dumps(record),
                                       stop_after_first_batch=True
                                       )

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': operation}]

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=database_name,
                                             collection=collection_name,
                                             unique_keys=unique_keys,
                                             upsert=upsert)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_destination.tls_mode = 'NONE'
        mongodb_atlas_destination.authentication_method = 'NONE'

    dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination

    pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(mongodb)

    try:
        # Create document in MongoDB Atlas using PyMongo.
        # First a database is created. Then a collection is created inside that database.
        # Then document is created in that collection.
        mongodb_database = mongodb.engine[database_name]
        mongodb_collection = mongodb_database[collection_name]
        inserted_doc = mongodb_collection.insert_one(origin_record)
        assert inserted_doc is not None

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        logger.info('Verifying docs updated with PyMongo...')
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][
            mongodb_atlas_destination.collection].find()]
        assert len(mongodb_documents) == len(expected_records)
        mongodb_documents_without_id = [_delete_id(doc) for doc in mongodb_documents]
        assert sorted(mongodb_documents_without_id, key=lambda x: x["id"]) == expected_records

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(database_name)


@sdc_min_version('5.5.0')
@pytest.mark.parametrize('nested', [True, False])
@pytest.mark.parametrize('element_exist', [True, False])
def test_mongodb_atlas_destination_upsert_map(sdc_builder, sdc_executor, mongodb, nested, element_exist):
    """
    Ensure that an upsert a nested document with map structure is correctly executed. 
    The expected performance:
        - If the map exist and have all the nested unique keys, it will replace the value 
        of the specific record for the new one.
        - If the map exist but only had document unique key, it will replace the document 
        for the record.
        - If the map or the document didn't exist, it will insert the record.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    database_name = get_random_string(ascii_letters, 5)
    collection_name = get_random_string(ascii_letters, 10)

    pipeline_title = 'Upsert - Map Structure - '
    operation = '4'
    upsert = True
    if nested:
        # When the id exist but not the element inside the map, the id of the origin record and 
        # the id of the inserted record will be the same. We add an extra field called "sort_id" 
        # to sort the documents in the assert.
        pipeline_title = pipeline_title + 'Nested UK - '
        sort_id = "sort_id"
        record = {"sort_id": 0, "id": 0, "f1": {"g1": 1, "g2": {"h1": 2, "h2": "Updated"}}}
        unique_keys = [{'collectionPath': 'id', 'recordPath': ''},
                       {'collectionPath': 'f1.g1', 'recordPath': ''},
                       {'collectionPath': 'f1.g2.h1', 'recordPath': ''}]
        if element_exist:
            pipeline_title = pipeline_title + 'Element exist'
            origin_record = {"sort_id": 0, "id": 0, "f1": {"g1": 1, "g2": {"h1": 2, "h2": "toBeUpdated"}, "f2": "NoUpdate"}}
            expected_records = [{"sort_id": 0, "id": 0, "f1": {"g1": 1, "g2": {"h1": 2, "h2": "Updated"}, "f2": "NoUpdate"}}]
        else:
            pipeline_title = pipeline_title + 'Element no exist'
            origin_record = {"sort_id": 1, "id": 0, "f1": {"g1": 2, "g2": {"h1": 2, "h2": "toBeUpdated"}, "f2": "NoUpdate"}}
            expected_records = [{"sort_id": 0, "id": 0, "f1": {"g1": 1, "g2": {"h1": 2, "h2": "Updated"}}}, {"sort_id": 1, "id": 0, "f1": {"g1": 2, "g2": {"h1": 2, "h2": "toBeUpdated"}, "f2": "NoUpdate"}}]
    else:
        pipeline_title = pipeline_title + 'Non-Nested UK - '
        sort_id = "id"
        record = {"id": 0, "f1": {"g1": 1, "g2": {"h1": 2, "h2": "Updated"}}}
        unique_keys = [{'collectionPath': 'id', 'recordPath': ''}]
        if element_exist:
            pipeline_title = pipeline_title + 'Element exist'
            origin_record = {"id": 0, "f1": {"g1": 1, "g2": {"h1": 2, "h2": "toBeUpdated"}, "f2": "NoUpdate"}}
            expected_records = [{"id": 0, "f1": {"g1": 1, "g2": {"h1": 2, "h2": "Updated"}, "f2": "NoUpdate"}}]
        else:
            pipeline_title = pipeline_title + 'Element no exist'
            origin_record = {"id": 1, "f1": {"g1": 2, "g2": {"h1": 2, "h2": "toBeUpdated"}, "f2": "NoUpdate"}}
            expected_records = [{"id": 0, "f1": {"g1": 1, "g2": {"h1": 2, "h2": "Updated"}}}, {"id": 1, "f1": {"g1": 2, "g2": {"h1": 2, "h2": "toBeUpdated"}, "f2": "NoUpdate"}}]

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=json_util.dumps(record),
                                       stop_after_first_batch=True
                                       )

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': operation}]

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=database_name,
                                             collection=collection_name,
                                             unique_keys=unique_keys,
                                             upsert=upsert)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_destination.tls_mode = 'NONE'
        mongodb_atlas_destination.authentication_method = 'NONE'

    dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination

    pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(mongodb)

    try:
        mongodb_database = mongodb.engine[database_name]
        mongodb_collection = mongodb_database[collection_name]
        inserted_doc = mongodb_collection.insert_one(origin_record)
        assert inserted_doc is not None

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        logger.info('Verifying docs updated with PyMongo...')
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][
            mongodb_atlas_destination.collection].find()]
        assert len(mongodb_documents) == len(expected_records)
        mongodb_documents_without_id = [_delete_id(doc) for doc in mongodb_documents]
        assert sorted(mongodb_documents_without_id, key=lambda x: x[sort_id]) == expected_records

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(database_name)


@sdc_min_version('5.5.0')
@pytest.mark.parametrize('element_exist', [True, False])
def test_mongodb_atlas_destination_update_simple_list(sdc_builder, sdc_executor, mongodb, element_exist):
    """
    Ensure that an update a nested document with list structure is correctly executed. 
    The expected performance:
        - If the list exist and have the unique key, it will replace the value of the 
        specific record for the new one.
        - If the list or the document didn't exist, it will do nothing.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    database_name = get_random_string(ascii_letters, 5)
    collection_name = get_random_string(ascii_letters, 10)

    pipeline_title = 'Update - List Structure - '
    operation = '3'
    upsert = False
    
    record = {"id": 0, "f1": [1, 2, 3]}
    unique_keys = [{'collectionPath': 'id', 'recordPath': ''}]
    if element_exist:
        pipeline_title = pipeline_title + 'Element exist'
        origin_record = {"id": 0, "f1": [4, 5, 6], "f2": "NoUpdate"}
        expected_records = [{"id": 0, "f1": [1, 2, 3], "f2": "NoUpdate"}]
    else:
        pipeline_title = pipeline_title + 'Element no exist'
        origin_record = {"id": 1, "f1": [4, 5, 6], "f2": "NoUpdate"}
        expected_records = [{"id": 1, "f1": [4, 5, 6], "f2": "NoUpdate"}]

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=json_util.dumps(record),
                                       stop_after_first_batch=True
                                       )

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': operation}]

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=database_name,
                                             collection=collection_name,
                                             unique_keys=unique_keys,
                                             upsert=upsert)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_destination.tls_mode = 'NONE'
        mongodb_atlas_destination.authentication_method = 'NONE'

    dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination

    pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(mongodb)

    try:
        # Create document in MongoDB Atlas using PyMongo.
        # First a database is created. Then a collection is created inside that database.
        # Then document is created in that collection.
        mongodb_database = mongodb.engine[database_name]
        mongodb_collection = mongodb_database[collection_name]
        inserted_doc = mongodb_collection.insert_one(origin_record)
        assert inserted_doc is not None

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        logger.info('Verifying docs updated with PyMongo...')
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][
            mongodb_atlas_destination.collection].find()]
        assert len(mongodb_documents) == len(expected_records)
        mongodb_documents_without_id = [_delete_id(doc) for doc in mongodb_documents]
        assert sorted(mongodb_documents_without_id, key=lambda x: x["id"]) == expected_records

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(database_name)


@sdc_min_version('5.5.0')
@pytest.mark.parametrize('element_exist', [True, False])
def test_mongodb_atlas_destination_upsert_simple_list(sdc_builder, sdc_executor, mongodb, element_exist):
    """
    Ensure that an upsert a nested document with list structure is correctly executed. 
    The expected performance:
        - If the list exist and have the unique key, it will replace the value of the 
        specific record for the new one.
        - If the list or the document didn't exist, it will insert the record.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    database_name = get_random_string(ascii_letters, 5)
    collection_name = get_random_string(ascii_letters, 10)

    pipeline_title = 'Upsert - List Structure - '
    operation = '4'
    upsert = True
    
    record = {"id": 0, "f1": [1, 2, 3]}
    unique_keys = [{'collectionPath': 'id', 'recordPath': ''}]
    if element_exist:
        pipeline_title = pipeline_title + 'Element exist'
        origin_record = {"id": 0, "f1": [4, 5, 6], "f2": "NoUpdate"}
        expected_records = [{"id": 0, "f1": [1, 2, 3], "f2": "NoUpdate"}]
    else:
        pipeline_title = pipeline_title + 'Element no exist'
        origin_record = {"id": 1, "f1": [4, 5, 6], "f2": "NoUpdate"}
        expected_records = [{"id": 0, "f1": [1, 2, 3]}, {"id": 1, "f1": [4, 5, 6], "f2": "NoUpdate"}]

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=json_util.dumps(record),
                                       stop_after_first_batch=True
                                       )

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': operation}]

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=database_name,
                                             collection=collection_name,
                                             unique_keys=unique_keys,
                                             upsert=upsert)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_destination.tls_mode = 'NONE'
        mongodb_atlas_destination.authentication_method = 'NONE'

    dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination

    pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(mongodb)

    try:
        # Create document in MongoDB Atlas using PyMongo.
        # First a database is created. Then a collection is created inside that database.
        # Then document is created in that collection.
        mongodb_database = mongodb.engine[database_name]
        mongodb_collection = mongodb_database[collection_name]
        inserted_doc = mongodb_collection.insert_one(origin_record)
        assert inserted_doc is not None

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        logger.info('Verifying docs updated with PyMongo...')
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][
            mongodb_atlas_destination.collection].find()]
        assert len(mongodb_documents) == len(expected_records)
        mongodb_documents_without_id = [_delete_id(doc) for doc in mongodb_documents]
        assert sorted(mongodb_documents_without_id, key=lambda x: x["id"]) == expected_records

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(database_name)


@sdc_min_version('5.5.0')
@pytest.mark.parametrize('nested', [True, False])
@pytest.mark.parametrize('element_exist', [True, False])
def test_mongodb_atlas_destination_update_nested_list(sdc_builder, sdc_executor, mongodb, nested, element_exist):
    """
    Ensure that an update a nested document with nested list structure is correctly executed.
    The expected performance:
        - If the nested list exist and have all the nested unique keys, it will replace the value
        of the specific record for the new one.
        - If the nested list exist but only had document unique key, it will replace the document
        for the record.
        - If the nested list or the document didn't exist, it will do nothing.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    database_name = get_random_string(ascii_letters, 5)
    collection_name = get_random_string(ascii_letters, 10)

    pipeline_title = 'Update - Nested List Structure - '
    operation = '3'
    upsert = False

    if nested:
        pipeline_title = pipeline_title + 'Nested UK - '
        record = {"id": 0, "f1": [{"g1": 1, "g2": {"h1": 2, "h2": "Updated"}}]}
        unique_keys = [{'collectionPath': 'id', 'recordPath': ''},
                       {'collectionPath': 'f1.$[el].g1', 'recordPath': ''}]
        if element_exist:
            pipeline_title = pipeline_title + 'Element exist'
            origin_record = {"id": 0, "f1": [{"g1": 0, "g2": {"h1": 1, "h2": "NoUpdate"}}, {"g1": 1, "g2": {"h1": 2, "h2": "ToBeUpdated"}}]}
            expected_records = [{"id": 0, "f1": [{"g1": 0, "g2": {"h1": 1, "h2": "NoUpdate"}}, {"g1": 1, "g2": {"h1": 2, "h2": "Updated"}}]}]
        else:
            pipeline_title = pipeline_title + 'Element no exist'
            origin_record = {"id": 0, "f1": [{"g1": 0, "g2": {"h1": 1, "h2": "NoUpdate"}}]}
            expected_records = [{"id": 0, "f1": [{"g1": 0, "g2": {"h1": 1, "h2": "NoUpdate"}}]}]
    else:
        pipeline_title = pipeline_title + 'Non-Nested UK - '
        record = {"id": 0, "f1": [{"g1": 1, "g2": {"h1": 2, "h2": "Updated"}}]}
        unique_keys = [{'collectionPath': 'id', 'recordPath': ''}]
        if element_exist:
            pipeline_title = pipeline_title + 'Element exist'
            origin_record = {"id": 0, "f1": [{"g1": 1, "g2": {"h1": 1, "h2": "NoUpdate"}}]}
            expected_records = [{"id": 0, "f1": [{"g1": 1, "g2": {"h1": 2, "h2": "Updated"}}]}]
        else:
            pipeline_title = pipeline_title + 'Element no exist'
            origin_record = {"id": 1, "f1": [{"g1": 0, "g2": {"h1": 1, "h2": "NoUpdate"}}]}
            expected_records = [{"id": 1, "f1": [{"g1": 0, "g2": {"h1": 1, "h2": "NoUpdate"}}]}]

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=json_util.dumps(record),
                                       stop_after_first_batch=True
                                       )

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': operation}]

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=database_name,
                                             collection=collection_name,
                                             unique_keys=unique_keys,
                                             upsert=upsert)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_destination.tls_mode = 'NONE'
        mongodb_atlas_destination.authentication_method = 'NONE'

    dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination

    pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(mongodb)

    try:
        # Create document in MongoDB Atlas using PyMongo.
        # First a database is created. Then a collection is created inside that database.
        # Then document is created in that collection.
        mongodb_database = mongodb.engine[database_name]
        mongodb_collection = mongodb_database[collection_name]
        inserted_doc = mongodb_collection.insert_one(origin_record)
        assert inserted_doc is not None

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        logger.info('Verifying docs updated with PyMongo...')
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][
            mongodb_atlas_destination.collection].find()]
        assert len(mongodb_documents) == len(expected_records)
        mongodb_documents_without_id = [_delete_id(doc) for doc in mongodb_documents]
        assert sorted(mongodb_documents_without_id, key=lambda x: x["id"]) == expected_records

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(database_name)


@sdc_min_version('5.5.0')
@pytest.mark.parametrize('nested', [True, False])
@pytest.mark.parametrize('element_exist', [True, False])
def test_mongodb_atlas_destination_upsert_nested_list(sdc_builder, sdc_executor, mongodb, nested, element_exist):
    """
    Ensure that an upsert a nested document with nested list structure is correctly executed. 
    The expected performance:
        - If the nested list exist and have all the nested unique keys, it will replace the value 
        of the specific record for the new one.
        - If the nested list exist but only had document unique key, it will replace the document 
        for the record.
        - If the nested list or the document didn't exist, it will insert the record.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    database_name = get_random_string(ascii_letters, 5)
    collection_name = get_random_string(ascii_letters, 10)

    pipeline_title = 'Upsert - Map Structure - '
    operation = '4'
    upsert = True
    if nested:
        # When the id exist but not the element inside the map, the id of the origin record and 
        # the id of the inserted record will be the same. We add an extra field called "sort_id" 
        # to sort the documents in the assert.
        pipeline_title = pipeline_title + 'Nested UK - '
        sort_id = "sort_id"
        record = {"sort_id": 0, "id": 0, "f1": [{"g1": 1, "g2": {"h1": 2, "h2": "Updated"}}]}
        unique_keys = [{'collectionPath': 'id', 'recordPath': ''},
                       {'collectionPath': 'f1.$[el].g1', 'recordPath': ''}]
        if element_exist:
            pipeline_title = pipeline_title + 'Element exist'
            origin_record = {"sort_id": 0, "id": 0, "f1": [{"g1": 0, "g2": {"h1": 1, "h2": "NoUpdate"}}, {"g1": 1, "g2": {"h1": 2, "h2": "ToBeUpdated"}}]}
            expected_records = [{"sort_id": 0, "id": 0, "f1": [{"g1": 0, "g2": {"h1": 1, "h2": "NoUpdate"}}, {"g1": 1, "g2": {"h1": 2, "h2": "Updated"}}]}]
        else:
            pipeline_title = pipeline_title + 'Element no exist'
            origin_record = {"sort_id": 0, "id": 0, "f1": [{"g1": 0, "g2": {"h1": 1, "h2": "NoUpdate"}}]}
            expected_records = [{"sort_id": 0, "id": 0, "f1": [{"g1": 0, "g2": {"h1": 1, "h2": "NoUpdate"}}, {"g1": 1, "g2": {"h1": 2, "h2": "Updated"}}]}]
    else:
        pipeline_title = pipeline_title + 'Non-Nested UK - '
        sort_id = "id"
        record = {"id": 0, "f1": [{"g1": 1, "g2": {"h1": 2, "h2": "Updated"}}]}
        unique_keys = [{'collectionPath': 'id', 'recordPath': ''}]
        if element_exist:
            pipeline_title = pipeline_title + 'Element exist'
            origin_record = {"id": 0, "f1": [{"g1": 1, "g2": {"h1": 1, "h2": "NoUpdate"}}]}
            expected_records = [{"id": 0, "f1": [{"g1": 1, "g2": {"h1": 2, "h2": "Updated"}}]}]
        else:
            pipeline_title = pipeline_title + 'Element no exist'
            origin_record = {"id": 1, "f1": [{"g1": 0, "g2": {"h1": 1, "h2": "NoUpdate"}}]}
            expected_records = [{"id": 0, "f1": [{"g1": 1, "g2": {"h1": 2, "h2": "Updated"}}]}, {"id": 1, "f1": [{"g1": 0, "g2": {"h1": 1, "h2": "NoUpdate"}}]}]

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=json_util.dumps(record),
                                       stop_after_first_batch=True
                                       )

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': operation}]

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=database_name,
                                             collection=collection_name,
                                             unique_keys=unique_keys,
                                             upsert=upsert)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_destination.tls_mode = 'NONE'
        mongodb_atlas_destination.authentication_method = 'NONE'

    dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination

    pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(mongodb)

    try:
        mongodb_database = mongodb.engine[database_name]
        mongodb_collection = mongodb_database[collection_name]
        inserted_doc = mongodb_collection.insert_one(origin_record)
        assert inserted_doc is not None

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        logger.info('Verifying docs updated with PyMongo...')
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][
            mongodb_atlas_destination.collection].find()]
        assert len(mongodb_documents) == len(expected_records)
        mongodb_documents_without_id = [_delete_id(doc) for doc in mongodb_documents]
        assert sorted(mongodb_documents_without_id, key=lambda x: x[sort_id]) == expected_records

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(database_name)


@sdc_min_version('6.1.0')
@pytest.mark.parametrize('element_exist', [True, False])
def test_mongodb_atlas_destination_upsert_nested_multi_list(sdc_builder, sdc_executor, mongodb, element_exist):
    """
    Ensure that an upsert a nested document with nested list structure is correctly executed.
    Here one of the unique keys is residing at n level nested array.
    The expected performance:
        - If the nested list exist and have all the nested unique keys, it will replace the value
        of the specific record for the new one.
        - If the nested list exist but only had document unique key, it will replace the document
        for the record.
        - If the nested list or the document didn't exist, it will insert the record.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    database_name = get_random_string(ascii_letters, 5)
    collection_name = get_random_string(ascii_letters, 10)

    pipeline_title = 'Upsert - N level Nested UK -  '
    operation = '4'
    upsert = True
    record = {"sort_id": 0,"id": 0,"f1": [{"g1": [{"h1": [{"i1": 2,"i2": "Updated"}]}]}]}
    sort_id = "sort_id"
    unique_keys = [{'collectionPath': 'id', 'recordPath': ''},
                   {'collectionPath': 'f1.$[].g1.$[].h1.$[el].i1', 'recordPath': ''}]

    if element_exist:
        pipeline_title = pipeline_title + 'Element exist'
        origin_record = {"sort_id": 0,"id": 0,"f1": [{ "g1": [{"h1": [{"i1": 1,"i2": "NoUpdate"}]}]}, {"g1": [{"h1": [{"i1": 2,"i2": "ToBeUpdated"}]}]}]}
        expected_records = [{"sort_id": 0,"id": 0,"f1": [{"g1": [{"h1": [{"i1":1,"i2": "NoUpdate"}]}]}, {"g1": [{"h1": [{"i1": 2,"i2": "Updated"}]}]}]}]
    else:
        pipeline_title = pipeline_title + 'Element no exist'
        origin_record = {"sort_id": 0, "id": 0, "f1": [{"g1": [{"h1": [{"i1": 1,"i2": "NoUpdate"}]}]}]}
        expected_records = [{"sort_id": 0, "id": 0, "f1": [{"g1": [{"h1": [{"i1": 1,"i2": "NoUpdate"}]}]}, {"g1": [{"h1": [{"i1": 2,"i2": "Updated"}]}]}]}]

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=json_util.dumps(record),
                                       stop_after_first_batch=True
                                       )

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': operation}]

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=database_name,
                                             collection=collection_name,
                                             unique_keys=unique_keys,
                                             upsert=upsert)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_destination.tls_mode = 'NONE'
        mongodb_atlas_destination.authentication_method = 'NONE'

    dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination

    pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(mongodb)

    try:
        mongodb_database = mongodb.engine[database_name]
        mongodb_collection = mongodb_database[collection_name]
        inserted_doc = mongodb_collection.insert_one(origin_record)
        assert inserted_doc is not None

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        logger.info('Verifying docs updated with PyMongo...')
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][
            mongodb_atlas_destination.collection].find()]
        assert len(mongodb_documents) == len(expected_records)
        mongodb_documents_without_id = [_delete_id(doc) for doc in mongodb_documents]
        assert sorted(mongodb_documents_without_id, key=lambda x: x[sort_id]) == expected_records

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(database_name)


@sdc_min_version('6.1.0')
@pytest.mark.parametrize('element_exist', [True, False])
def test_mongodb_atlas_destination_update_nested_multi_list(sdc_builder, sdc_executor, mongodb, element_exist):
    """
    Ensure that an update a nested document with nested list structure is correctly executed.
    Here one of the unique keys is residing at n level nested array.
    The expected performance:
        - If the nested list exist and have all the nested unique keys, it will replace the value
        of the specific record for the new one.
        - If the nested list exist but only had document unique key, it will replace the document
        for the record.
        - If the nested list or the document didn't exist, it will do nothing.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    database_name = get_random_string(ascii_letters, 5)
    collection_name = get_random_string(ascii_letters, 10)

    pipeline_title = 'Update - Nested Multi List Structure - '
    operation = '3'
    upsert = False


    pipeline_title = pipeline_title + 'Nested UK - '
    record = {"id": 0, "f1": [{"g1": [{"h1": 2, "h2": {"i1": "Updated"}}]}]}
    unique_keys = [{'collectionPath': 'id', 'recordPath': ''},
                     {'collectionPath': 'f1.$[].g1.$[el].h1', 'recordPath': ''}]
    if element_exist:
        pipeline_title = pipeline_title + 'Element exist'
        origin_record = {"id": 0, "f1": [{"g1": [{"h1": 1, "h2": {"i1": "NoUpdate"}}]},
                                             {"g1": [{"h1": 2, "h2": {"i1": "TobeUpdated"}}]}]}
        expected_records = [{"id": 0, "f1": [{"g1": [{"h1": 1, "h2": {"i1": "NoUpdate"}}]},
                                                 {"g1": [{"h1": 2, "h2": {"i1": "Updated"}}]}]}]
    else:
        pipeline_title = pipeline_title + 'Element no exist'
        origin_record = {"id": 0, "f1": [{"g1": 0, "g2": [{"h1": 1,  "h2": {"i1": "NoUpdate"}}]}]}
        expected_records = [{"id": 0, "f1": [{"g1": 0, "g2": [{"h1": 1,  "h2": {"i1": "NoUpdate"}}]}]}]

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=json_util.dumps(record),
                                       stop_after_first_batch=True
                                       )

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': operation}]

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=database_name,
                                             collection=collection_name,
                                             unique_keys=unique_keys,
                                             upsert=upsert)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_destination.tls_mode = 'NONE'
        mongodb_atlas_destination.authentication_method = 'NONE'

    dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination

    pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(mongodb)

    try:
        # Create document in MongoDB Atlas using PyMongo.
        # First a database is created. Then a collection is created inside that database.
        # Then document is created in that collection.
        mongodb_database = mongodb.engine[database_name]
        mongodb_collection = mongodb_database[collection_name]
        inserted_doc = mongodb_collection.insert_one(origin_record)
        assert inserted_doc is not None

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        logger.info('Verifying docs updated with PyMongo...')
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][
            mongodb_atlas_destination.collection].find()]
        assert len(mongodb_documents) == len(expected_records)
        mongodb_documents_without_id = [_delete_id(doc) for doc in mongodb_documents]
        assert sorted(mongodb_documents_without_id, key=lambda x: x["id"]) == expected_records

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(database_name)


@sdc_min_version('6.0.0')
@pytest.mark.parametrize('nested', [True, False])
@pytest.mark.parametrize('element_exist', [True, False])
def test_mongodb_atlas_destination_update_map_inside_list(sdc_builder, sdc_executor, mongodb, nested, element_exist):
    """
    Ensure that an update an object inside a list structure is correctly executed. 
    The expected performance:
        - If the object exist inside the list and have all the nested unique keys, it will replace the value 
        of the specific record for the new one.
        - If the object exist inside the list exist but only had document unique key, it will replace the document 
        for the record.
        - If the object didn't exist inside the list or the document didn't exist, it will do nothing.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    database_name = get_random_string(ascii_letters, 5)
    collection_name = get_random_string(ascii_letters, 10)

    pipeline_title = 'Update - Object Inside List - '
    operation = '3'
    upsert = False
    
    if nested:
        pipeline_title = pipeline_title + 'Nested UK - '
        record = {"id": 0, "f1": [{"g1": {"h1": 1, "h2": 'Updated'}}]}
        unique_keys = [{'collectionPath': 'id', 'recordPath': ''},
                       {'collectionPath': 'f1.$[el].g1.h1', 'recordPath': ''}]
        if element_exist:
            pipeline_title = pipeline_title + 'Element exist'
            origin_record = {"id": 0, "f1": [{"g1": {"h1": 1, "h2": 'ToBeUpdated'}}, {"g1": {"h1": 2, "h2": 'NoUpdate'}}]}
            expected_records = [{"id": 0, "f1": [{"g1": {"h1": 1, "h2": 'Updated'}}, {"g1": {"h1": 2, "h2": 'NoUpdate'}}]}]
        else:
            pipeline_title = pipeline_title + 'Element no exist'
            origin_record = {"id": 0, "f1": [{"g1": {"h1": 2, "h2": 'NoUpdate'}}]}
            expected_records = [{"id": 0, "f1": [{"g1": {"h1": 2, "h2": 'NoUpdate'}}]}]
    else:
        pipeline_title = pipeline_title + 'Non-Nested UK - '
        record = {"id": 0, "f1": [{"g1": {"h1": 1, "h2": 'Updated'}}]}
        unique_keys = [{'collectionPath': 'id', 'recordPath': ''}]
        if element_exist:
            pipeline_title = pipeline_title + 'Element exist'
            origin_record = {"id": 0, "f1": [{"g1": {"h1": 1, "h2": 'ToBeUpdated'}}, {"g1": {"h1": 2, "h2": 'NoUpdate'}}]}
            expected_records = [{"id": 0, "f1": [{"g1": {"h1": 1, "h2": 'Updated'}}]}]
        else:
            pipeline_title = pipeline_title + 'Element no exist'
            origin_record = {"id": 1, "f1": [{"g1": {"h1": 2, "h2": 'NoUpdate'}}]}
            expected_records = [{"id": 1, "f1": [{"g1": {"h1": 2, "h2": 'NoUpdate'}}]}]

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=json_util.dumps(record),
                                       stop_after_first_batch=True
                                       )

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': operation}]

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=database_name,
                                             collection=collection_name,
                                             unique_keys=unique_keys,
                                             upsert=upsert)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_destination.tls_mode = 'NONE'
        mongodb_atlas_destination.authentication_method = 'NONE'

    dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination

    pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(mongodb)

    try:
        # Create document in MongoDB Atlas using PyMongo.
        # First a database is created. Then a collection is created inside that database.
        # Then document is created in that collection.
        mongodb_database = mongodb.engine[database_name]
        mongodb_collection = mongodb_database[collection_name]
        inserted_doc = mongodb_collection.insert_one(origin_record)
        assert inserted_doc is not None

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        logger.info('Verifying docs updated with PyMongo...')
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][
            mongodb_atlas_destination.collection].find()]
        assert len(mongodb_documents) == len(expected_records)
        mongodb_documents_without_id = [_delete_id(doc) for doc in mongodb_documents]
        assert sorted(mongodb_documents_without_id, key=lambda x: x["id"]) == expected_records

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(database_name)


@sdc_min_version('6.0.0')
@pytest.mark.parametrize('nested', [True, False])
@pytest.mark.parametrize('element_exist', [True, False])
def test_mongodb_atlas_destination_upsert_map_inside_list(sdc_builder, sdc_executor, mongodb, nested, element_exist):
    """
    Ensure that an upsert an object inside a list structure is correctly executed. 
    The expected performance:
        - If the object exist inside the list and have all the nested unique keys, it will replace the value 
        of the specific record for the new one.
        - If the object exist inside the list exist but only had document unique key, it will replace the document 
        for the record.
        - If the object or the document didn't exist, it will insert the record.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    database_name = get_random_string(ascii_letters, 5)
    collection_name = get_random_string(ascii_letters, 10)

    pipeline_title = 'Upsert - Object Inside List - '
    operation = '4'
    upsert = True
    
    if nested:
        pipeline_title = pipeline_title + 'Nested UK - '
        record = {"id": 0, "f1": [{"g1": {"h1": 1, "h2": 'Updated'}}]}
        unique_keys = [{'collectionPath': 'id', 'recordPath': ''},
                       {'collectionPath': 'f1.$[el].g1.h1', 'recordPath': ''}]
        if element_exist:
            pipeline_title = pipeline_title + 'Element exist'
            origin_record = {"id": 0, "f1": [{"g1": {"h1": 1, "h2": 'ToBeUpdated'}}, {"g1": {"h1": 2, "h2": 'NoUpdate'}}]}
            expected_records = [{"id": 0, "f1": [{"g1": {"h1": 1, "h2": 'Updated'}}, {"g1": {"h1": 2, "h2": 'NoUpdate'}}]}]
        else:
            pipeline_title = pipeline_title + 'Element no exist'
            origin_record = {"id": 0, "f1": [{"g1": {"h1": 2, "h2": 'NoUpdate'}}]}
            expected_records = [{"id": 0, "f1": [{"g1": {"h1": 1, "h2": 'Updated'}}, {"g1": {"h1": 2, "h2": 'NoUpdate'}}]}]
    else:
        pipeline_title = pipeline_title + 'Non-Nested UK - '
        record = {"id": 0, "f1": [{"g1": {"h1": 1, "h2": 'Updated'}}]}
        unique_keys = [{'collectionPath': 'id', 'recordPath': ''}]
        if element_exist:
            pipeline_title = pipeline_title + 'Element exist'
            origin_record = {"id": 0, "f1": [{"g1": {"h1": 1, "h2": 'ToBeUpdated'}}, {"g1": {"h1": 2, "h2": 'NoUpdate'}}]}
            expected_records = [{"id": 0, "f1": [{"g1": {"h1": 1, "h2": 'Updated'}}]}]
        else:
            pipeline_title = pipeline_title + 'Element no exist'
            origin_record = {"id": 1, "f1": [{"g1": {"h1": 2, "h2": 'NoUpdate'}}]}
            expected_records = [{"id": 0, "f1": [{"g1": {"h1": 1, "h2": 'Updated'}}]}, {"id": 1, "f1": [{"g1": {"h1": 2, "h2": 'NoUpdate'}}]}]

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=json_util.dumps(record),
                                       stop_after_first_batch=True
                                       )

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': operation}]

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=database_name,
                                             collection=collection_name,
                                             unique_keys=unique_keys,
                                             upsert=upsert)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_destination.tls_mode = 'NONE'
        mongodb_atlas_destination.authentication_method = 'NONE'

    dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination

    pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(mongodb)

    try:
        # Create document in MongoDB Atlas using PyMongo.
        # First a database is created. Then a collection is created inside that database.
        # Then document is created in that collection.
        mongodb_database = mongodb.engine[database_name]
        mongodb_collection = mongodb_database[collection_name]
        inserted_doc = mongodb_collection.insert_one(origin_record)
        assert inserted_doc is not None

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        logger.info('Verifying docs updated with PyMongo...')
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][
            mongodb_atlas_destination.collection].find()]
        assert len(mongodb_documents) == len(expected_records)
        mongodb_documents_without_id = [_delete_id(doc) for doc in mongodb_documents]
        if nested and not element_exist:
            mongodb_documents_without_id[0]['f1'].sort(key=lambda x: x['g1']['h1'])
            assert mongodb_documents_without_id == expected_records
        else:
            assert sorted(mongodb_documents_without_id, key=lambda x: x["id"]) == expected_records

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(database_name)


@sdc_min_version('6.0.0')
@pytest.mark.parametrize('nested', [True, False])
@pytest.mark.parametrize('element_exist', [True, False])
def test_mongodb_atlas_destination_update_list_inside_map(sdc_builder, sdc_executor, mongodb, nested, element_exist):
    """
    Ensure that an update a nested list inside an object structure is correctly executed. 
    The expected performance:
        - If the object exist inside the list that is inside an object, and have all the nested unique keys, it will 
        replace the value of the specific element for the new one.
        - If the object exist inside the list that is inside an object, but only had document unique key, it will 
        replace the document for the record.
        - If the element didn't exist inside the nested list or the document didn't exist, it will do nothing.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    database_name = get_random_string(ascii_letters, 5)
    collection_name = get_random_string(ascii_letters, 10)

    pipeline_title = 'Update - List Inside Object - '
    operation = '3'
    upsert = False
    
    if nested:
        pipeline_title = pipeline_title + 'Nested UK - '
        record = {"id": 0, "f1": {"g1": [{"h1": 1, "h2": 'Updated'}]}}
        unique_keys = [{'collectionPath': 'id', 'recordPath': ''},
                       {'collectionPath': 'f1.g1.$[el].h1', 'recordPath': ''}]
        if element_exist:
            pipeline_title = pipeline_title + 'Element exist'
            origin_record = {"id": 0, "f1": {"g1": [{"h1": 1, "h2": 'ToBeUpdated'}, {"h1": 2, "h2": 'NoUpdate'}]}}
            expected_records = [{"id": 0, "f1": {"g1": [{"h1": 1, "h2": 'Updated'}, {"h1": 2, "h2": 'NoUpdate'}]}}]
        else:
            pipeline_title = pipeline_title + 'Element no exist'
            origin_record = {"id": 0, "f1": {"g1": [{"h1": 2, "h2": 'NoUpdate'}]}}
            expected_records = [{"id": 0, "f1": {"g1": [{"h1": 2, "h2": 'NoUpdate'}]}}]
    else:
        pipeline_title = pipeline_title + 'Non-Nested UK - '
        record = {"id": 0, "f1": {"g1": [{"h1": 1, "h2": 'Updated'}]}}
        unique_keys = [{'collectionPath': 'id', 'recordPath': ''}]
        if element_exist:
            pipeline_title = pipeline_title + 'Element exist'
            origin_record = {"id": 0, "f1": {"g1": [{"h1": 1, "h2": 'ToBeUpdated'}, {"h1": 2, "h2": 'NoUpdate'}]}}
            expected_records = [{"id": 0, "f1": {"g1": [{"h1": 1, "h2": 'Updated'}]}}]
        else:
            pipeline_title = pipeline_title + 'Element no exist'
            origin_record = {"id": 1, "f1": {"g1": [{"h1": 2, "h2": 'NoUpdate'}]}}
            expected_records = [{"id": 1, "f1": {"g1": [{"h1": 2, "h2": 'NoUpdate'}]}}]

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=json_util.dumps(record),
                                       stop_after_first_batch=True
                                       )

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': operation}]

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=database_name,
                                             collection=collection_name,
                                             unique_keys=unique_keys,
                                             upsert=upsert)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_destination.tls_mode = 'NONE'
        mongodb_atlas_destination.authentication_method = 'NONE'

    dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination

    pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(mongodb)

    try:
        # Create document in MongoDB Atlas using PyMongo.
        # First a database is created. Then a collection is created inside that database.
        # Then document is created in that collection.
        mongodb_database = mongodb.engine[database_name]
        mongodb_collection = mongodb_database[collection_name]
        inserted_doc = mongodb_collection.insert_one(origin_record)
        assert inserted_doc is not None

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        logger.info('Verifying docs updated with PyMongo...')
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][
            mongodb_atlas_destination.collection].find()]
        assert len(mongodb_documents) == len(expected_records)
        mongodb_documents_without_id = [_delete_id(doc) for doc in mongodb_documents]
        assert sorted(mongodb_documents_without_id, key=lambda x: x["id"]) == expected_records

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(database_name)


@sdc_min_version('6.1.0')
@pytest.mark.parametrize('element_exist', [True, False])
def test_mongodb_atlas_destination_upsert_dup_key(sdc_builder, sdc_executor, mongodb, element_exist):
    """
    Ensure that an upsert a nested list, which contains objects (with same key duplicated inside it) inside an object structure is correctly executed.
    Here one of the unique keys is an object (residing at n level nested array) itself with some properties, also the property keys are duplicated.
         i.e., e.g:- here h1, h2 are duplicated as below. Also, if the unique key is h1, a match for the document is done by matching all it's
         inner properties.
                {
                    "h1": {
                        "h2": {
                            "h1": 2,
                            "h2": "h2-value"
                        }
                    }
                }
     The expected performance:
        - If the object exist inside the list that is inside an object, and have all the nested unique keys, it will
        replace the value of the specific element for the new one.
        - If the object exist inside the list that is inside an object, but only had document unique key, it will
        replace the document for the record.
        - If the element didn't exist inside the nested list or the document didn't exist, it will insert the record.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    database_name = get_random_string(ascii_letters, 5)
    collection_name = get_random_string(ascii_letters, 10)

    pipeline_title = 'Upsert - Object With Same Key Duplicated Inside It - Nested UK - '
    operation = '4'
    upsert = True

    record = {"id": 0, "f1": {"g1": [{ "h1": {"h2": {"h1": 2,"h2": "h2-value"}}}]}}
    unique_keys = [{'collectionPath': 'id', 'recordPath': ''},
                    {'collectionPath': 'f1.g1.$[el].h1', 'recordPath': ''}]
    if element_exist:
        pipeline_title = pipeline_title + 'Element exist'
        origin_record = {"id": 0, "f1": {"g1": [{ "h1": {"h2": {"h1": 2,"h2": "h2-value"}}}, { "h1": {"h2": {"h1": 3,"h2": "NoUpdate"}}}]}}
        expected_records = [{"id": 0, "f1": {"g1": [{ "h1": {"h2": {"h1" : 2,"h2" : "h2-value"}}}, { "h1": {"h2": {"h1": 3,"h2": "NoUpdate"}}}]}}]
    else:
        pipeline_title = pipeline_title + 'Element no exist'
        origin_record = {"id": 0, "f1": {"g1": [{"h1": {"h2": {"h1": 3,"h2" : "NoUpdate"}}}]}}
        expected_records = [{"id": 0, "f1": {"g1": [{"h1": {"h2": {"h1": 2,"h2": "h2-value"}}}, {"h1": {"h2": {"h1": 3,"h2": "NoUpdate"}}}]}}]

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=json_util.dumps(record),
                                       stop_after_first_batch=True
                                       )

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': operation}]

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=database_name,
                                             collection=collection_name,
                                             unique_keys=unique_keys,
                                             upsert=upsert)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_destination.tls_mode = 'NONE'
        mongodb_atlas_destination.authentication_method = 'NONE'

    dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination

    pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(mongodb)

    try:
        # Create document in MongoDB Atlas using PyMongo.
        # First a database is created. Then a collection is created inside that database.
        # Then document is created in that collection.
        mongodb_database = mongodb.engine[database_name]
        mongodb_collection = mongodb_database[collection_name]
        inserted_doc = mongodb_collection.insert_one(origin_record)
        assert inserted_doc is not None

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        logger.info('Verifying docs updated with PyMongo...')
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][
            mongodb_atlas_destination.collection].find()]
        assert len(mongodb_documents) == len(expected_records)
        mongodb_documents_without_id = [_delete_id(doc) for doc in mongodb_documents]
        if not element_exist:
            mongodb_documents_without_id[0]['f1']['g1'].sort(key=lambda x: x['h1']['h2']['h1'])
            assert mongodb_documents_without_id == expected_records
        else:
            assert sorted(mongodb_documents_without_id, key=lambda x: x["id"]) == expected_records

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(database_name)


@sdc_min_version('6.0.0')
@pytest.mark.parametrize('nested', [True, False])
@pytest.mark.parametrize('element_exist', [True, False])
def test_mongodb_atlas_destination_upsert_list_inside_map(sdc_builder, sdc_executor, mongodb, nested, element_exist):
    """
    Ensure that an upsert a nested list inside an object structure is correctly executed. 
    The expected performance:
        - If the object exist inside the list that is inside an object, and have all the nested unique keys, it will 
        replace the value of the specific element for the new one.
        - If the object exist inside the list that is inside an object, but only had document unique key, it will 
        replace the document for the record.
        - If the element didn't exist inside the nested list or the document didn't exist, it will insert the record.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    database_name = get_random_string(ascii_letters, 5)
    collection_name = get_random_string(ascii_letters, 10)

    pipeline_title = 'Upsert - Object Inside List - '
    operation = '4'
    upsert = True
    
    if nested:
        pipeline_title = pipeline_title + 'Nested UK - '
        record = {"id": 0, "f1": {"g1": [{"h1": 1, "h2": 'Updated'}]}}
        unique_keys = [{'collectionPath': 'id', 'recordPath': ''},
                       {'collectionPath': 'f1.g1.$[el].h1', 'recordPath': ''}]
        if element_exist:
            pipeline_title = pipeline_title + 'Element exist'
            origin_record = {"id": 0, "f1": {"g1": [{"h1": 1, "h2": 'ToBeUpdated'}, {"h1": 2, "h2": 'NoUpdate'}]}}
            expected_records = [{"id": 0, "f1": {"g1": [{"h1": 1, "h2": 'Updated'}, {"h1": 2, "h2": 'NoUpdate'}]}}]
        else:
            pipeline_title = pipeline_title + 'Element no exist'
            origin_record = {"id": 0, "f1": {"g1": [{"h1": 2, "h2": 'NoUpdate'}]}}
            expected_records = [{"id": 0, "f1": {"g1": [{"h1": 1, "h2": 'Updated'}, {"h1": 2, "h2": 'NoUpdate'}]}}]
    else:
        pipeline_title = pipeline_title + 'Non-Nested UK - '
        record = {"id": 0, "f1": {"g1": [{"h1": 1, "h2": 'Updated'}]}}
        unique_keys = [{'collectionPath': 'id', 'recordPath': ''}]
        if element_exist:
            pipeline_title = pipeline_title + 'Element exist'
            origin_record = {"id": 0, "f1": {"g1": [{"h1": 1, "h2": 'ToBeUpdated'}, {"h1": 2, "h2": 'NoUpdate'}]}}
            expected_records = [{"id": 0, "f1": {"g1": [{"h1": 1, "h2": 'Updated'}]}}]
        else:
            pipeline_title = pipeline_title + 'Element no exist'
            origin_record = {"id": 1, "f1": {"g1": [{"h1": 2, "h2": 'NoUpdate'}]}}
            expected_records = [{"id": 0, "f1": {"g1": [{"h1": 1, "h2": 'Updated'}]}}, {"id": 1, "f1": {"g1": [{"h1": 2, "h2": 'NoUpdate'}]}}]

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=json_util.dumps(record),
                                       stop_after_first_batch=True
                                       )

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': operation}]

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=database_name,
                                             collection=collection_name,
                                             unique_keys=unique_keys,
                                             upsert=upsert)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_destination.tls_mode = 'NONE'
        mongodb_atlas_destination.authentication_method = 'NONE'

    dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination

    pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(mongodb)

    try:
        # Create document in MongoDB Atlas using PyMongo.
        # First a database is created. Then a collection is created inside that database.
        # Then document is created in that collection.
        mongodb_database = mongodb.engine[database_name]
        mongodb_collection = mongodb_database[collection_name]
        inserted_doc = mongodb_collection.insert_one(origin_record)
        assert inserted_doc is not None

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        logger.info('Verifying docs updated with PyMongo...')
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][
            mongodb_atlas_destination.collection].find()]
        assert len(mongodb_documents) == len(expected_records)
        mongodb_documents_without_id = [_delete_id(doc) for doc in mongodb_documents]
        if nested and not element_exist:
            mongodb_documents_without_id[0]['f1']['g1'].sort(key=lambda x: x['h1'])
            assert mongodb_documents_without_id == expected_records
        else:
            assert sorted(mongodb_documents_without_id, key=lambda x: x["id"]) == expected_records

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(database_name)


UPDATE_DATA = [
    (
        'LIST_NESTED_EXIST', 
        {"id1": 0, "id2": 1, "id3": 2, "list_map": [{"id_list": 0, "val": 0}]}, 
        {"id1": 0, "id2": 1, "id3": 2, "list_map": [{"id_list": 1, "val": 1}]}, 
        [{"id1": 0, "id2": 1, "id3": 2, "list_map": [{"id_list": 1, "val": 1}]}], 
        [{'collectionPath': 'id1', 'recordPath': ''}, {'collectionPath': 'id2', 'recordPath': ''}, {'collectionPath': 'id3', 'recordPath': ''}]
    ),
    (
        'LIST_NESTED_NO_EXIST', 
        {"id1": 0, "id2": 3, "id3": 4, "list_map": [{"id_list": 0, "val": 0}]}, 
        {"id1": 0, "id2": 1, "id3": 2, "list_map": [{"id_list": 1, "val": 1}]}, 
        [{"id1": 0, "id2": 3, "id3": 4, "list_map": [{"id_list": 0, "val": 0}]}], 
        [{'collectionPath': 'id1', 'recordPath': ''}, {'collectionPath': 'id2', 'recordPath': ''}, {'collectionPath': 'id3', 'recordPath': ''}]
    ),
    (
        'MAP_NESTED_EXIST', 
        {"id1": 0, "id2": 1, "id3": 2, "map": {"id": 0, "val": 0}}, 
        {"id1": 0, "id2": 1, "id3": 2, "map": {"id": 1, "val": 1}}, 
        [{"id1": 0, "id2": 1, "id3": 2, "map": {"id": 1, "val": 1}}], 
        [{'collectionPath': 'id1', 'recordPath': ''}, {'collectionPath': 'id2', 'recordPath': ''}, {'collectionPath': 'id3', 'recordPath': ''}]
    ),
    (
        'MAP_NESTED_NO_EXIST', 
        {"id1": 0, "id2": 3, "id3": 4, "map": {"id": 0, "val": 0}}, 
        {"id1": 0, "id2": 1, "id3": 2, "map": {"id": 1, "val": 1}}, 
        [{"id1": 0, "id2": 3, "id3": 4, "map": {"id": 0, "val": 0}}], 
        [{'collectionPath': 'id1', 'recordPath': ''}, {'collectionPath': 'id2', 'recordPath': ''}, {'collectionPath': 'id3', 'recordPath': ''}]
    ),
    (
        'LIST_INSIDE_MAP_NESTED_EXIST', 
        {"id1": 0, "id2": 1, "id3": 2, "map": {"list": [{"id_list": 0, "val": 0}, {"id_list": 1, "val": 1}]}}, 
        {"id1": 0, "id2": 1, "id3": 2, "map": {"list": [{"id_list": 2, "val": 2}]}}, 
        [{"id1": 0, "id2": 1, "id3": 2, "map": {"list": [{"id_list": 2, "val": 2}]}}], 
        [{'collectionPath': 'id1', 'recordPath': ''}, {'collectionPath': 'id2', 'recordPath': ''}, {'collectionPath': 'id3', 'recordPath': ''}]
    ),
    (
        'LIST_INSIDE_MAP_NESTED_NO_EXIST', 
        {"id1": 0, "id2": 3, "id3": 4, "map": {"list": [{"id_list": 0, "val": 0}, {"id_list": 1, "val": 1}]}}, 
        {"id1": 0, "id2": 1, "id3": 2, "map": {"list": [{"id_list": 2, "val": 2}]}}, 
        [{"id1": 0, "id2": 3, "id3": 4, "map": {"list": [{"id_list": 0, "val": 0}, {"id_list": 1, "val": 1}]}}], 
        [{'collectionPath': 'id1', 'recordPath': ''}, {'collectionPath': 'id2', 'recordPath': ''}, {'collectionPath': 'id3', 'recordPath': ''}]
    ),
    (
        'MAP_INSIDE_LIST_NESTED_EXIST', 
        {"id1": 0, "id2": 1, "id3": 2, "list_map": [{"map": {"id_list": 2, "val": 2}}]}, 
        {"id1": 0, "id2": 1, "id3": 2, "list_map": [{"map": {"id_list": 0, "val": 0}}, {"map": {"id_list": 1, "val": 1}}]}, 
        [{"id1": 0, "id2": 1, "id3": 2, "list_map": [{"map": {"id_list": 0, "val": 0}}, {"map": {"id_list": 1, "val": 1}}]}], 
        [{'collectionPath': 'id1', 'recordPath': ''}, {'collectionPath': 'id2', 'recordPath': ''}, {'collectionPath': 'id3', 'recordPath': ''}]
    ),
    (
        'MAP_INSIDE_LIST_NESTED_NO_EXIST', 
        {"id1": 0, "id2": 1, "id3": 2, "list_map": [{"map": {"id_list": 0, "val": 0}}, {"map": {"id_list": 1, "val": 1}}]},
        {"id1": 0, "id2": 3, "id3": 4, "list_map": [{"map": {"id_list": 2, "val": 2}}]},  
        [{"id1": 0, "id2": 1, "id3": 2, "list_map": [{"map": {"id_list": 0, "val": 0}}, {"map": {"id_list": 1, "val": 1}}]}], 
        [{'collectionPath': 'id1', 'recordPath': ''}, {'collectionPath': 'id2', 'recordPath': ''}, {'collectionPath': 'id3', 'recordPath': ''}]
    ),
    (
        'MAP_INSIDE_LIST_WITH_NESTED_PK_EXIST',
        {"id1": 0, "id2": 1, "id3": 2, "list_map": [{"map": {"id_list": 0, "val": 0}}, {"map": {"id_list": 1, "val": 1}}]},
        {"id1": 0, "id2": 1, "id3": 2, "list_map": [{"map": {"id_list": 1, "val": 2}}]},
        [{"id1": 0, "id2": 1, "id3": 2, "list_map": [{"map": {"id_list": 0, "val": 0}}, {"map": {"id_list": 1, "val": 2}}]}], 
        [{'collectionPath': 'id1', 'recordPath': ''}, {'collectionPath': 'id2', 'recordPath': ''}, {'collectionPath': 'id3', 'recordPath': ''}, {'collectionPath': 'list_map.$[el].map.id_list', 'recordPath': ''}]
    ),
    (
        'MAP_INSIDE_LIST_WITH_NESTED_PK_NO_EXIST', 
        {"id1": 0, "id2": 1, "id3": 2, "list_map": [{"map": {"id_list": 0, "val": 0}}, {"map": {"id_list": 1, "val": 1}}]},
        {"id1": 0, "id2": 2, "id3": 2, "list_map": [{"map": {"id_list": 1, "val": 2}}]},
        [{"id1": 0, "id2": 1, "id3": 2, "list_map": [{"map": {"id_list": 0, "val": 0}}, {"map": {"id_list": 1, "val": 1}}]}], 
        [{'collectionPath': 'id1', 'recordPath': ''}, {'collectionPath': 'id2', 'recordPath': ''}, {'collectionPath': 'id3', 'recordPath': ''}, {'collectionPath': 'list_map.$[el].map.id_list', 'recordPath': ''}]
    )
]

DEBUG_UPDATE = [
    (
        'INT_HUMANA_NO_EXIST',
        {"id1": 0, "id2": 3, "id3": 4, "list_map": [{"map": {"id_list": 2, "val": 2}}]}, 
        {"id1": 0, "id2": 1, "id3": 2, "list_map": [{"map": {"id_list": 0, "val": 0}}, {"map": {"id_list": 1, "val": 1}}]}, 
        [{"id1": 0, "id2": 3, "id3": 4, "list_map": [{"map": {"id_list": 2, "val": 2}}]}], 
        [{'collectionPath': 'id1', 'recordPath': ''}, {'collectionPath': 'id2', 'recordPath': ''}, {'collectionPath': 'id3', 'recordPath': ''}]
    )
]


@pytest.mark.parametrize('structure,insert_data,input_data,expected_data,unique_keys', UPDATE_DATA)
def test_mongodb_atlas_destination_update_multi_pk(sdc_builder, sdc_executor, mongodb, structure, insert_data, 
                                                   input_data, expected_data, unique_keys):
    """
    Ensure that the update a document with multiple unique keys in different structures is correctly executed.
    The expected performance:
        - If the object exist and have first level unique keys and nested unique key, it will update the value.
        - If the object exist but only had first level unique keys, it will replace the document for the record.
        - If the element didn't exist or the document didn't exist, it will do nothing.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    database_name = get_random_string(ascii_letters, 5)
    collection_name = get_random_string(ascii_letters, 10)

    pipeline_title = 'Update Multi UK - ' + structure
    operation = '3'
    upsert = False

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=json_util.dumps(input_data),
                                       stop_after_first_batch=True
                                       )

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': operation}]

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=database_name,
                                             collection=collection_name,
                                             unique_keys=unique_keys,
                                             upsert=upsert)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_destination.tls_mode = 'NONE'
        mongodb_atlas_destination.authentication_method = 'NONE'

    dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination

    pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(mongodb)

    try:
        mongodb_database = mongodb.engine[database_name]
        mongodb_collection = mongodb_database[collection_name]
        if insert_data is not None:
            inserted_doc = mongodb_collection.insert_one(insert_data)
            assert inserted_doc is not None

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        logger.info('Verifying docs updated with PyMongo...')
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][
            mongodb_atlas_destination.collection].find()]

        assert len(mongodb_documents) == len(expected_data)
        mongodb_documents_without_id = [_delete_id(doc) for doc in mongodb_documents]
        assert sorted(mongodb_documents_without_id, key=lambda x: x["id2"], reverse=True) == expected_data

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(database_name)


UPSERT_DATA = [
    (
        'LIST_NESTED_EXIST', 
        {"id1": 0, "id2": 1, "id3": 2, "list_map": [{"id_list": 0, "val": 0}]}, 
        {"id1": 0, "id2": 1, "id3": 2, "list_map": [{"id_list": 1, "val": 1}]}, 
        [{"id1": 0, "id2": 1, "id3": 2, "list_map": [{"id_list": 1, "val": 1}]}], 
        [{'collectionPath': 'id1', 'recordPath': ''}, {'collectionPath': 'id2', 'recordPath': ''}, {'collectionPath': 'id3', 'recordPath': ''}]
    ),
    (
        'LIST_NESTED_NO_EXIST', 
        {"id1": 0, "id2": 3, "id3": 4, "list_map": [{"id_list": 0, "val": 0}]}, 
        {"id1": 0, "id2": 1, "id3": 2, "list_map": [{"id_list": 1, "val": 1}]}, 
        [{"id1": 0, "id2": 1, "id3": 2, "list_map": [{"id_list": 1, "val": 1}]}, {"id1": 0, "id2": 3, "id3": 4, "list_map": [{"id_list": 0, "val": 0}]}], 
        [{'collectionPath': 'id1', 'recordPath': ''}, {'collectionPath': 'id2', 'recordPath': ''}, {'collectionPath': 'id3', 'recordPath': ''}]
    ),
    (
        'MAP_NESTED_EXIST', 
        {"id1": 0, "id2": 1, "id3": 2, "map": {"id": 0, "val": 0}}, 
        {"id1": 0, "id2": 1, "id3": 2, "map": {"id": 1, "val": 1}}, 
        [{"id1": 0, "id2": 1, "id3": 2, "map": {"id": 1, "val": 1}}], 
        [{'collectionPath': 'id1', 'recordPath': ''}, {'collectionPath': 'id2', 'recordPath': ''}, {'collectionPath': 'id3', 'recordPath': ''}]
    ),
    (
        'MAP_NESTED_NO_EXIST', 
        {"id1": 0, "id2": 3, "id3": 4, "map": {"id": 0, "val": 0}}, 
        {"id1": 0, "id2": 1, "id3": 2, "map": {"id": 1, "val": 1}}, 
        [{"id1": 0, "id2": 1, "id3": 2, "map": {"id": 1, "val": 1}}, {"id1": 0, "id2": 3, "id3": 4, "map": {"id": 0, "val": 0}}], 
        [{'collectionPath': 'id1', 'recordPath': ''}, {'collectionPath': 'id2', 'recordPath': ''}, {'collectionPath': 'id3', 'recordPath': ''}]
    ),
    (
        'LIST_INSIDE_MAP_NESTED_EXIST', 
        {"id1": 0, "id2": 1, "id3": 2, "map": {"list": [{"id_list": 0, "val": 0}, {"id_list": 1, "val": 1}]}}, 
        {"id1": 0, "id2": 1, "id3": 2, "map": {"list": [{"id_list": 2, "val": 2}]}}, 
        [{"id1": 0, "id2": 1, "id3": 2, "map": {"list": [{"id_list": 2, "val": 2}]}}], 
        [{'collectionPath': 'id1', 'recordPath': ''}, {'collectionPath': 'id2', 'recordPath': ''}, {'collectionPath': 'id3', 'recordPath': ''}]
    ),
    (
        'LIST_INSIDE_MAP_NESTED_NO_EXIST', 
        {"id1": 0, "id2": 3, "id3": 4, "map": {"list": [{"id_list": 0, "val": 0}, {"id_list": 1, "val": 1}]}}, 
        {"id1": 0, "id2": 1, "id3": 2, "map": {"list": [{"id_list": 2, "val": 2}]}}, 
        [{"id1": 0, "id2": 1, "id3": 2, "map": {"list": [{"id_list": 2, "val": 2}]}}, {"id1": 0, "id2": 3, "id3": 4, "map": {"list": [{"id_list": 0, "val": 0}, {"id_list": 1, "val": 1}]}}], 
        [{'collectionPath': 'id1', 'recordPath': ''}, {'collectionPath': 'id2', 'recordPath': ''}, {'collectionPath': 'id3', 'recordPath': ''}]
    ),
    (
        'MAP_INSIDE_LIST_NESTED_EXIST', 
        {"id1": 0, "id2": 1, "id3": 2, "list_map": [{"map": {"id_list": 2, "val": 2}}]}, 
        {"id1": 0, "id2": 1, "id3": 2, "list_map": [{"map": {"id_list": 0, "val": 0}}, {"map": {"id_list": 1, "val": 1}}]}, 
        [{"id1": 0, "id2": 1, "id3": 2, "list_map": [{"map": {"id_list": 0, "val": 0}}, {"map": {"id_list": 1, "val": 1}}]}], 
        [{'collectionPath': 'id1', 'recordPath': ''}, {'collectionPath': 'id2', 'recordPath': ''}, {'collectionPath': 'id3', 'recordPath': ''}]
    ),
    (
        'MAP_INSIDE_LIST_NESTED_NO_EXIST', 
        {"id1": 0, "id2": 1, "id3": 2, "list_map": [{"map": {"id_list": 0, "val": 0}}, {"map": {"id_list": 1, "val": 1}}]},
        {"id1": 0, "id2": 3, "id3": 4, "list_map": [{"map": {"id_list": 2, "val": 2}}]},  
        [{"id1": 0, "id2": 1, "id3": 2, "list_map": [{"map": {"id_list": 0, "val": 0}}, {"map": {"id_list": 1, "val": 1}}]}, {"id1": 0, "id2": 3, "id3": 4, "list_map": [{"map": {"id_list": 2, "val": 2}}]}], 
        [{'collectionPath': 'id1', 'recordPath': ''}, {'collectionPath': 'id2', 'recordPath': ''}, {'collectionPath': 'id3', 'recordPath': ''}]
    ),
    (
        'MAP_INSIDE_LIST_WITH_NESTED_PK_EXIST',
        {"id1": 0, "id2": 1, "id3": 2, "list_map": [{"map": {"id_list": 0, "val": 0}}, {"map": {"id_list": 1, "val": 1}}]},
        {"id1": 0, "id2": 1, "id3": 2, "list_map": [{"map": {"id_list": 1, "val": 2}}]},
        [{"id1": 0, "id2": 1, "id3": 2, "list_map": [{"map": {"id_list": 0, "val": 0}}, {"map": {"id_list": 1, "val": 2}}]}], 
        [{'collectionPath': 'id1', 'recordPath': ''}, {'collectionPath': 'id2', 'recordPath': ''}, {'collectionPath': 'id3', 'recordPath': ''}, {'collectionPath': 'list_map.$[el].map.id_list', 'recordPath': ''}]
    ),
    (
        'MAP_INSIDE_LIST_WITH_NESTED_PK_NO_EXIST', 
        {"id1": 0, "id2": 1, "id3": 2, "list_map": [{"map": {"id_list": 0, "val": 0}}, {"map": {"id_list": 1, "val": 1}}]},
        {"id1": 0, "id2": 2, "id3": 2, "list_map": [{"map": {"id_list": 1, "val": 2}}]},
        [{"id1": 0, "id2": 1, "id3": 2, "list_map": [{"map": {"id_list": 0, "val": 0}}, {"map": {"id_list": 1, "val": 1}}]}, {"id1": 0, "id2": 2, "id3": 2, "list_map": [{"map": {"id_list": 1, "val": 2}}]}], 
        [{'collectionPath': 'id1', 'recordPath': ''}, {'collectionPath': 'id2', 'recordPath': ''}, {'collectionPath': 'id3', 'recordPath': ''}, {'collectionPath': 'list_map.$[el].map.id_list', 'recordPath': ''}]
    )
]


@sdc_min_version('6.0.0')
@pytest.mark.parametrize('structure,insert_data,input_data,expected_data,unique_keys', UPSERT_DATA)
def test_mongodb_atlas_destination_upsert_multi_pk(sdc_builder, sdc_executor, mongodb, structure, insert_data, 
                                                   input_data, expected_data, unique_keys):
    """
    Ensure that the upsert a document with multiple unique keys in different structures is correctly executed.
    The expected performance:
        - If the object exist and have first level unique keys and nested unique key, it will update the value.
        - If the object exist but only had first level unique keys, it will replace the document for the record.
        - If the element didn't exist or the document didn't exist, it will insert the record.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    database_name = get_random_string(ascii_letters, 5)
    collection_name = get_random_string(ascii_letters, 10)

    pipeline_title = 'Upsert Multi UK - ' + structure
    operation = '4'
    upsert = True

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=json_util.dumps(input_data),
                                       stop_after_first_batch=True
                                       )

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': operation}]

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=database_name,
                                             collection=collection_name,
                                             unique_keys=unique_keys,
                                             upsert=upsert)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_destination.tls_mode = 'NONE'
        mongodb_atlas_destination.authentication_method = 'NONE'

    dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination

    pipeline = pipeline_builder.build(title=pipeline_title).configure_for_environment(mongodb)

    try:
        mongodb_database = mongodb.engine[database_name]
        mongodb_collection = mongodb_database[collection_name]
        if insert_data is not None:
            inserted_doc = mongodb_collection.insert_one(insert_data)
            assert inserted_doc is not None

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        logger.info('Verifying docs updated with PyMongo...')
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][
            mongodb_atlas_destination.collection].find()]

        assert len(mongodb_documents) == len(expected_data)
        mongodb_documents_without_id = [_delete_id(doc) for doc in mongodb_documents]
        assert sorted(mongodb_documents_without_id, key=lambda x: x["id2"], reverse=False) == expected_data

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(database_name)


def _delete_id(record):
    if record.get('_id') is not None:
        record.pop('_id', None)
    return record


def _assert_nested_lists(record, expected, pk):
    for key, value in expected.items():
        record_value = record.get(key)
        if isinstance(record_value, list) and isinstance(value, list):
            sort_nested_record = sorted(record_value, key=lambda d: d[pk])
            sort_nested_value = sorted(value, key=lambda d: d[pk])
            assert sort_nested_record == sort_nested_value
        else:
            assert record_value == value
