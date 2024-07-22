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


DATA = ['To be or not to be.',
        'Excellence is not an act, it is a habit.',
        'No pains, no gains.']


@pytest.mark.parametrize('operation,sdc_operation,input_data,expected_data', [
    ('INSERT', '1', {"f1": {"f2": "a"}, "f3": "b"}, {"f1": {"f2": "a"}, "f3": "b"}),
    ('DELETE', '2', {"f3": "b"}, {"f1": {"f2": "a"}}),
    ('UPDATE', '3', {"f3": "c"}, {"f1": {"f2": "a"}, "f3": "c"}),
    ('UPSERT', '4', {"f3": "c"}, {"f1": {"f2": "a"}, "f3": "c"})
])
def test_mongodb_atlas_destination(sdc_builder, sdc_executor, mongodb, operation, sdc_operation, input_data,
                                   expected_data):
    """
    Send simple text into MongoDB Atlas destination from Dev Raw Data Source and confirm that MongoDB Atlas correctly
        received them using PyMongo.

    The pipeline looks like:
        dev_raw_data_source >> record_deduplicator >> expression_evaluator >> mongodb_atlas_destination
                               record_deduplicator >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=json.dumps(input_data), stop_after_first_batch=True)

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': sdc_operation}]

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=get_random_string(ascii_letters, 5),
                                             collection=get_random_string(ascii_letters, 10),
                                             unique_keys=[{'collectionPath': '/f3', 'recordPath': ''}])

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
        if operation in ('DELETE', 'UPDATE', 'UPSERT'):
            input_data_1 = {"f1": {"f2": "a"}}
            input_data_2 = {"f3": "b"}

            # Create document in MongoDB Atlas using PyMongo.
            # First a database is created. Then a collection is created inside that database.
            # Then document is created in that collection.
            logger.info('Adding document into %s collection using PyMongo...', mongodb_atlas_destination.collection)
            mongodb_database = mongodb.engine[mongodb_atlas_destination.database]
            mongodb_collection = mongodb_database[mongodb_atlas_destination.collection]
            inserted_doc = mongodb_collection.insert_one(input_data_1)
            assert inserted_doc is not None
            if operation != 'UPSERT':
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
        for key, value in mongodb_documents[0].items():
            if key != '_id':
                assert value == expected_data.get(key)

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(mongodb_atlas_destination.database)


@pytest.mark.parametrize('with_record_path', [True, False])
def test_mongodb_atlas_destination_update_on_nested_unique_key(sdc_builder, sdc_executor, mongodb, with_record_path):
    """
    Ensure that an update on a document with a nested unique field is correctly executed

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')
    record = {"f1": {"f2": "a"}, "f3": "b"}
    if with_record_path:
        unique_keys = [{'collectionPath': '/f1/f2', 'recordPath': 'f1.f2'}]
    else:
        unique_keys = [{'collectionPath': '/f1/f2', 'recordPath': ''}]

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=json.dumps(record))

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': '3'}]

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=get_random_string(ascii_letters, 5),
                                             collection=get_random_string(ascii_letters, 10),
                                             unique_keys=unique_keys)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_destination.tls_mode = 'NONE'
        mongodb_atlas_destination.authentication_method = 'NONE'

    dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        # Change value of field which will be updated
        record["f3"] = "c"

        # Create document in MongoDB Atlas using PyMongo.
        # First a database is created. Then a collection is created inside that database.
        # Then document is created in that collection.
        logger.info('Adding document into %s collection using PyMongo...', mongodb_atlas_destination.collection)
        mongodb_database = mongodb.engine[mongodb_atlas_destination.database]
        mongodb_collection = mongodb_database[mongodb_atlas_destination.collection]
        inserted_doc = mongodb_collection.insert_one(record)
        assert inserted_doc is not None

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(1)
        sdc_executor.stop_pipeline(pipeline)

        logger.info('Verifying docs updated with PyMongo...')
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][
            mongodb_atlas_destination.collection].find()]
        assert len(mongodb_documents) == 1
        assert mongodb_documents[0]["f3"] == "b"

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(mongodb_atlas_destination.database)


def test_mongodb_atlas_destination_unique_keys(sdc_builder, sdc_executor, mongodb):
    """
    Insert 1 simple document in MongoDB Atlas with dynamic and explicit unique keys and confirm it works in
        MongoDB Atlas.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    database = get_random_string(ascii_letters, 5)
    collection = get_random_string(ascii_letters, 10)

    input_data = {'f1': 'a', 'f2': 'f1'}
    unique_key = 'f1'

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=json.dumps(input_data), stop_after_first_batch=True)

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': '1'},
                                                         {'attributeToSet': 'bsonKey',
                                                          'headerAttributeExpression': 'True'}]

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=database,
                                             collection=collection,
                                             unique_keys=[{'collectionPath': unique_key, 'recordPath': unique_key}])

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_destination.tls_mode = 'NONE'
        mongodb_atlas_destination.authentication_method = 'NONE'

    dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        logger.info('Verifying docs updated with PyMongo...')
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][
            mongodb_atlas_destination.collection].find()]
        assert len(mongodb_documents) == 1
        assert mongodb_documents[0].get('f2') == 'f1'

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
    else:
        collection = get_random_string(ascii_letters, 5)
        database = prefix + '${record:value(\'/name\')}'

    input_data = {'data': 'a', 'name': 'test'}

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=json.dumps(input_data), stop_after_first_batch=True)

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

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

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
        for key, value in mongodb_documents[0].items():
            if key != '_id':
                assert value == input_data.get(key)

    finally:
        logger.info('Dropping %s database...', database)
        mongodb.engine.drop_database(database)


@pytest.mark.parametrize('prefix', ['.', '$', '/'])
def test_mongodb_atlas_destination_reserved_field_names(sdc_builder, sdc_executor, mongodb, prefix):
    """
    Insert 1 simple document in MongoDB Atlas with reserved field names with periods (.), dollar digns ($) and
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

    input_data = {f"{prefix}db": "a", f"{prefix}inc": "d", f"{prefix}ref": "c"}

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=json.dumps(input_data), stop_after_first_batch=True)

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': '1'}]

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=database,
                                             collection=collection)

    dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        # Start pipeline and verify the documents using PyMongo.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        logger.info('Verifying docs updated with PyMongo...')
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][
            mongodb_atlas_destination.collection].find()]
        for key, value in mongodb_documents[0].items():
            if key != '_id':
                assert value == input_data.get(key)

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(mongodb_atlas_destination.database)


@sdc_min_version('5.5.0')
@pytest.mark.parametrize('operation', ['3', '4'])
@pytest.mark.parametrize('nested', [True, False])
def test_mongodb_atlas_destination_update_map(sdc_builder, sdc_executor, mongodb, operation, nested):
    """
    Ensure that an update and upsert a nested documents with map structure is correctly executed.
     - UPDATE: if the map exist, it will replace the value of the specific record for the new one.
     If it didn't exist, it will do nothing.
     - UPSERT: if the map exist, it will replace the value of the specific record for the new one.
     If it didn't exist, it will insert it in the document.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    database_name = get_random_string(ascii_letters, 5)
    collection_name = get_random_string(ascii_letters, 10)

    pipeline_title = 'Map Structure - '
    if operation == '3':
        upsert = False
        pipeline_title = pipeline_title + 'Update - '
        if nested:
            pipeline_title = pipeline_title + 'Nested Element'
            origin_record = {"id": 0, "f1": {"g1": 1, "g2": {"h1": 2, "h2": "toBeUpdated"}, "f2": "NoUpdate"}}
            record = {"id": 0, "f1": {"g1": 1, "g2": {"h1": 2, "h2": "Updated"}}}
            expected_record = {"id": 0, "f1": {"g1": 1, "g2": {"h1": 2, "h2": "Updated"}, "f2": "NoUpdate"}}
            unique_keys = [{'collectionPath': 'id', 'recordPath': ''},
                           {'collectionPath': 'f1.g1', 'recordPath': ''},
                           {'collectionPath': 'f1.g2.h1', 'recordPath': ''}]
        else:
            pipeline_title = pipeline_title + 'Non-Nested Element'
            origin_record = {"id": 0, "f1": {"g1": 1, "g2": "toBeUpdated"}, "f2": "NoUpdate"}
            record = {"id": 0, "f1": {"g1": 1, "g2": "Updated"}}
            expected_record = {"id": 0, "f1": {"g1": 1, "g2": "Updated"}, "f2": "NoUpdate"}
            unique_keys = [{'collectionPath': 'id', 'recordPath': ''}]
    else:
        upsert = True
        pipeline_title = pipeline_title + 'Upsert - '
        if nested:
            pipeline_title = pipeline_title + 'Nested Element'
            origin_record = {"id": 0, "f1": {"g1": 1, "g2": "ToBeUpdate"}}
            record = {"id": 0, "f1": {"g1": 1, "g2": "Updated"}}
            expected_record = {"id": 0, "f1": {"g1": 1, "g2": "Updated"}}
            unique_keys = [{'collectionPath': 'id', 'recordPath': ''},
                           {'collectionPath': 'f1.g1', 'recordPath': ''}]
        else:
            pipeline_title = pipeline_title + 'Non-Nested Element'
            origin_record = {"id": 9}
            record = {"id": 0, "f1": {"g1": 1, "g2": "Updated"}}
            expected_record = {"id": 0, "f1": {"g1": 1, "g2": "Updated"}}
            unique_keys = [{'collectionPath': 'id', 'recordPath': ''}]

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
        if operation == '3':
            assert inserted_doc is not None
        else:
            # Left the collection in the database empty
            mongodb_collection.find_one_and_delete(origin_record)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        logger.info('Verifying docs updated with PyMongo...')
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][
            mongodb_atlas_destination.collection].find()]

        assert len(mongodb_documents) == 1
        assert _delete_id(mongodb_documents[0]) == expected_record

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(database_name)


@sdc_min_version('5.5.0')
@pytest.mark.parametrize('operation', ['3', '4'])
def test_mongodb_atlas_destination_update_simple_list(sdc_builder, sdc_executor, mongodb, operation):
    """
    Ensure that an update and upsert a document with simple list structure is correctly executed.
     - UPDATE: if the document exist, it will replace the list of the specific field for the new one.
     If it didn't exist, it will do nothing.
     - UPSERT: if the document exist, it will replace the list of the specific field for the new one.
     If it didn't exist, it will insert it in a new document.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    database_name = get_random_string(ascii_letters, 5)
    collection_name = get_random_string(ascii_letters, 10)

    pipeline_title = 'Simple List Structure- '
    origin_record = {"id": 0, "f1": [1, 2, 3, 4]}
    if operation == '3':
        upsert = False
        pipeline_title = pipeline_title + 'Update'
        record = {"id": 0, "f1": [1, 2, 3, 4, 5]}
        expected_record = {"id": 0, "f1": [1, 2, 3, 4, 5]}

    else:
        upsert = True
        pipeline_title = pipeline_title + 'Upsert'
        record = {"id": 0, "f1": [1, 2, 3, 4, 5]}
        expected_record = {"id": 0, "f1": [1, 2, 3, 4, 5]}

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
                                             unique_keys=[{'collectionPath': 'id', 'recordPath': ''}],
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
        if operation == '3':
            assert inserted_doc is not None
        else:
            # Left the collection in the database empty
            mongodb_collection.find_one_and_delete(origin_record)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        logger.info('Verifying docs updated with PyMongo...')
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][
            mongodb_atlas_destination.collection].find()]

        assert len(mongodb_documents) == 1
        record = _delete_id(mongodb_documents[0])
        assert record == expected_record

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(database_name)


@sdc_min_version('5.5.0')
@pytest.mark.parametrize('operation', ['3', '4'])
@pytest.mark.parametrize('nested', [True, False])
def test_mongodb_atlas_destination_update_list(sdc_builder, sdc_executor, mongodb, operation, nested):
    """
    Ensure that an update and upsert a nested documents with list structure is correctly executed.
     - UPDATE: if the element inside the list exist, it will replace the value of the element for the new one.
     If it didn't exist, it will do nothing.
     - UPSERT: if the element inside the list exist, it will replace the value of the element for the new one.
     If it didn't exist, it will insert the element in the list.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    database_name = get_random_string(ascii_letters, 5)
    collection_name = get_random_string(ascii_letters, 10)

    pipeline_title = 'List Structure - '
    if operation == '3':
        upsert = False
        pipeline_title = pipeline_title + 'Update - '
        if nested:
            pipeline_title = pipeline_title + 'Nested Element'
            origin_record = {"id": 0, "f1": [{"g1": 1, "g2": "toBeUpdated"}, {"g1": 2, "g2": "NoUpdate"}]}
            record = {"id": 0, "f1": [{"g1": 1, "g2": "Updated"}, {"g1": 2, "g2": "NoUpdate"}]}
            expected_record = {"id": 0, "f1": [{"g1": 1, "g2": "Updated"}, {"g1": 2, "g2": "NoUpdate"}]}
            unique_keys = [{'collectionPath': 'id', 'recordPath': ''},
                           {'collectionPath': 'f1.$[el].g1', 'recordPath': ''}]
        else:
            pipeline_title = pipeline_title + 'Non-Nested Element'
            origin_record = {"id": 0, "f1": [{"g1": 1, "g2": "toBeUpdated"}, {"g1": 2, "g2": "NoUpdate"}]}
            record = {"id": 0, "f1": [{"g1": 1, "g2": "Updated"}, {"g1": 2, "g2": "NoUpdate"}]}
            expected_record = {"id": 0, "f1": [{"g1": 1, "g2": "Updated"}, {"g1": 2, "g2": "NoUpdate"}]}
            unique_keys = [{'collectionPath': 'id', 'recordPath': ''}]

    else:
        upsert = True
        pipeline_title = pipeline_title + 'Upsert - '
        if nested:
            pipeline_title = pipeline_title + 'Nested Element'
            origin_record = {"id": 0, "f1": [{"g1": 2, "g2": "NoUpdate"}]}
            record = {"id": 0, "f1": [{"g1": 1, "g2": "Updated"}, {"g1": 2, "g2": "NoUpdate"}]}
            expected_record = {"id": 0, "f1": [{"g1": 1, "g2": "Updated"}, {"g1": 2, "g2": "NoUpdate"}]}
            unique_keys = [{'collectionPath': 'id', 'recordPath': ''},
                           {'collectionPath': 'f1.$[el].g1', 'recordPath': ''}]
        else:
            pipeline_title = pipeline_title + 'Non-Nested Element'
            origin_record = {"id": 0, "f1": [{"g1": 2, "g2": "NoUpdate"}]}
            record = {"id": 0, "f1": [{"g1": 1, "g2": "Updated"}, {"g1": 2, "g2": "NoUpdate"}]}
            expected_record = {"id": 0, "f1": [{"g1": 1, "g2": "Updated"}, {"g1": 2, "g2": "NoUpdate"}]}
            unique_keys = [{'collectionPath': 'id', 'recordPath': ''}]

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

        assert len(mongodb_documents) == 1
        record = _delete_id(mongodb_documents[0])

        _assert_nested_lists(record, expected_record, 'g1')

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(database_name)


@sdc_min_version('5.5.0')
@pytest.mark.parametrize('operation', ['3', '4'])
@pytest.mark.parametrize('nested', [True, False])
@pytest.mark.parametrize('record_format', ['ONLY_ELEMENT_UPDATE', 'FULL_RECORD'])
def test_mongodb_atlas_destination_update_list_inside_map(sdc_builder, sdc_executor, mongodb, operation, nested,
                                                          record_format):
    """
    Ensure that an update and upsert a list inside a map structure is correctly executed.
     - UPDATE: if the list inside the map exist, it will replace the value of the specific record for the new one.
     If it didn't exist, it will do nothing.
     - UPSERT: if the list inside the map exist, it will replace the value of the specific record for the new one.
     If it didn't exist, it will insert it in the document.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    if record_format == 'ONLY_ELEMENT_UPDATE' and not nested:
        pytest.skip("Do not allow updating on an element within a document with only the document's unique key")

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    database_name = get_random_string(ascii_letters, 5)
    collection_name = get_random_string(ascii_letters, 10)

    pipeline_title = 'List inside Map Structure - '
    origin_record = {"id": 0, "f1": {"g1": [{'h1': 1, 'h2': "ToBeUpdated"}], "g2": "NoUpdate"}}
    expected_record = {"id": 0, "f1": {"g1": [{'h1': 1, 'h2': "Updated"}], "g2": "NoUpdate"}}
    if record_format == 'ONLY_ELEMENT_UPDATE':
        pipeline_title = pipeline_title + 'Only Element - '
        record = {"id": 0, "f1": {"g1": [{'h1': 1, 'h2': "Updated"}]}}
    else:
        pipeline_title = pipeline_title + 'Full Record - '
        record = {"id": 0, "f1": {"g1": [{'h1': 1, 'h2': "Updated"}], "g2": "NoUpdate"}}

    if operation == '3':
        upsert = False
        pipeline_title = pipeline_title + 'Update - '
    else:
        upsert = True
        pipeline_title = pipeline_title + 'Upsert - '

    if nested:
        pipeline_title = pipeline_title + 'Nested Element'
        unique_keys = [{'collectionPath': 'id', 'recordPath': ''},
                       {'collectionPath': 'f1.g1.$[el].h1', 'recordPath': ''}]
    else:
        pipeline_title = pipeline_title + 'Non-Nested Element'
        unique_keys = [{'collectionPath': 'id', 'recordPath': ''}]

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

        assert len(mongodb_documents) == 1
        record = _delete_id(mongodb_documents[0])

        _assert_nested_lists(record, expected_record, 'g1')

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(database_name)


@sdc_min_version('6.0.0')
@pytest.mark.parametrize('operation', ['3', '4'])
@pytest.mark.parametrize('nested', [True, False])
@pytest.mark.parametrize('record_format', ['ONLY_ELEMENT_UPDATE', 'FULL_RECORD'])
def test_mongodb_atlas_destination_update_map_inside_list(sdc_builder, sdc_executor, mongodb, operation, nested,
                                                          record_format):
    """
    Ensure that an update and upsert a map inside a list structure is correctly executed.
     - UPDATE: if the map inside the list exist, it will replace the value of the specific record for the new one.
     If it didn't exist, it will do nothing.
     - UPSERT: if the map inside the list exist, it will replace the value of the specific record for the new one.
     If it didn't exist, it will insert it in the document.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    if record_format == 'ONLY_ELEMENT_UPDATE' and not nested:
        pytest.skip("Do not allow updating on an element within a document with only the document's unique key")

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    database_name = get_random_string(ascii_letters, 5)
    collection_name = get_random_string(ascii_letters, 10)

    pipeline_title = 'Map inside List Structure - '
    origin_record = {"id": 0, "f1": [{"g1": {'h1': 1, 'h2': "ToBeUpdated"}}, {"g2": "NoUpdate"}]}
    expected_record = {"id": 0, "f1": [{"g1": {'h1': 1, 'h2': "Updated"}}, {"g2": "NoUpdate"}]}
    if record_format == 'ONLY_ELEMENT_UPDATE':
        pipeline_title = pipeline_title + 'Only Element - '
        record = {"id": 0, "f1": [{"g1": {'h1': 1, 'h2': "Updated"}}]}
    else:
        pipeline_title = pipeline_title + 'Full Record - '
        record = {"id": 0, "f1": [{"g1": {'h1': 1, 'h2': "Updated"}}, {"g2": "NoUpdate"}]}

    if operation == '3':
        upsert = False
        pipeline_title = pipeline_title + 'Update'
    else:
        upsert = True
        pipeline_title = pipeline_title + 'Upsert'

    if nested:
        pipeline_title = pipeline_title + 'Nested Element'
        unique_keys = [{'collectionPath': 'id', 'recordPath': ''},
                       {'collectionPath': 'f1.$[el].g1.h1', 'recordPath': ''}]
    else:
        pipeline_title = pipeline_title + 'Non-Nested Element'
        unique_keys = [{'collectionPath': 'id', 'recordPath': ''}]

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

        assert len(mongodb_documents) == 1
        record = _delete_id(mongodb_documents[0])
        assert record == expected_record

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(database_name)


@sdc_min_version('5.5.0')
@pytest.mark.parametrize('origin_record', [
    {"id": 0, "f1": [{"g1": "a", "g2": "b"}, {"g1": "a", "g2": "b"}], "f2": 2},
    {"id": 1, "f1": [{"nest_id": "a", "g1": "c"}, {"nest_id": "b", "g1": "d"}], "f2": 2}
])
def test_mongodb_atlas_destination_upsert_without_unique_key(sdc_builder, sdc_executor, mongodb, origin_record):
    """
    Ensure that an upsert a list without the correct pk is correctly executed.
     - UPSERT: if the document exist but the list don't had the pk, it will insert the element in the list.
     If the document didn't exist, it will insert it in a new document.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_atlas_destination
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    database_name = get_random_string(ascii_letters, 5)
    collection_name = get_random_string(ascii_letters, 10)

    pipeline_title = 'List without PK - Upsert -'
    record = {"id": 0, "f1": [{"nest_id": "a", "g1": "a"}], "f2": 2}
    unique_keys = [{'collectionPath': 'id', 'recordPath': ''},
                   {'collectionPath': 'f1.$[ident].nest_id', 'recordPath': ''}]

    if origin_record.get("id") == 0:
        pipeline_title = pipeline_title + 'Element'
        expected_records = {"id": 0, "f1": [{"g1": "a", "g2": "b"}, {"g1": "a", "g2": "b"}], "f2": 2}
    else:
        pipeline_title = pipeline_title + 'Document'
        expected_records = [{"id": 0, "f1": [{"nest_id": "a", "g1": "a"}], "f2": 2},
                            {"id": 1, "f1": [{"nest_id": "a", "g1": "c"}, {"nest_id": "b", "g1": "d"}], "f2": 2}
                            ]

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=json_util.dumps(record),
                                       stop_after_first_batch=True
                                       )

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': '4'}]

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=database_name,
                                             collection=collection_name,
                                             unique_keys=unique_keys,
                                             upsert=True)

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

        if origin_record.get("id") == 0:
            assert len(mongodb_documents) == 1
            record = _delete_id(mongodb_documents[0])
            assert record == expected_records
        else:
            assert len(mongodb_documents) == len(expected_records)
            records = [_delete_id(docs) for docs in mongodb_documents]
            sort_records = sorted(records, key=lambda d: d['id'])
            sort_expected_records = sorted(expected_records, key=lambda d: d['id'])
            for i in range(len(records)):
                _assert_nested_lists(sort_records[i], sort_expected_records[i], 'nest_id')

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
