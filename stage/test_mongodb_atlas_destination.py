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
def test_mongodb_atlas_destination(sdc_builder, sdc_executor, mongodb, operation, sdc_operation, input_data, expected_data):
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
        mongodb_atlas_destination.authentication_mechanism = 'NONE'

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
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][mongodb_atlas_destination.collection].find()]
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
        mongodb_atlas_destination.authentication_mechanism = 'NONE'

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
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][mongodb_atlas_destination.collection].find()]
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
        mongodb_atlas_destination.authentication_mechanism = 'NONE'

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


@pytest.mark.parametrize('object', ['COLLECTION', 'DATABASE'])
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
        mongodb_atlas_destination.authentication_mechanism = 'NONE'

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
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][mongodb_atlas_destination.collection].find()]
        for key, value in mongodb_documents[0].items():
            if key != '_id':
                assert value == input_data.get(key)

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(mongodb_atlas_destination.database)
