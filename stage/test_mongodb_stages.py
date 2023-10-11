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

import copy
import json
import logging
import time
import string
import random
import datetime
from string import ascii_letters

import pytest
from bson import binary, DBRef, decimal128
from streamsets.sdk.exceptions import StartError
from streamsets.sdk.utils import Version
from streamsets.testframework.markers import mongodb, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

ORIG_DOCS = [
    {'name': 'Flute'},
    {'name': 'Oboe'},
    {'name': 'Violin'}
]

NESTED_DOC = [
    {
        "data": {
            "data": {
                "foo": "bar"
            },
            "metadata": {
                "created_time": "2018-03-22T02:24:06.945319214Z",
                "deletion_time": "",
                "destroyed": False,
                "version": 1
            }
        }
    }
]

NESTED_DOCS = [
    {
        'name': 'StreamSets',
        'location': {
            'city': 'San Francisco',
            'state': 'California'
        }
    },
    {
        'name': 'MongoDB',
        'location': {
            'city': 'Palo Alto',
            'state': 'California'
        }
    }
]

DATA = ['To be or not to be.',
        'Excellence is not an act, it is a habit.',
        'No pains, no gains.']


@mongodb('legacy')
def test_mongodb_oplog_origin(sdc_builder, sdc_executor, mongodb):
    """
    Insert data in MongoDB and then check if MongoDB Oplog origin captures changes in data from MongoDB correctly.

    The pipeline looks like:
        mongodb_oplog >> wiretap
    """
    if mongodb.atlas:
        pytest.skip("MongoDB stages don't support connect to MongoDB Atlas")

    input_rec_count = 6
    input_rec_count2 = 2
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    time_now = int(time.time())
    mongodb_oplog = pipeline_builder.add_stage('MongoDB Oplog')
    database_name = get_random_string(ascii_letters, 10)
    # Specify that MongoDB Oplog needs to read changes occurring after time_now.
    mongodb_oplog.set_attributes(collection='oplog.rs', initial_timestamp_in_secs=time_now, initial_ordinal=1)

    wiretap = pipeline_builder.add_wiretap()
    mongodb_oplog >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    sdc_executor.add_pipeline(pipeline)

    try:
        # Insert documents in MongoDB using PyMongo.
        # First a database is created. Then a collection is created inside that database.
        # Then documents are inserted in that collection.
        mongodb_database = mongodb.engine[database_name]
        mongodb_collection = mongodb_database[get_random_string(ascii_letters, 10)]
        inserted_list = mongodb_collection.insert_many([{'x': i} for i in range(input_rec_count)])
        assert len(inserted_list.inserted_ids) == input_rec_count

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', input_rec_count)
        sdc_executor.stop_pipeline(pipeline)

        assert len(wiretap.output_records) == input_rec_count
        for i, record in enumerate(wiretap.output_records):
            assert record.field['o']['x'].value == i
            # Verify the operation type is 'i' which is for 'insert' since we inserted the records earlier.
            assert record.field['op'].value == 'i'
            assert record.field['ts']['timestamp'].value.timestamp() >= time_now

        # Now we want to make sure that the previous offset is respected over the
        # configured initial timestamp and ordinal
        inserted_list2 = mongodb_collection.insert_many([{'x': i} for i in range(input_rec_count2)])
        assert len(inserted_list2.inserted_ids) == input_rec_count2

        wiretap.reset()

        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', input_rec_count2)
        sdc_executor.stop_pipeline(pipeline)

        assert len(wiretap.output_records) == 2
    finally:
        logger.info('Dropping %s database...', database_name)
        mongodb.engine.drop_database(database_name)


@mongodb('legacy')
def test_mongodb_origin_simple(sdc_builder, sdc_executor, mongodb):
    """
    Create 3 simple documents in MongoDB and confirm that MongoDB origin reads them.

    The pipeline looks like:
        mongodb_origin >> wiretap
    """
    if mongodb.atlas:
        pytest.skip("MongoDB stages don't support connect to MongoDB Atlas")

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_origin = pipeline_builder.add_stage('MongoDB', type='origin')
    mongodb_origin.set_attributes(capped_collection=False,
                                  database=get_random_string(ascii_letters, 5),
                                  collection=get_random_string(ascii_letters, 10))

    wiretap = pipeline_builder.add_wiretap()
    mongodb_origin >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        # MongoDB and PyMongo add '_id' to the dictionary entries e.g. docs_in_database
        # when used for inserting in collection. Hence the deep copy.
        docs_in_database = copy.deepcopy(ORIG_DOCS)

        # Create documents in MongoDB using PyMongo.
        # First a database is created. Then a collection is created inside that database.
        # Then documents are created in that collection.
        logger.info('Adding documents into %s collection using PyMongo...', mongodb_origin.collection)
        mongodb_database = mongodb.engine[mongodb_origin.database]
        mongodb_collection = mongodb_database[mongodb_origin.collection]
        insert_list = [mongodb_collection.insert_one(doc) for doc in docs_in_database]
        assert len(insert_list) == len(docs_in_database)

        # Start pipeline and verify the documents using snaphot.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(docs_in_database))
        sdc_executor.stop_pipeline(pipeline)

        assert ORIG_DOCS == [{'name': record.field['name'].value} for record in wiretap.output_records]
    finally:
        logger.info('Dropping %s database...', mongodb_origin.database)
        mongodb.engine.drop_database(mongodb_origin.database)


@mongodb('legacy')
@sdc_min_version('3.5.1')
def test_mongodb_origin_DBRef_type(sdc_builder, sdc_executor, mongodb):
    """
    DBRef datatype is a reference to a document in other collection.
    Step 1. Create two collections(#1 and #2) in MongoDB and add sample documents in collection #1.
    Step 2. Add test documents to collection #2 which refer to the sample documents in collection #1.
    Step 3. Confirm that MongoDB origin reads the test documents from collection #2

    The pipeline looks like:
        mongodb_origin >> wiretap
    """
    if mongodb.atlas:
        pytest.skip("MongoDB stages don't support connect to MongoDB Atlas")

    database_name = get_random_string(ascii_letters, 5)
    collection1 = get_random_string(ascii_letters, 10)
    collection2 = get_random_string(ascii_letters, 10)
    logger.debug('database_name: %s, collection1: %s, collection2: %s', database_name, collection1, collection2)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_origin = pipeline_builder.add_stage('MongoDB', type='origin')
    mongodb_origin.set_attributes(capped_collection=False,
                                  database=database_name,
                                  collection=collection2)

    wiretap = pipeline_builder.add_wiretap()
    mongodb_origin >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(mongodb)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Step 1. A sample documents to collection #1
        docs_in_database = copy.deepcopy(ORIG_DOCS)
        mongodb_database = mongodb.engine[database_name]
        mongodb_collection1 = mongodb_database[collection1]
        insert_list = [mongodb_collection1.insert_one(doc) for doc in docs_in_database]
        num_of_records = len(insert_list)
        logger.info('Added %i documents into %s collection', num_of_records, collection1)

        # Step 2. Generate test documents with DBRef datatype and insert to collection #2
        logger.info('Adding test documents into %s collection...', collection2)
        mongodb_collection2 = mongodb_database[collection2]
        docs_in_col1 = []  # This will be used to compare the result
        # Obtain sample document's _id from collection #1 and assign it to test document as inserting into collection #2
        for doc in mongodb_collection1.find().sort('_id', 1):
            # Sort by _id so that we can compare the result easily later
            mongodb_collection2.insert_one({'test_ref': DBRef(collection=collection1, id=doc['_id'])})
            docs_in_col1.append(doc)

        # Step 3. Start pipeline and verify the documents using wiretap.
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)
        for record, expected in zip(wiretap.output_records, docs_in_col1):
            assert record.get_field_data('/test_ref/$ref') == collection1
            assert record.get_field_data('/test_ref/$id') == str(expected['_id'])

    finally:
        logger.info('Dropping %s database...', database_name)
        mongodb.engine.drop_database(mongodb_origin.database)


@mongodb('legacy')
@sdc_min_version('3.0.1.0')
def test_mongodb_origin_simple_with_BSONBinary(sdc_builder, sdc_executor, mongodb):
    """
    Create 3 simple documents consists with BSON Binary data type in MongoDB and confirm that MongoDB origin reads them.

    The pipeline looks like:
        mongodb_origin >> wiretap
    """
    if mongodb.atlas:
        pytest.skip("MongoDB stages don't support connect to MongoDB Atlas")

    ORIG_BINARY_DOCS = [
        {'data': binary.Binary(b'Binary Data Flute')},
        {'data': binary.Binary(b'Binary Data Oboe')},
        {'data': binary.Binary(b'Binary Data Violin')}
    ]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_origin = pipeline_builder.add_stage('MongoDB', type='origin')
    mongodb_origin.set_attributes(capped_collection=False,
                                  database=get_random_string(ascii_letters, 5),
                                  collection=get_random_string(ascii_letters, 10))

    wiretap = pipeline_builder.add_wiretap()
    mongodb_origin >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        # MongoDB and PyMongo add '_id' to the dictionary entries e.g. docs_in_database
        # when used for inserting in collection. Hence the deep copy.
        docs_in_database = copy.deepcopy(ORIG_BINARY_DOCS)

        # Create documents in MongoDB using PyMongo.
        # First a database is created. Then a collection is created inside that database.
        # Then documents are created in that collection.
        logger.info('Adding documents into %s collection using PyMongo...', mongodb_origin.collection)
        mongodb_database = mongodb.engine[mongodb_origin.database]
        mongodb_collection = mongodb_database[mongodb_origin.collection]
        insert_list = [mongodb_collection.insert_one(doc) for doc in docs_in_database]
        assert len(insert_list) == len(docs_in_database)

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(docs_in_database))
        sdc_executor.stop_pipeline(pipeline)

        assert [{'data': str(record.field['data'])} for record in wiretap.output_records] == [
            {'data': str(record.get('data'))} for record in ORIG_BINARY_DOCS]
    finally:
        logger.info('Dropping %s database...', mongodb_origin.database)
        mongodb.engine.drop_database(mongodb_origin.database)


@mongodb('legacy')
@sdc_min_version('3.8.3')
def test_mongodb_origin_simple_with_decimal(sdc_builder, sdc_executor, mongodb):
    """
    Validate that we properly process decimal type.

    The pipeline looks like:
        mongodb_origin >> wiretap
    """
    if mongodb.atlas:
        pytest.skip("MongoDB stages don't support connect to MongoDB Atlas")

    ORIG_BINARY_DOCS = [{'data': decimal128.Decimal128("0.5")}]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_origin = pipeline_builder.add_stage('MongoDB', type='origin')
    mongodb_origin.set_attributes(capped_collection=False,
                                  database=get_random_string(ascii_letters, 5),
                                  collection=get_random_string(ascii_letters, 10))

    wiretap = pipeline_builder.add_wiretap()
    mongodb_origin >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        # MongoDB and PyMongo add '_id' to the dictionary entries e.g. docs_in_database
        # when used for inserting in collection. Hence the deep copy.
        docs_in_database = copy.deepcopy(ORIG_BINARY_DOCS)

        # Create documents in MongoDB using PyMongo.
        # First a database is created. Then a collection is created inside that database.
        # Then documents are created in that collection.
        logger.info('Adding documents into %s collection using PyMongo...', mongodb_origin.collection)
        mongodb_database = mongodb.engine[mongodb_origin.database]
        mongodb_collection = mongodb_database[mongodb_origin.collection]
        insert_list = [mongodb_collection.insert_one(doc) for doc in docs_in_database]
        assert len(insert_list) == len(docs_in_database)

        # Verify that insert was in-fact successful
        assert docs_in_database == [item
                                    for item in
                                    mongodb.engine[mongodb_origin.database][mongodb_origin.collection].find()]

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(docs_in_database))
        sdc_executor.stop_pipeline(pipeline)

        assert [{'data': decimal128.Decimal128(str(record.field['data']))} for record in
                wiretap.output_records] == ORIG_BINARY_DOCS
    finally:
        logger.info('Dropping %s database...', mongodb_origin.database)
        mongodb.engine.drop_database(mongodb_origin.database)


@mongodb('legacy')
def test_mongodb_origin_nested_field_offset(sdc_builder, sdc_executor, mongodb):
    """
    Create 1 simple document with nested fields in MongoDB and confirm that MongoDB origin reads it using as offset the
    nested field.

    The pipeline looks like:
        mongodb_origin >> wiretap
    """
    if mongodb.atlas:
        pytest.skip("MongoDB stages don't support connect to MongoDB Atlas")

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_origin = pipeline_builder.add_stage('MongoDB', type='origin')
    mongodb_origin.set_attributes(capped_collection=False,
                                  database=get_random_string(ascii_letters, 5),
                                  collection=get_random_string(ascii_letters, 10),
                                  initial_offset='baa',
                                  offset_field_type='STRING',
                                  offset_field='data.data.foo')

    wiretap = pipeline_builder.add_wiretap()
    mongodb_origin >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        # MongoDB and PyMongo add '_id' to the dictionary entries e.g. docs_in_database
        # when used for inserting in collection. Hence the deep copy.
        docs_in_database = copy.deepcopy(NESTED_DOC)

        # Create document in MongoDB using PyMongo.
        # First a database is created. Then a collection is created inside that database.
        # Then document is created in that collection.
        logger.info('Adding document into %s collection using PyMongo...', mongodb_origin.collection)
        mongodb_database = mongodb.engine[mongodb_origin.database]
        mongodb_collection = mongodb_database[mongodb_origin.collection]
        insert_list = [mongodb_collection.insert_one(doc) for doc in docs_in_database]
        assert len(insert_list) == len(docs_in_database)

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(docs_in_database))
        sdc_executor.stop_pipeline(pipeline)

        assert [{'foo': record.field['data']['data']['foo'].value} for record in wiretap.output_records] == [
            {'foo': 'bar'}]
    finally:
        logger.info('Dropping %s database...', mongodb_origin.database)
        mongodb.engine.drop_database(mongodb_origin.database)


def mongodbLookupResultFieldName(sdc_builder):
    """Resolve proper name for the "Result Field" in lookup - it will differ based on SDC version."""
    if Version(sdc_builder.version) >= Version("3.7.0"):
        return 'result_field'
    else:
        return 'new_field_to_save_lookup_result'


def mongodbLookupMappingName(sdc_builder):
    """Resolve proper name for the "Document to SDC Field Mappings" in lookup - it will differ based on SDC version."""
    if Version(sdc_builder.version) >= Version("3.7.0"):
        return 'document_to_sdc_field_mappings'
    else:
        return 'sdc_field_to_document_field_mapping'


@mongodb('legacy')
@sdc_min_version('3.5.0')
def test_mongodb_lookup_processor_simple(sdc_builder, sdc_executor, mongodb):
    """
    Create 2 nested documents in MongoDB and confirm that MongoDB Lookup Processor can find the documents.

    The pipeline looks like:
        dev_raw_data_source >> MongoDB Lookup Processor >> wiretap
    """
    if mongodb.atlas:
        pytest.skip("MongoDB stages don't support connect to MongoDB Atlas")

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    lookup_data = ['name'] + [row['name'] for row in NESTED_DOCS]
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(lookup_data),
                                       stop_after_first_batch=True)

    mapping = [dict(keyName='name', sdcField='/name')]
    mongodb_lookup = pipeline_builder.add_stage('MongoDB Lookup', type='processor')
    mongodb_lookup.set_attributes(database=get_random_string(ascii_letters, 5),
                                  collection=get_random_string(ascii_letters, 10))
    setattr(mongodb_lookup, mongodbLookupResultFieldName(sdc_builder), '/result')
    setattr(mongodb_lookup, mongodbLookupMappingName(sdc_builder), mapping)

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> mongodb_lookup >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        # MongoDB and PyMongo add '_id' to the dictionary entries e.g. docs_in_database
        # when used for inserting in collection. Hence the deep copy.
        docs_in_database = copy.deepcopy(NESTED_DOCS)

        # Create documents in MongoDB using PyMongo.
        # First a database is created. Then a collection is created inside that database.
        # Then documents are created in that collection.
        logger.info('Adding documents into %s collection using PyMongo...', mongodb_lookup.collection)
        mongodb_database = mongodb.engine[mongodb_lookup.database]
        mongodb_collection = mongodb_database[mongodb_lookup.collection]

        insert_list = [mongodb_collection.insert_one(doc) for doc in docs_in_database]
        assert len(insert_list) == len(docs_in_database)

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        for record, actual in zip(wiretap.output_records, NESTED_DOCS):
            assert record.get_field_data('/result/location/city') == actual['location']['city']
            assert record.get_field_data('/result/location/state') == actual['location']['state']

    finally:
        logger.info('Dropping %s database...', mongodb_lookup.database)
        mongodb.engine.drop_database(mongodb_lookup.database)


@mongodb('legacy')
# SDC-11418
@sdc_min_version('3.5.0')
def test_mongodb_lookup_processor_implicit_port(sdc_builder, sdc_executor, mongodb):
    """
    Just set up the origin and processor; don't need any data in
    MongoDB.

    The pipeline looks like:
        dev_raw_data_source >> MongoDB Lookup Processor >> trash
    """
    if mongodb.atlas:
        pytest.skip("MongoDB stages don't support connect to MongoDB Atlas")

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    lookup_data = ['name'] + [row['name'] for row in NESTED_DOCS]
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(lookup_data),
                                       stop_after_first_batch=True)

    mapping = [dict(keyName='name', sdcField='/name')]
    mongodb_lookup = pipeline_builder.add_stage('MongoDB Lookup', type='processor')
    mongodb_lookup.set_attributes(database=get_random_string(ascii_letters, 5),
                                  collection=get_random_string(ascii_letters, 10))
    setattr(mongodb_lookup, mongodbLookupResultFieldName(sdc_builder), '/result')
    setattr(mongodb_lookup, mongodbLookupMappingName(sdc_builder), mapping)

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> mongodb_lookup >> trash
    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    # configure_for_environment will set the connection string, so we
    # need to override it here without the explicit port number
    connection_string = f'mongodb://{mongodb.hostname}/{mongodb.database}?{mongodb.options}'
    mongodb_lookup.set_attributes(connection_string=connection_string)

    try:
        # Start pipeline - no exception should be raised
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
        # Pipeline might be finished, since dev origin is set to stop
        # after first batch
        assert status in ['RUNNING', 'FINISHED']

    finally:
        logger.info('Dropping %s database...', mongodb_lookup.database)
        mongodb.engine.drop_database(mongodb_lookup.database)


@mongodb('legacy')
# SDC-11416
@sdc_min_version('3.5.0')
def test_mongodb_lookup_processor_invalid_url(sdc_builder, sdc_executor, mongodb):
    """
    Just set up the origin and processor; don't need any data in
    MongoDB.

    The pipeline looks like:
        dev_raw_data_source >> MongoDB Lookup Processor >> trash
    """
    if mongodb.atlas:
        pytest.skip("MongoDB stages don't support connect to MongoDB Atlas")

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    lookup_data = ['name'] + [row['name'] for row in NESTED_DOCS]
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(lookup_data),
                                       stop_after_first_batch=True)

    mapping = [dict(keyName='name', sdcField='/name')]
    mongodb_lookup = pipeline_builder.add_stage('MongoDB Lookup', type='processor')
    mongodb_lookup.set_attributes(database=get_random_string(ascii_letters, 5),
                                  collection=get_random_string(ascii_letters, 10))
    setattr(mongodb_lookup, mongodbLookupResultFieldName(sdc_builder), '/result')
    setattr(mongodb_lookup, mongodbLookupMappingName(sdc_builder), mapping)

    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> mongodb_lookup >> trash
    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    # configure_for_environment will set the connection string, so we
    # need to override it here with an invalid one
    mongodb_lookup.set_attributes(connection_string='mongodb://bogus-hostname:27101')

    try:
        # Start pipeline and verify that the exception is raised
        sdc_executor.add_pipeline(pipeline)
        with pytest.raises(StartError) as start_error:
            sdc_executor.start_pipeline(pipeline)
        assert 'MONGODB_09' in start_error.value.message

    finally:
        logger.info('Dropping %s database...', mongodb_lookup.database)
        mongodb.engine.drop_database(mongodb_lookup.database)


@mongodb('legacy')
@sdc_min_version('3.5.0')
def test_mongodb_lookup_processor_nested_lookup(sdc_builder, sdc_executor, mongodb):
    """
    Create 2 nested documents in MongoDB and confirm that MongoDB Lookup Processor can find the documents.

    The pipeline looks like:
        dev_raw_data_source >> MongoDB Lookup Processor >> wiretap
    """
    if mongodb.atlas:
        pytest.skip("MongoDB stages don't support connect to MongoDB Atlas")

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    lookup_data = ['name,state'] + ['{},{}'.format(row['name'], row['location']['state']) for row in NESTED_DOCS]
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(lookup_data),
                                       stop_after_first_batch=True)

    mapping = [dict(keyName='name', sdcField='/name'),
               dict(keyName='location.state', sdcField='/state')]

    mongodb_lookup = pipeline_builder.add_stage('MongoDB Lookup', type='processor')
    mongodb_lookup.set_attributes(database=get_random_string(ascii_letters, 5),
                                  collection=get_random_string(ascii_letters, 10))
    setattr(mongodb_lookup, mongodbLookupResultFieldName(sdc_builder), '/result')
    setattr(mongodb_lookup, mongodbLookupMappingName(sdc_builder), mapping)

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> mongodb_lookup >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        # MongoDB and PyMongo add '_id' to the dictionary entries e.g. docs_in_database
        # when used for inserting in collection. Hence the deep copy.
        docs_in_database = copy.deepcopy(NESTED_DOCS)

        # Create documents in MongoDB using PyMongo.
        # First a database is created. Then a collection is created inside that database.
        # Then documents are created in that collection.
        logger.info('Adding documents into %s collection using PyMongo...', mongodb_lookup.collection)
        mongodb_database = mongodb.engine[mongodb_lookup.database]
        mongodb_collection = mongodb_database[mongodb_lookup.collection]

        insert_list = [mongodb_collection.insert_one(doc) for doc in docs_in_database]
        assert len(insert_list) == len(docs_in_database)

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        for record, actual in zip(wiretap.output_records, NESTED_DOCS):
            assert record.get_field_data('/result/location/city') == actual['location']['city']

    finally:
        logger.info('Dropping %s database...', mongodb_lookup.database)
        mongodb.engine.drop_database(mongodb_lookup.database)


@mongodb('legacy')
def test_mongodb_destination(sdc_builder, sdc_executor, mongodb):
    """
    Send simple text into MongoDB destination from Dev Raw Data Source and
        confirm that MongoDB correctly received them using PyMongo.

    The pipeline looks like:
        dev_raw_data_source >> record_deduplicator >> expression_evaluator >> mongodb_dest
                               record_deduplicator >> trash
    """
    if mongodb.atlas:
        pytest.skip("MongoDB stages don't support connect to MongoDB Atlas")

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data='\n'.join(DATA))

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    # MongoDB destination uses the CRUD operation in the sdc.operation.type record header attribute when writing
    # to MongoDB. Value 4 specified below is for UPSERT.
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': '1'}]

    mongodb_dest = pipeline_builder.add_stage('MongoDB', type='destination')
    mongodb_dest.set_attributes(database=get_random_string(ascii_letters, 5),
                                collection=get_random_string(ascii_letters, 10))

    if Version(sdc_builder.version) >= Version('4.1.0'):
        mongodb_dest.set_attributes(improve_type_conversion=False)

    # From 3.6.0, unique key field is a list, otherwise single string for older version.
    mongodb_dest.unique_key_field = ['/text'] if Version(sdc_builder.version) >= Version('3.6.0') else '/text'

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    trash = pipeline_builder.add_stage('Trash')
    dev_raw_data_source >> record_deduplicator >> expression_evaluator >> mongodb_dest
    record_deduplicator >> trash
    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        # Data is generated in dev_raw_data_source and sent to MongoDB using pipeline.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(DATA))
        sdc_executor.stop_pipeline(pipeline)

        # Verify data is received correctly using PyMongo.
        # Similar to writing, while reading data, we specify MongoDB database and the collection inside it.
        logger.info('Verifying docs received with PyMongo...')
        assert [item['text'] for item in mongodb.engine[mongodb_dest.database][mongodb_dest.collection].find()] == DATA

    finally:
        logger.info('Dropping %s database...', mongodb_dest.database)
        mongodb.engine.drop_database(mongodb_dest.database)


@mongodb('legacy')
def test_mongodb_destination_update_on_nested_key(sdc_builder, sdc_executor, mongodb):
    """Ensure that an update on a document with a nested unique field is correctly executed"""
    if mongodb.atlas:
        pytest.skip("MongoDB stages don't support connect to MongoDB Atlas")

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')
    record = {"f1": {"f2": "a"}, "f3": "b"}

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=json.dumps(record))

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': '3'}]

    mongodb_dest = pipeline_builder.add_stage('MongoDB', type='destination')
    unique_key_field = '/f1/f2' if Version(sdc_builder.version) <= Version('3.5.0') else ['/f1/f2']
    mongodb_dest.set_attributes(database=get_random_string(ascii_letters, 5),
                                collection=get_random_string(ascii_letters, 10),
                                unique_key_field=unique_key_field)

    if Version(sdc_builder.version) >= Version('4.1.0'):
        mongodb_dest.set_attributes(improve_type_conversion=False)

    dev_raw_data_source >> expression_evaluator >> mongodb_dest

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        # Change value of field which will be updated
        record["f3"] = "c"

        # Create document in MongoDB using PyMongo.
        # First a database is created. Then a collection is created inside that database.
        # Then document is created in that collection.
        logger.info('Adding document into %s collection using PyMongo...', mongodb_dest.collection)
        mongodb_database = mongodb.engine[mongodb_dest.database]
        mongodb_collection = mongodb_database[mongodb_dest.collection]
        inserted_doc = mongodb_collection.insert_one(record)
        assert inserted_doc is not None

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(1)
        sdc_executor.stop_pipeline(pipeline)

        logger.info('Verifying docs updated with PyMongo...')
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_dest.database][mongodb_dest.collection].find()]
        assert len(mongodb_documents) == 1
        assert mongodb_documents[0]["f3"] == "b"
    finally:
        logger.info('Dropping %s database...', mongodb_dest.database)
        mongodb.engine.drop_database(mongodb_dest.database)


@mongodb('legacy')
@pytest.mark.parametrize('batch_size', [
    2,
    6,
    11
])
def test_mongodb_origin_duplicate_dates(sdc_builder, sdc_executor, mongodb, batch_size):
    """Ensure that an having duplicates values in the offset field don't force the no-more-data event and stops the
    pipeline before reading all the documents stored in MongoDB.
    The pipeline looks like:

        mongodb_origin >> wiretap.destination
        mongodb_origin >= finisher
    """
    collection_name = get_random_string(ascii_letters, 5)
    database_name = get_random_string(ascii_letters, 5)

    try:
        number_records = 500
        data_input = []
        for i in range(number_records):
            year = f'20{random.randint(0, 9)}{random.randint(1, 9)}'
            data_input.append(dict(pk=i,
                                   updatedOn=datetime.datetime(int(year), 3, 24, 13, 0),
                                   text='b' + get_random_string(string.ascii_letters, 4)))

        docs_in_database = copy.deepcopy(data_input)
        mongodb_database = mongodb.engine[database_name]
        mongodb_collection = mongodb_database[collection_name]
        insert_list = [mongodb_collection.insert_one(doc) for doc in docs_in_database]
        assert len(insert_list) == len(docs_in_database)

        pipeline_builder = sdc_builder.get_pipeline_builder()
        pipeline_builder.add_error_stage('Discard')

        mongodb_origin = pipeline_builder.add_stage('MongoDB', type='origin')
        mongodb_origin.set_attributes(capped_collection=False,
                                      database=database_name,
                                      collection=collection_name,
                                      offset_field_type='DATE',
                                      initial_offset='2000-01-01 00:00:00',
                                      offset_field='updatedOn',
                                      batch_size_in_records=batch_size)

        wiretap = pipeline_builder.add_wiretap()

        finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
        finisher.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

        mongodb_origin >> wiretap.destination
        mongodb_origin >= finisher

        pipeline = pipeline_builder.build(title=f'Test MongoDB Origin Duplicate Date - {batch_size} records') \
            .configure_for_environment(mongodb)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        assert len(records) == number_records
        output_records = [dict(pk=record.get_field_data('pk'),
                               updatedOn=record.get_field_data('updatedOn'),
                               text=record.get_field_data('text'))
                          for record in records]
        assert sorted(output_records, key=lambda d: (d['pk'])) == data_input

    finally:
        logger.info('Dropping %s database...', database_name)
        mongodb.engine.drop_database(database_name)
