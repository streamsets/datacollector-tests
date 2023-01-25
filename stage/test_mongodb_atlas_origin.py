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
import copy
import logging
import time
import uuid
from string import ascii_letters

import pytest
from bson import binary, DBRef, decimal128
import datetime
import bson
from streamsets.testframework.markers import mongodb, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

MONGODB_ATLAS_ORIGIN = 'com_streamsets_pipeline_stage_origin_mongodb_atlas_MongoDBAtlasDSource'
MONGODB_ATLAS_DESTINATION = 'com_streamsets_pipeline_stage_destination_mongodb_atlas_MongoDBAtlasDTarget'
pytestmark = [mongodb, sdc_min_version('5.2.0')]

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


def _write_in_mongodb_atlas(mongodb, mongodb_atlas_origin, data):
    # MongoDB Atlas and PyMongo add '_id' to the dictionary entries e.g. docs_in_database
    # when used for inserting in collection. Hence the deep copy.
    docs_in_database = copy.deepcopy(data)

    # Create documents in MongoDB Atlas using PyMongo.
    # First a database is created. Then a collection is created inside that database.
    # Then documents are created in that collection.
    logger.info('Adding documents into %s collection using PyMongo...', mongodb_atlas_origin.collection)
    mongodb_database = mongodb.engine[mongodb_atlas_origin.database]
    mongodb_collection = mongodb_database[mongodb_atlas_origin.collection]
    if isinstance(docs_in_database, list):
        insert_list = [mongodb_collection.insert_one(doc) for doc in docs_in_database]
        assert len(insert_list) == len(docs_in_database)
    else:
        mongodb_collection.insert_one(docs_in_database)
        docs_in_database = data.values()

    return docs_in_database


def test_mongodb_atlas_origin(sdc_builder, sdc_executor, mongodb):
    """
    Create 3 simple documents in MongoDB Atlas and confirm that MongoDB Atlas origin reads them.

    The pipeline looks like:
        mongodb_atlas_origin >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_atlas_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_ORIGIN)
    mongodb_atlas_origin.set_attributes(database=get_random_string(ascii_letters, 5),
                                        collection=get_random_string(ascii_letters, 10))

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_origin.tls_mode = 'NONE'
        mongodb_atlas_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        # Write data in a MongoDB Atlas database
        docs_in_database = _write_in_mongodb_atlas(mongodb, mongodb_atlas_origin, ORIG_DOCS)

        # Start pipeline and verify the documents using snapshot.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(docs_in_database))
        sdc_executor.stop_pipeline(pipeline)

        assert ORIG_DOCS == [{'name': record.field['name'].value} for record in wiretap.output_records]
    finally:
        logger.info('Dropping %s database...', mongodb_atlas_origin.database)
        mongodb.engine.drop_database(mongodb_atlas_origin.database)


def test_mongodb_atlas_origin_DBRef_type(sdc_builder, sdc_executor, mongodb):
    """
    DBRef datatype is a reference to a document in other collection.
    Step 1. Create two collections(#1 and #2) in MongoDB Atlas and add sample documents in collection #1.
    Step 2. Add test documents to collection #2 which refer to the sample documents in collection #1.
    Step 3. Confirm that MongoDB origin reads the test documents from collection #2

    The pipeline looks like:
        mongodb_atlas_origin >> wiretap
    """
    database_name = get_random_string(ascii_letters, 5)
    collection1 = get_random_string(ascii_letters, 10)
    collection2 = get_random_string(ascii_letters, 10)
    logger.debug('database_name: %s, collection1: %s, collection2: %s', database_name, collection1, collection2)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_atlas_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_ORIGIN)
    mongodb_atlas_origin.set_attributes(database=database_name,
                                        collection=collection2)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_origin.tls_mode = 'NONE'
        mongodb_atlas_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_origin >> wiretap.destination

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
            assert record.get_field_data('/test_ref').get('collection') == collection1
            assert record.get_field_data('/test_ref').get('id') == str(expected['_id'])

    finally:
        logger.info('Dropping %s database...', database_name)
        mongodb.engine.drop_database(mongodb_atlas_origin.database)


def test_mongodb_atlas_origin_simple_with_BSONBinary(sdc_builder, sdc_executor, mongodb):
    """
    Create 3 simple documents consists with BSON Binary data type in MongoDB Atlas and confirm that MongoDB Atlas origin
    reads them.

    The pipeline looks like:
        mongodb_atlas_origin >> wiretap
    """

    ORIG_BINARY_DOCS = [
        {'data': binary.Binary(b'Binary Data Flute')},
        {'data': binary.Binary(b'Binary Data Oboe')},
        {'data': binary.Binary(b'Binary Data Violin')}
    ]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_atlas_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_ORIGIN)
    mongodb_atlas_origin.set_attributes(database=get_random_string(ascii_letters, 5),
                                        collection=get_random_string(ascii_letters, 10))

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_origin.tls_mode = 'NONE'
        mongodb_atlas_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        # Write data in a MongoDB Atlas database
        docs_in_database = _write_in_mongodb_atlas(mongodb, mongodb_atlas_origin, ORIG_BINARY_DOCS)

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(docs_in_database))
        sdc_executor.stop_pipeline(pipeline)

        assert [{'data': str(record.field['data'])} for record in wiretap.output_records] == [
            {'data': str(record.get('data'))} for record in ORIG_BINARY_DOCS]
    finally:
        logger.info('Dropping %s database...', mongodb_atlas_origin.database)
        mongodb.engine.drop_database(mongodb_atlas_origin.database)


def test_mongodb_atlas_origin_simple_with_decimal(sdc_builder, sdc_executor, mongodb):
    """
    Validate that we properly process decimal type.

    The pipeline looks like:
        mongodb_atlas_origin >> wiretap
    """
    ORIG_BINARY_DOCS = [{'data': decimal128.Decimal128("0.5")}]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_atlas_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_ORIGIN)
    mongodb_atlas_origin.set_attributes(database=get_random_string(ascii_letters, 5),
                                        collection=get_random_string(ascii_letters, 10))

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_origin.tls_mode = 'NONE'
        mongodb_atlas_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        # Write data in a MongoDB Atlas database
        docs_in_database = _write_in_mongodb_atlas(mongodb, mongodb_atlas_origin, ORIG_BINARY_DOCS)

        # Verify that insert was in-fact successful
        assert docs_in_database == [item
                                    for item in
                                    mongodb.engine[mongodb_atlas_origin.database][
                                        mongodb_atlas_origin.collection].find()]

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(docs_in_database))
        sdc_executor.stop_pipeline(pipeline)

        assert [{'data': decimal128.Decimal128(str(record.field['data']))} for record in
                wiretap.output_records] == ORIG_BINARY_DOCS
    finally:
        logger.info('Dropping %s database...', mongodb_atlas_origin.database)
        mongodb.engine.drop_database(mongodb_atlas_origin.database)


@pytest.mark.parametrize('nested_notation_offset', ['/data/data/foo', 'data.data.foo'])
def test_mongodb_atlas_origin_nested_field_offset(sdc_builder, sdc_executor, mongodb, nested_notation_offset):
    """
    Create 1 simple document with nested fields in MongoDB Atlas and confirm that MongoDB Atlas origin reads them using
    as offset the nested field.

    The pipeline looks like:
        mongodb_atlas_origin >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_atlas_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_ORIGIN)
    mongodb_atlas_origin.set_attributes(database=get_random_string(ascii_letters, 5),
                                        collection=get_random_string(ascii_letters, 10),
                                        offset_field=nested_notation_offset,
                                        initial_offset='baa')

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_origin.tls_mode = 'NONE'
        mongodb_atlas_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        # Write data in a MongoDB Atlas database
        docs_in_database = _write_in_mongodb_atlas(mongodb, mongodb_atlas_origin, NESTED_DOC)

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(docs_in_database))
        sdc_executor.stop_pipeline(pipeline)

        assert [{'foo': record.field['data']['data']['foo'].value} for record in wiretap.output_records] == [
            {'foo': 'bar'}]
    finally:
        logger.info('Dropping %s database...', mongodb_atlas_origin.database)
        mongodb.engine.drop_database(mongodb_atlas_origin.database)


# https://www.mongodb.com/docs/manual/reference/bson-types/
BSON_TYPES_DATA = [
    (
        'Code',
        bson.code.Code('<!DOCTYPE html> \n <html> \n <body> \n\n <h1>Example JavaScript</h1>\n </html> \n </body>'),
        '<!DOCTYPE html> \n <html> \n <body> \n\n <h1>Example JavaScript</h1>\n </html> \n </body>', False),
    ('CodeWithScope',
     bson.code.Code('<!DOCTYPE html> \n <html> \n <body> \n\n <h1>Example JavaScript</h1>\n </html> \n </body>',
                    scope={'Example': 'example'}),
     '<!DOCTYPE html> \n <html> \n <body> \n\n <h1>Example JavaScript</h1>\n </html> \n </body>', False),
    ('Binary', bson.binary.Binary(b'hello'), b'hello', False),
    ('ObjectId', bson.objectid.ObjectId('626bccb9697a12204fb22ea3'), '626bccb9697a12204fb22ea3', False),
    ('Timestamp', bson.timestamp.Timestamp(641912491, 1), datetime.datetime(1990, 5, 5, 13, 1, 31), False),
    ('DBRef', bson.dbref.DBRef(get_random_string(ascii_letters, 5), bson.objectid.ObjectId('626bccb9697a12204fb22ea3')),
     '626bccb9697a12204fb22ea3', False),
    ('Boolean', True, True, False),  # No write as BSON type
    ('String', 'Hello', 'Hello', False),  # No write as BSON type
    ('Int64', {'$numberLong': '87696512964783'}, '87696512964783', True),
    ('Int32', {'$numberInt': '87696512964783'}, '87696512964783', True),
    ('Decimal128', {"$numberDecimal": "9823.1297"}, '9823.1297', True),
    ('Date', {"$date": "1641954803067"}, '1641954803067', True),
    ('Array', ['Hello', True, 12345], ['Hello', True, 12345], False),
    ('Double', {"$numberDouble": "10.5"}, '10.5', True),
    ('MaxKey', {"$maxKey": 1}, 1, True),
    ('MinKey', {"$minKey": 1}, 1, True),
    ('RegularExpression', {"$regularExpression": {"pattern": "^H", "options": "i"}}, {"pattern": "^H", "options": "i"},
     True)
]


@pytest.mark.parametrize('bson_type,input_data,expected_data,json_format', BSON_TYPES_DATA,
                         ids=[i[0] for i in BSON_TYPES_DATA])
def test_mongodb_atlas_origin_bson_types(sdc_builder, sdc_executor, mongodb,
                                         bson_type, input_data, expected_data, json_format):
    """
    Create 1 simple document with BSON data type in MongoDB Atlas and confirm that MongoDB Atlas origin reads them.

    The pipeline looks like:
        mongodb_atlas_origin >> wiretap
    """
    database = get_random_string(ascii_letters, 5)
    collection = get_random_string(ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_atlas_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_ORIGIN)
    mongodb_atlas_origin.set_attributes(database=database,
                                        collection=collection)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_origin.tls_mode = 'NONE'
        mongodb_atlas_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        # Write data in a MongoDB Atlas database
        docs_in_database = _write_in_mongodb_atlas(mongodb, mongodb_atlas_origin, {'data': input_data})

        # Start pipeline and verify the documents using snapshot.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(docs_in_database))
        sdc_executor.stop_pipeline(pipeline)

        if json_format:
            data_type_list = list(input_data.keys())
            assert expected_data == wiretap.output_records[0].field['data'].get('\"' + data_type_list[0] + '\"')
        else:
            if bson_type == 'DBRef':
                assert expected_data == wiretap.output_records[0].field['data'].get('id')
            elif bson_type == 'Array':
                assert expected_data == wiretap.output_records[0].field['data']
            else:
                assert expected_data == wiretap.output_records[0].field['data'].value
    finally:
        logger.info('Dropping %s database...', mongodb_atlas_origin.database)
        mongodb.engine.drop_database(mongodb_atlas_origin.database)


def test_mongodb_atlas_origin_auto_flatten(sdc_builder, sdc_executor, mongodb):
    """
    Create 1 simple document with a entries with non-flatten key items in MongoDB Atlas and confirm that MongoDB Atlas
    origin reads them with flatten structure.

    The pipeline looks like:
        mongodb_atlas_origin >> wiretap
    """
    database = get_random_string(ascii_letters, 5)
    collection = get_random_string(ascii_letters, 10)
    input_data = [{'data1': {'id': 1, 'value': 'Red'}},
                  {'data2': {'id': 2, 'value': 'Blue'}}]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_atlas_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_ORIGIN)
    mongodb_atlas_origin.set_attributes(database=database,
                                        collection=collection,
                                        auto_flatten_nested_structures=True)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_origin.tls_mode = 'NONE'
        mongodb_atlas_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        # Write data in a MongoDB Atlas database
        [_write_in_mongodb_atlas(mongodb, mongodb_atlas_origin, data) for data in input_data]

        # Start pipeline and verify the documents using snapshot.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(input_data))
        sdc_executor.stop_pipeline(pipeline)

        records = wiretap.output_records
        assert len(records) == len(input_data)
        for i in range(len(records)):
            record = records[i]
            for key, value in record.field.items():
                if key != '_id':
                    # Since the input data is non-flattened, the keys have to be converted to JSON
                    key_split = key.split('.')
                    assert input_data[i].get(key_split[0]).get(key_split[1]) == value
    finally:
        logger.info('Dropping %s database...', mongodb_atlas_origin.database)
        mongodb.engine.drop_database(mongodb_atlas_origin.database)


CUSTOM_QUERIES = [
    ('all', '{}', 5),
    ('equal', '{\'status\': \'D\'}', 2),
    ('in', '{ status: {$in:["A", "D"] }}', 5),
    ('and', '{ status: "A", qty: { $lt: 30 } }', 1),
    ('or', '{ $or: [ { status: "A" }, { qty: { $lt: 30 } } ] }', 3),
    ('embedded', '{ "size.uom": "in" }', 2)
]


@pytest.mark.parametrize('type_query,custom_filter,number_items', CUSTOM_QUERIES, ids=[i[0] for i in CUSTOM_QUERIES])
def test_mongodb_atlas_origin_custom_query(sdc_builder, sdc_executor, mongodb, type_query, custom_filter, number_items):
    """
    Create 1 simple document with 5 items in MongoDB Atlas and apply custom filters and confirm that MongoDB Atlas
    origin reads the items matching the filters.

    The pipeline looks like:
        mongodb_atlas_origin >> wiretap
    """
    database = get_random_string(ascii_letters, 5)
    collection = get_random_string(ascii_letters, 10)
    raw_data = [
        {'item': 'journal', 'qty': 25, 'size': {'h': 14, 'w': 21, 'uom': 'cm'}, 'status': 'A'},
        {'item': 'notebook', 'qty': 50, 'size': {'h': 8.5, 'w': 11, 'uom': 'in'}, 'status': 'A'},
        {'item': 'paper', 'qty': 100, 'size': {'h': 8.5, 'w': 11, 'uom': 'in'}, 'status': 'D'},
        {'item': 'planner', 'qty': 75, 'size': {'h': 22.85, 'w': 30, 'uom': 'cm'}, 'status': 'D'},
        {'item': 'postcard', 'qty': 45, 'size': {'h': 10, 'w': 15.25, 'uom': 'cm'}, 'status': 'A'}
    ]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_atlas_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_ORIGIN)
    mongodb_atlas_origin.set_attributes(database=database,
                                        collection=collection,
                                        custom_filter=custom_filter)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_origin.tls_mode = 'NONE'
        mongodb_atlas_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        # Write data in a MongoDB Atlas database
        _write_in_mongodb_atlas(mongodb, mongodb_atlas_origin, raw_data)

        # Start pipeline and verify the documents using snapshot.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', number_items)
        sdc_executor.stop_pipeline(pipeline)

        records = wiretap.output_records
        assert len(records) == number_items
        for record in records:
            data = record.field
            record_data = {'item': data.get('item'),
                           'qty': data.get('qty'),
                           'size':
                               {'h': data.get('size').get('h'),
                                'w': data.get('size').get('w'),
                                'uom': data.get('size').get('uom')
                                },
                           'status': data.get('status')}
            assert record_data in raw_data

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_origin.database)
        mongodb.engine.drop_database(mongodb_atlas_origin.database)


BSON_TYPES_OFFSET = [
    ('String', ({'item': 1, 'offset': 'a'}, {'item': 2, 'offset': 'b'})),
    ('ObjectId', ({'item': 1, 'offset': '2015-01-01 00:00:00'},
                  {'item': 2, 'offset': '2016-01-01 00:00:00'}))
]


@pytest.mark.parametrize('bson_type,input_data', BSON_TYPES_OFFSET, ids=[i[0] for i in BSON_TYPES_OFFSET])
def test_mongodb_atlas_origin_bson_offset(sdc_builder, sdc_executor, mongodb, bson_type, input_data):
    """
    Create 1 simple document in MongoDB Atlas and start to read from offset with different BSON type and confirm that
    MongoDB Atlas origin reads only 1 item.

    The pipeline looks like:
        mongodb_atlas_origin >> wiretap
    """
    database = get_random_string(ascii_letters, 5)
    collection = get_random_string(ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_atlas_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_ORIGIN)
    mongodb_atlas_origin.set_attributes(database=database,
                                        collection=collection,
                                        offset_field='offset',
                                        initial_offset=input_data[0].get('offset'))

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_origin.tls_mode = 'NONE'
        mongodb_atlas_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        # Write data in a MongoDB Atlas database
        [_write_in_mongodb_atlas(mongodb, mongodb_atlas_origin, data) for data in input_data]

        # Start pipeline and verify the documents using snapshot.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
        sdc_executor.stop_pipeline(pipeline)

        records = wiretap.output_records
        assert len(records) == 1
        assert records[0].field.get('item') == 2

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_origin.database)
        mongodb.engine.drop_database(mongodb_atlas_origin.database)


@pytest.mark.parametrize('capped_cursor', [
    'NONTAILABLE',
    'TAILABLE',
    'TAILABLE_AWAIT'
])
def test_mongodb_atlas_origin_capped_cursor_types(sdc_builder, sdc_executor, mongodb, capped_cursor):
    """
    Create 1 simple document in MongoDB Atlas and confirm that MongoDB Atlas origin reads them using different type
    of capped cursor. For Tailable cursor, it reads from a capped collection.

    The pipeline looks like:
        mongodb_atlas_origin >> wiretap
    """
    database = get_random_string(ascii_letters, 5)
    collection = get_random_string(ascii_letters, 10)

    data = [
        {'name': 'Shepard'},
        {'name': 'Kaidan'},
        {'name': 'Garrus'}
    ]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_atlas_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_ORIGIN)
    mongodb_atlas_origin.set_attributes(database=database,
                                        collection=collection,
                                        capped_collection_cursor_type=capped_cursor)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_origin.tls_mode = 'NONE'
        mongodb_atlas_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        # Create documents in MongoDB Atlas using PyMongo.
        # First a database is created. Then a collection (capped in case the Tailable Cursor, and non capped for Non
        # Tailable Cursor is created inside that database.
        # Then documents are created in that collection.
        logger.info('Adding documents into %s collection using PyMongo...', mongodb_atlas_origin.collection)
        mongodb_database = mongodb.engine[database]
        if capped_cursor.startswith('TAILABLE'):
            mongodb_collection = mongodb_database.create_collection(collection, capped=True, size=1000, max=3)
        else:
            mongodb_collection = mongodb_database.create_collection(collection, capped=False)

        for document in data:
            document_to_insert = copy.deepcopy(document)
            mongodb_collection.insert_one(document_to_insert)

        # Start pipeline and verify the documents using snapshot.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(data))
        sdc_executor.stop_pipeline(pipeline, force=True)

        assert data == [{'name': record.field['name'].value} for record in wiretap.output_records]

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_origin.database)
        mongodb.engine.drop_database(mongodb_atlas_origin.database)


@pytest.mark.parametrize('capped_cursor', [
    'NONTAILABLE',
    'TAILABLE'
])
def test_mongodb_atlas_origin_max_batch_time(sdc_builder, sdc_executor, mongodb, capped_cursor):
    """
    Create 1 simple document in MongoDB Atlas with 60 seconds as max batch wait time and confirm that MongoDB Atlas
    origin with Tailable cursor takes +-5 seconds to read them around the max batch wait time.

    The pipeline looks like:
        mongodb_atlas_origin >> wiretap
    """
    database = get_random_string(ascii_letters, 5)
    collection = get_random_string(ascii_letters, 10)
    max_wait_time = 30

    data = [{'name': 'Shepard'},
            {'name': 'Kaidan'},
            {'name': 'Garrus'}]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_atlas_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_ORIGIN)
    mongodb_atlas_origin.set_attributes(database=database,
                                        collection=collection,
                                        batch_size_in_records=100,
                                        max_batch_wait_time_in_sec='${' + str(max_wait_time) + ' * SECONDS}')

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_origin.tls_mode = 'NONE'
        mongodb_atlas_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        # Write data in a MongoDB Atlas database
        logger.info('Adding documents into %s collection using PyMongo...', mongodb_atlas_origin.collection)
        mongodb_database = mongodb.engine[database]
        if capped_cursor.startswith('TAILABLE'):
            mongodb_collection = mongodb_database.create_collection(collection, capped=True, size=1000, max=3)
        else:
            mongodb_collection = mongodb_database.create_collection(collection, capped=False)
        for document in data:
            document_to_insert = copy.deepcopy(document)
            mongodb_collection.insert_one(document_to_insert)

        # Start pipeline and verify the documents using snapshot.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        start = time.time()
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(data), timeout_sec=120)
        end = time.time()
        sdc_executor.stop_pipeline(pipeline)

        # Execution time is around +-5 seconds around the max batch wait time, which is what we expect it to wait
        total_time = (end - start)
        if capped_cursor.startswith('TAILABLE'):
            assert (max_wait_time + 5) > total_time > (max_wait_time - 5)
        else:
            # Since it's a normal collection (Non Tailable Cursor), the origin didn't depend of the max batch wait
            # time and take less than few seconds on read the documents from MongoDB
            assert total_time < 5

        records = wiretap.output_records
        assert [data == {'name': record.field['name'].value} for record in records]

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_origin.database)
        mongodb.engine.drop_database(mongodb_atlas_origin.database)


def test_mongodb_atlas_origin_multiple_documents(sdc_builder, sdc_executor, mongodb):
    """
    Create 1 simple document in MongoDB Atlas and start to read from offset with different BSON type and confirm that
    MongoDB Atlas origin reads only 1 item.

    The pipeline looks like:
        mongodb_atlas_origin >> wiretap
    """
    database = get_random_string(ascii_letters, 5)
    collection = get_random_string(ascii_letters, 10)

    input_data = [{'item': i, 'value': get_random_string(ascii_letters, 4)} for i in range(1, 11)]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_atlas_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_ORIGIN)
    mongodb_atlas_origin.set_attributes(database=database,
                                        collection=collection)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_origin.tls_mode = 'NONE'
        mongodb_atlas_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        # Write data in a MongoDB Atlas database
        [_write_in_mongodb_atlas(mongodb, mongodb_atlas_origin, data) for data in input_data]

        # Start pipeline and verify the documents using snapshot.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 10)
        sdc_executor.stop_pipeline(pipeline)

        records = sorted([record.field["item"] for record in wiretap.output_records])
        assert len(records) == 10
        assert [i for i in range(1,11)] == records

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_origin.database)
        mongodb.engine.drop_database(mongodb_atlas_origin.database)


@pytest.mark.parametrize('database_value,collection_value', [
    ('${PARAM}', get_random_string(ascii_letters, 5)),
    (get_random_string(ascii_letters, 5), '${PARAM}')
])
def test_mongodb_atlas_origin_database_collection_parameters(sdc_builder, sdc_executor, mongodb,
                                                             database_value, collection_value):
    """
    Create 1 simple document in MongoDB Atlas with pipeline parameters in the Database and Collection name.

    The pipeline looks like:
        mongodb_atlas_origin >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_atlas_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_ORIGIN)
    mongodb_atlas_origin.set_attributes(database=database_value, collection=collection_value)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_origin.tls_mode = 'NONE'
        mongodb_atlas_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    if database_value == '${PARAM}':
        database_value = get_random_string(ascii_letters, 5)
        pipeline.add_parameters(PARAM=database_value)
    else:
        collection_value = get_random_string(ascii_letters, 5)
        pipeline.add_parameters(PARAM=collection_value)

    try:
        # Write data in a MongoDB Atlas database
        # MongoDB Atlas and PyMongo add '_id' to the dictionary entries e.g. docs_in_database
        # when used for inserting in collection. Hence the deep copy.
        docs_in_database = copy.deepcopy(ORIG_DOCS)

        # Create documents in MongoDB Atlas using PyMongo.
        # First a database is created. Then a collection is created inside that database.
        # Then documents are created in that collection.
        logger.info('Adding documents into %s collection using PyMongo...', collection_value)
        mongodb_database = mongodb.engine[database_value]
        mongodb_collection = mongodb_database[collection_value]
        insert_list = [mongodb_collection.insert_one(doc) for doc in docs_in_database]
        assert len(insert_list) == len(docs_in_database)

        # Start pipeline and verify the documents using snapshot.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(ORIG_DOCS))
        sdc_executor.stop_pipeline(pipeline)

        assert ORIG_DOCS == [{'name': record.field['name'].value} for record in wiretap.output_records]
    finally:
        logger.info('Dropping %s database...', database_value)
        mongodb.engine.drop_database(database_value)


@sdc_min_version('5.4.0')
@pytest.mark.parametrize('use_capped_collection', [True, False])
def test_mongodb_atlas_origin_with_validator_schema(sdc_builder, sdc_executor, mongodb, use_capped_collection):
    """
    Create a document in MongoDB Atlas with a validator schema and confirm that MongoDB Atlas can correctly read it when
    using capped and uncapped collections.

    The pipeline looks like:
        mongodb_atlas_origin >> wiretap
    """
    database = get_random_string(ascii_letters, 5)
    collection = get_random_string(ascii_letters, 10)

    data = [{'name': 'Shepard'},
            {'name': 'Kaidan'},
            {'name': 'Garrus'}]

    validator = {
        '$jsonSchema': {
            'bsonType': 'object',
            "properties": {
                "name": {"bsonType": "string"}
            },
        }
    }

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_atlas_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_ORIGIN)
    mongodb_atlas_origin.set_attributes(
        database=database,
        collection=collection,
        batch_size_in_records=100
    )

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_origin.tls_mode = 'NONE'
        mongodb_atlas_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        # Write data in a MongoDB Atlas database
        logger.info('Adding documents into %s collection using PyMongo...', mongodb_atlas_origin.collection)
        mongodb_database = mongodb.engine[database]
        if use_capped_collection:
            mongodb_collection = mongodb_database.create_collection(
                collection,
                capped=True,
                size=1000,
                max=3,
                validator=validator
            )
        else:
            mongodb_collection = mongodb_database.create_collection(
                collection,
                capped=False,
                validator=validator
            )

        for document in data:
            document_to_insert = copy.deepcopy(document)
            mongodb_collection.insert_one(document_to_insert)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(data), timeout_sec=120)
        sdc_executor.stop_pipeline(pipeline)

        records = wiretap.output_records
        assert [data == {'name': record.field['name'].value} for record in records]

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_origin.database)
        mongodb.engine.drop_database(mongodb_atlas_origin.database)


@mongodb
@sdc_min_version('5.4.0')
@pytest.mark.parametrize('uuid_mode', [
    'STANDARD',
    'C_SHARP_LEGACY',
    'JAVA_LEGACY',
    'PYTHON_LEGACY',
    'UNSPECIFIED'
])
def test_mongodb_atlas_origin_uuid_modes(sdc_builder, sdc_executor, mongodb, uuid_mode):
    """
    Test MongoDB Atlas Origin is able to read UUIDs in older formats using different UUID modes.
    """
    database = get_random_string(ascii_letters, 5)
    collection = get_random_string(ascii_letters, 5)

    data = [{'_id': str(uuid.uuid1()), 'uuid': str(uuid.uuid4())}]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_atlas_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_ORIGIN)
    mongodb_atlas_origin.set_attributes(database=database,
                                        collection=collection,
                                        uuid_interpretation_mode=uuid_mode)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_origin.tls_mode = 'NONE'
        mongodb_atlas_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()
    mongodb_atlas_origin >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        # MongoDB Atlas and PyMongo add '_id' to the dictionary entries e.g. docs_in_database
        # when used for inserting in collection. Hence the deep copy.
        docs_in_database = copy.deepcopy(data)

        # Create documents in MongoDB Atlas using PyMongo.
        # First a database is created. Then a collection is created inside that database.
        # Then documents are created in that collection.
        logger.info('Adding documents into %s collection using PyMongo...', mongodb_atlas_origin.collection)
        mongodb_database = mongodb.engine[mongodb_atlas_origin.database]
        mongodb_collection = mongodb_database[mongodb_atlas_origin.collection]
        insert_list = [mongodb_collection.insert_one(doc) for doc in docs_in_database]
        assert len(insert_list) == len(docs_in_database)

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(docs_in_database))
        sdc_executor.stop_pipeline(pipeline)

        assert data == [{'_id': record.field['_id'].value, 'uuid': record.field['uuid'].value}
                        for record in wiretap.output_records]

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_origin.database)
        mongodb.engine.drop_database(mongodb_atlas_origin.database)


@mongodb
@sdc_min_version('5.4.0')
@pytest.mark.parametrize('origin_uuid_mode, destination_uuid_mode', [
    ('STANDARD', 'JAVA_LEGACY'),
    ('JAVA_LEGACY', 'STANDARD')
])
def test_mongodb_atlas_origin_read_write_uuid(sdc_builder, sdc_executor, mongodb,
                                              origin_uuid_mode, destination_uuid_mode):
    """
    Test MongoDB Atlas Origin is able to write/read UUIDs between STANDARD and JAVA_LEGACY UUID mode.
    """
    database = get_random_string(ascii_letters, 5)
    collection = get_random_string(ascii_letters, 5)

    data = [{'_id': str(uuid.uuid1()), 'uuid': str(uuid.uuid4())}]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_atlas_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_ORIGIN)
    mongodb_atlas_origin.set_attributes(database=database,
                                        collection=collection,
                                        uuid_interpretation_mode=origin_uuid_mode)

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=database,
                                             collection=collection,
                                             uuid_interpretation_mode=destination_uuid_mode)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_origin.tls_mode = 'NONE'
        mongodb_atlas_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()
    mongodb_atlas_origin >> mongodb_atlas_destination
    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        # MongoDB Atlas and PyMongo add '_id' to the dictionary entries e.g. docs_in_database
        # when used for inserting in collection. Hence the deep copy.
        docs_in_database = copy.deepcopy(data)

        # Create documents in MongoDB Atlas using PyMongo.
        # First a database is created. Then a collection is created inside that database.
        # Then documents are created in that collection.
        logger.info('Adding documents into %s collection using PyMongo...', mongodb_atlas_origin.collection)
        mongodb_database = mongodb.engine[mongodb_atlas_origin.database]
        mongodb_collection = mongodb_database[mongodb_atlas_origin.collection]
        insert_list = [mongodb_collection.insert_one(doc) for doc in docs_in_database]
        assert len(insert_list) == len(docs_in_database)

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(docs_in_database))
        sdc_executor.stop_pipeline(pipeline)

        # Read from MongoDB Atlas to assert
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][
            mongodb_atlas_destination.collection].find()]
        assert len(mongodb_documents) == 1
        assert mongodb_documents[0]['_id'] == data[0]['_id']
        assert mongodb_documents[0]['uuid'] == data[0]['uuid']

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_origin.database)
        mongodb.engine.drop_database(mongodb_atlas_origin.database)
