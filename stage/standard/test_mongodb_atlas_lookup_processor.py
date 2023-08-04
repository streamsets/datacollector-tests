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
import json
import copy
import datetime
from string import ascii_letters
from decimal import Decimal
import bson
from bson.decimal128 import Decimal128

import pytest
from streamsets.testframework.markers import mongodb, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

MONGODB_ATLAS_DESTINATION = 'com_streamsets_pipeline_stage_destination_mongodb_atlas_MongoDBAtlasDTarget'
MONGODB_ATLAS_PROCESSOR = 'com_streamsets_pipeline_stage_processor_mongodb_atlas_MongoDBAtlasDProcessor'
pytestmark = [mongodb('atlas'), sdc_min_version('5.7.0')]


def _write_docs_in_database(mongodb, mongodb_atlas_lookup, data, database, collection):
    # Create documents in MongoDB using PyMongo.
    # First a database is created. Then a collection is created inside that database.
    # Then documents are created in that collection.
    logger.info('Adding documents into %s collection using PyMongo...', collection)
    mongodb_database = mongodb.engine[database]
    mongodb_collection = mongodb_database[collection]

    insert_list = [mongodb_collection.insert_one(doc) for doc in data]
    assert len(insert_list) == len(data)

    return mongodb_atlas_lookup


# https://www.mongodb.com/docs/manual/reference/bson-types/
DATA_TYPES = [
    # Value, Type
    (True, True, 'BOOLEAN'),  # Bool
    ('hello', 'hello', 'STRING'),  # String
    ('a', 'a', 'STRING'),  # Char
    # ('a', 'BYTE'),                                    # Not supported today
    (120, 120, 'INTEGER'),  # Integer
    (bson.Int64(120), bson.Int64(120), 'LONG'),  # Long
    (20.1, 20.1, 'DOUBLE'),  # Double
    (Decimal128('20.10'), Decimal('20.10'), 'DECIMAL'),  # Decimal
    (datetime.datetime(2020, 1, 1, 10, 0), datetime.datetime(2020, 1, 1, 10, 0), 'DATE'),  # DATE
    (datetime.datetime(2020, 1, 1, 10, 0, 0, 0, tzinfo=datetime.timezone.utc), datetime.datetime(2020, 1, 1, 10, 0),
     'DATE'),  # Zoned Datetime
    (b'string', b'string', 'BYTE_ARRAY'),  # Byte Array
    ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'.encode('ascii'), 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'.encode('ascii'),
     'BYTE_ARRAY')  # Byte Array
]


def test_data_types(sdc_builder, sdc_executor, mongodb):
    """
    Test all feasible MongoDB Atlas types.
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    database_name = get_random_string(ascii_letters, 5)
    collection_name = get_random_string(ascii_letters, 5)
    documents_to_insert = [{"id": str(i), "value": DATA_TYPES[i][0], "type": DATA_TYPES[i][2]}
                           for i in range(len(DATA_TYPES))]
    try:
        dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
        lookup_data = ['id'] + [row['id'] for row in documents_to_insert]
        dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                           header_line='WITH_HEADER',
                                           raw_data='\n'.join(lookup_data),
                                           stop_after_first_batch=True)

        mongodb_atlas_lookup = pipeline_builder.add_stage(name=MONGODB_ATLAS_PROCESSOR)
        column_mappings = [dict(keyName='id', sdcField='/id')]
        mongodb_atlas_lookup.set_attributes(database=database_name,
                                            collection=collection_name,
                                            result_field='/result',
                                            document_to_sdc_field_mappings=column_mappings)

        # Configure MongoDB Atlas origin to connect to old MongoDB version
        if not mongodb.atlas:
            mongodb_atlas_lookup.tls_mode = 'NONE'
            mongodb_atlas_lookup.authentication_method = 'NONE'

        wiretap = pipeline_builder.add_wiretap()

        dev_raw_data_source >> mongodb_atlas_lookup >> wiretap.destination

        pipeline = pipeline_builder.build(title=f'Test Standard MongoDB Atlas Lookup Data Types') \
            .configure_for_environment(mongodb)
        sdc_executor.add_pipeline(pipeline)

        # Create the collection and write documents into the database
        _write_docs_in_database(mongodb, mongodb_atlas_lookup, documents_to_insert, database_name, collection_name)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        assert len(records) == len(DATA_TYPES)

        for i in range(len(DATA_TYPES)):
            record = records[i]
            assert record.get_field_data('/result/value').value == DATA_TYPES[i][1]
            assert record.get_field_data('/result/value').type == DATA_TYPES[i][2]

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_lookup.database)
        mongodb.engine.drop_database(mongodb_atlas_lookup.database)


# Restrictions: https://www.mongodb.com/docs/upcoming/reference/limits/#std-label-restrictions-on-db-names
MONGODB_ATLAS_DATABASE_NAMES = [
    ('minsize', get_random_string(string.ascii_lowercase, 1)),
    ('maxsize', get_random_string(string.ascii_lowercase, 37)),
    ('lowercase', get_random_string(string.ascii_lowercase)),
    ('uppercase', get_random_string(string.ascii_uppercase)),
    ('digits', get_random_string(string.digits)),
    ('hexadecimal', get_random_string(string.hexdigits).lower())
]


@pytest.mark.parametrize('database_name_type,database_name', MONGODB_ATLAS_DATABASE_NAMES,
                         ids=[i[0] for i in MONGODB_ATLAS_DATABASE_NAMES])
def test_object_names_database(sdc_builder, sdc_executor, mongodb, database_name_type, database_name):
    database = database_name
    collection = get_random_string(ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')
    documents_to_insert = [{"id": '1', "value": 'Hello'}]

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    lookup_data = ['id'] + [row['id'] for row in documents_to_insert]
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(lookup_data),
                                       stop_after_first_batch=True)

    mongodb_atlas_lookup = pipeline_builder.add_stage(name=MONGODB_ATLAS_PROCESSOR)
    mongodb_atlas_lookup.set_attributes(database=database,
                                        collection=collection,
                                        result_field='/result',
                                        document_to_sdc_field_mappings=[dict(keyName='id', sdcField='/id')])

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_lookup.tls_mode = 'NONE'
        mongodb_atlas_lookup.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> mongodb_atlas_lookup >> wiretap.destination

    pipeline = pipeline_builder.build(title=f'Test Standard MongoDB Atlas Lookup Database Name[{database_name_type}]') \
        .configure_for_environment(mongodb)

    try:
        _write_docs_in_database(mongodb, mongodb_atlas_lookup, copy.deepcopy(documents_to_insert), database, collection)

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        record = wiretap.output_records[0]
        assert record.get_field_data('/result/id') == documents_to_insert[0]['id']
        assert record.get_field_data('/result/value') == documents_to_insert[0]['value']

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_lookup.database)
        mongodb.engine.drop_database(mongodb_atlas_lookup.database)


# Restrictions: https://www.mongodb.com/docs/upcoming/reference/limits/#std-label-restrictions-on-db-names
MONGODB_ATLAS_COLLECTION_NAMES = [
    ('minsize', lambda: get_random_string(string.ascii_lowercase, 1)),
    ('maxsize', lambda: get_random_string(string.ascii_lowercase, 37)),
    ('lowercase', lambda: get_random_string(string.ascii_lowercase)),
    ('uppercase', lambda: get_random_string(string.ascii_uppercase)),
    ('digits', lambda: get_random_string(string.digits)),
    ('hexadecimal', lambda: get_random_string(string.hexdigits).lower())
]


@pytest.mark.parametrize('collection_name_type,collection_name', MONGODB_ATLAS_DATABASE_NAMES,
                         ids=[i[0] for i in MONGODB_ATLAS_DATABASE_NAMES])
def test_object_names_collection(sdc_builder, sdc_executor, mongodb, collection_name_type, collection_name):
    database = get_random_string(ascii_letters, 10)
    collection = collection_name

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')
    documents_to_insert = [{"id": '1', "value": 'Hello'}]

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    lookup_data = ['id'] + [row['id'] for row in documents_to_insert]
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(lookup_data),
                                       stop_after_first_batch=True)

    mongodb_atlas_lookup = pipeline_builder.add_stage(name=MONGODB_ATLAS_PROCESSOR)
    mongodb_atlas_lookup.set_attributes(database=database,
                                        collection=collection,
                                        result_field='/result',
                                        document_to_sdc_field_mappings=[dict(keyName='id', sdcField='/id')])

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_lookup.tls_mode = 'NONE'
        mongodb_atlas_lookup.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> mongodb_atlas_lookup >> wiretap.destination

    pipeline = pipeline_builder.build(title=f'Test Standard MongoDB Atlas Lookup Collection Name[{collection_name_type}]') \
        .configure_for_environment(mongodb)

    try:
        _write_docs_in_database(mongodb, mongodb_atlas_lookup, copy.deepcopy(documents_to_insert), database, collection)

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        record = wiretap.output_records[0]
        assert record.get_field_data('/result/id') == documents_to_insert[0]['id']
        assert record.get_field_data('/result/value') == documents_to_insert[0]['value']

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_lookup.database)
        mongodb.engine.drop_database(mongodb_atlas_lookup.database)


def test_multiple_batches(sdc_builder, sdc_executor, mongodb):
    pytest.skip("MongoDB Atlas Lookup Processor doesn't use multiple batches")


def test_data_format(sdc_builder, sdc_executor, mongodb):
    pytest.skip("MongoDB Atlas Lookup Processor doesn't deal with data formats")


def test_dataflow_events(sdc_builder, sdc_executor, mongodb):
    pytest.skip("MongoDB Atlas Lookup processor does not support events today")


def test_multithread(sdc_builder, sdc_executor, mongodb):
    database = get_random_string(ascii_letters, 5)
    collection = get_random_string(ascii_letters, 5)
    number_of_records = 100

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')
    documents_to_insert = [{"id": i, "value": get_random_string(ascii_letters, 4)} for i in range(number_of_records)]

    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(number_of_threads=5,
                                      fields_to_generate=[{'field': 'id',
                                                           'type': 'LONG_SEQUENCE',
                                                           'precision': 10,
                                                           'scale': 2}],
                                      records_to_be_generated=number_of_records)

    mongodb_atlas_lookup = pipeline_builder.add_stage(name=MONGODB_ATLAS_PROCESSOR)
    mongodb_atlas_lookup.set_attributes(database=database,
                                        collection=collection,
                                        result_field='/result',
                                        document_to_sdc_field_mappings=[dict(keyName='id', sdcField='/id')])

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_lookup.tls_mode = 'NONE'
        mongodb_atlas_lookup.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    dev_data_generator >> mongodb_atlas_lookup >> wiretap.destination

    pipeline = pipeline_builder.build(title=f'Test Standard MongoDB Atlas Lookup Multithread') \
        .configure_for_environment(mongodb)

    try:
        _write_docs_in_database(mongodb, mongodb_atlas_lookup, copy.deepcopy(documents_to_insert), database, collection)

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        assert len(records) == number_of_records
        records.sort(key=lambda r: r.field['result']['id'])
        for record, actual in zip(records, documents_to_insert):
            assert record.get_field_data('/result/id') == actual['id']
            assert record.get_field_data('/result/value') == actual['value']

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_lookup.database)
        mongodb.engine.drop_database(mongodb_atlas_lookup.database)
