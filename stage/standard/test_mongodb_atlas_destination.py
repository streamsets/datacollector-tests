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

import datetime
import json
import logging
import string
from bson.decimal128 import Decimal128

import pytest
from streamsets.testframework.markers import mongodb, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

MONGODB_ATLAS_DESTINATION = 'com_streamsets_pipeline_stage_destination_mongodb_atlas_MongoDBAtlasDTarget'
pytestmark = [mongodb, sdc_min_version('5.2.0')]

# BSON types: https://www.mongodb.com/docs/manual/reference/bson-types/
DATA_TYPES = [
    ('true', 'BOOLEAN', 'bool', True),
    ('a', 'CHAR', 'string', 'a'),
    # ('a', 'BYTE', None),  # Not supported today
    (120, 'SHORT', 'int', 120),
    (120, 'INTEGER', 'int', 120),
    (120, 'LONG', 'long', 120),
    (20.1, 'FLOAT', 'double', 20.100000381469727),
    (20.1, 'DOUBLE', 'double', 20.1),
    (20.1, 'DECIMAL', 'decimal', Decimal128('20.10')),
    ('2020-01-01 10:00:00', 'DATE', 'date', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'TIME', 'date', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATETIME', 'date', datetime.datetime(2020, 1, 1, 10, 0)),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'date', datetime.datetime(2020, 1, 1, 10, 0)),
    ('hello', 'STRING', 'string', 'hello'),
    ('string', 'BYTE_ARRAY', 'array', b'string'),
    ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'BYTE_ARRAY', 'uuid', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'.encode('ascii'))
]


@mongodb
@pytest.mark.parametrize('input,converter_type,bson_type,expected', DATA_TYPES,
                         ids=[f'{i[1]}_{i[0]}' for i in DATA_TYPES])
def test_data_types(sdc_builder, sdc_executor, mongodb, input, converter_type, bson_type, expected):
    """
    Test all feasible data types MongoDB Atlas can write
    """
    database = get_random_string(string.ascii_letters, 5)
    collection = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    origin = pipeline_builder.add_stage('Dev Raw Data Source')
    origin.set_attributes(data_format='JSON',
                          stop_after_first_batch=True,
                          raw_data=json.dumps({"value": input}))

    converter = pipeline_builder.add_stage('Field Type Converter')
    converter.set_attributes(conversion_method='BY_FIELD',
                             field_type_converter_configs=[{
                                 'fields': ['/value'],
                                 'targetType': converter_type,
                                 'dataLocale': 'en,US',
                                 'dateFormat': 'YYYY_MM_DD_HH_MM_SS',
                                 'zonedDateTimeFormat': 'ISO_OFFSET_DATE_TIME',
                                 'scale': 2
                             }])

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': '1'},
                                                         {'attributeToSet': 'bson.type',
                                                          'headerAttributeExpression': bson_type}]

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=database,
                                             collection=collection)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_destination.tls_mode = 'NONE'
        mongodb_atlas_destination.authentication_method = 'NONE'

    origin >> converter >> expression_evaluator >> mongodb_atlas_destination

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)
    pipeline.configuration["shouldRetry"] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Run pipeline and read from MongoDB Atlas to assert
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_atlas_destination.database][
            mongodb_atlas_destination.collection].find()]

        assert len(mongodb_documents) == 1
        doc = mongodb_documents[0]

        assert doc['value'] == expected
    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(mongodb_atlas_destination.database)


DATA = ['To be or not to be.',
        'Excellence is not an act, it is a habit.',
        'No pains, no gains.']

# Reference https://docs.mongodb.com/manual/reference/limits/
INDEX_DATABASE = [
    ('max_size', get_random_string(string.ascii_lowercase, 64)),
    ('plus', get_random_string(string.ascii_lowercase, 5) + '+' + get_random_string(string.ascii_lowercase, 5)),
    ('underscore', get_random_string(string.ascii_lowercase, 5) + '_' + get_random_string(string.ascii_lowercase, 5)),
    ('comma', get_random_string(string.ascii_lowercase, 5) + ',' + get_random_string(string.ascii_lowercase, 5)),
    ('short', 'a'),
]


@mongodb
@pytest.mark.parametrize('database_name_category,index', INDEX_DATABASE, ids=[i[0] for i in INDEX_DATABASE])
def test_object_names_database(sdc_builder, sdc_executor, mongodb, database_name_category, index):
    """
    Verify that we can respect all the documented database names possible
    """
    database = database_name_category
    collection = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                           raw_data='\n'.join(DATA))

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

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> record_deduplicator >> expression_evaluator >> mongodb_atlas_destination
    record_deduplicator >> trash

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Data is generated in dev_raw_data_source and sent to MongoDB Atlas using pipeline.
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(DATA))
        sdc_executor.stop_pipeline(pipeline)

        # Verify data is received correctly using PyMongo.
        # Similar to writing, while reading data, we specify MongoDB Atlas database and the collection inside it.
        logger.info('Verifying docs received with PyMongo...')
        assert [item['text'] for item in
                mongodb.engine[mongodb_atlas_destination.database][mongodb_atlas_destination.collection].find()] == DATA

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(mongodb_atlas_destination.database)


# Reference https://docs.mongodb.com/manual/reference/limits/
INDEX_COLLECTION = [
    ('max_size', get_random_string(string.ascii_lowercase, 255)),
    ('begin_number', '5' + get_random_string(string.ascii_lowercase, 5)),
    ('plus', get_random_string(string.ascii_lowercase, 5) + '+' + get_random_string(string.ascii_lowercase, 5)),
    ('underscore', get_random_string(string.ascii_lowercase, 5) + '_' + get_random_string(string.ascii_lowercase, 5)),
    ('comma', get_random_string(string.ascii_lowercase, 5) + ',' + get_random_string(string.ascii_lowercase, 5)),
    ('short', 'a'),
]


@mongodb
@pytest.mark.parametrize('collection_name_category,index', INDEX_COLLECTION, ids=[i[0] for i in INDEX_COLLECTION])
def test_object_names_collection(sdc_builder, sdc_executor, mongodb, collection_name_category, index):
    """
    Verify that we can respect all the documented collection names possible
    """
    database = get_random_string(string.ascii_letters, 5)
    collection = collection_name_category

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                           raw_data='\n'.join(DATA))

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

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> record_deduplicator >> expression_evaluator >> mongodb_atlas_destination
    record_deduplicator >> trash

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Data is generated in dev_raw_data_source and sent to MongoDB Atlas using pipeline.
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(DATA))
        sdc_executor.stop_pipeline(pipeline)

        # Verify data is received correctly using PyMongo.
        # Similar to writing, while reading data, we specify MongoDB Atlas database and the collection inside it.
        logger.info('Verifying docs received with PyMongo...')
        assert [item['text'] for item in
                mongodb.engine[mongodb_atlas_destination.database][mongodb_atlas_destination.collection].find()] == DATA

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(mongodb_atlas_destination.database)


@mongodb
def test_dataflow_events(sdc_builder, sdc_executor, mongodb):
    pytest.skip("No events supported in MongoDB Atlas Destination at this time.")


@mongodb
def test_multiple_batches(sdc_builder, sdc_executor, mongodb):
    """
    Test for MongoDB Atlas target stage. We verify that the destination work fine with more than one batch.
    """
    database = get_random_string(string.ascii_letters, 5)
    collection = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')

    dev_data_generator.set_attributes(batch_size=1,
                                      records_to_be_generated=2,
                                      fields_to_generate=[
                                          {'field': 'stringField', 'type': 'STRING', 'precision': 10, 'scale': 2}])

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

    wiretap = pipeline_builder.add_wiretap()

    dev_data_generator >> expression_evaluator >> [mongodb_atlas_destination, wiretap.destination]

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Assert data in Mongodb is equal to data from Dev data generator
        data_from_mongodb = [doc['stringField'] for doc in
                             mongodb.engine[mongodb_atlas_destination.database][
                                 mongodb_atlas_destination.collection].find()]
        data_from_wiretap = [record.field['stringField'] for record in wiretap.output_records]
        assert data_from_mongodb == data_from_wiretap

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(mongodb_atlas_destination.database)


@mongodb
def test_data_format(sdc_builder, sdc_executor, mongodb, keep_data):
    pytest.skip("MongoDB Atlas Destination doesn't deal with data formats")


@mongodb
def test_push_pull(sdc_builder, sdc_executor, mongodb):
    pytest.skip("We haven't re-implemented this test since Dev Data Generator (push) is part of "
                "test_multiple_batches and Dev Raw Data Source (pull) is part of test_data_types.")
