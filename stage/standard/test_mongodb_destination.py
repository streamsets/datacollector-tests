# Copyright 2020 StreamSets Inc.
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
from decimal import Decimal

import pytest
from streamsets.testframework.markers import mongodb
from streamsets.testframework.utils import get_random_string, Version

logger = logging.getLogger(__name__)


DATA_TYPES = [
    ('true', 'BOOLEAN', False, True),
    ('true', 'BOOLEAN', True, True),
    ('a', 'CHAR', False, 'a'),
    ('a', 'CHAR', True, 'a'),
    # ('a', 'BYTE', False, None),  # Not supported today
    # ('a', 'BYTE', True, None),  # Not supported today
    (120, 'SHORT', False, 120),
    (120, 'SHORT', True, 120),
    (120, 'INTEGER', False, 120),
    (120, 'INTEGER', True, 120),
    (120, 'LONG', False, 120),
    (120, 'LONG', True, 120),
    (20.1, 'FLOAT', False, 20.1),
    (20.1, 'FLOAT', True, 20.1),
    (20.1, 'DOUBLE', False, 20.1),
    (20.1, 'DOUBLE', True, 20.1),
    (20.1, 'DECIMAL', False, 20.1),
    (20.1, 'DECIMAL', True, Decimal(20.10)),
    ('2020-01-01 10:00:00', 'DATE', False, 1577872800000),
    ('2020-01-01 10:00:00', 'DATE', True, datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'TIME', False, 1577872800000),
    ('2020-01-01 10:00:00', 'TIME', True, datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATETIME', False, 1577872800000),
    ('2020-01-01 10:00:00', 'DATETIME', True,  datetime.datetime(2020, 1, 1, 10, 0)),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', False, '2020-01-01T10:00:00Z'),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', True, '2020-01-01T10:00:00Z'),
    ('string', 'STRING', False, 'string'),
    ('string', 'STRING', True, 'string'),
    # ('string', 'BYTE_ARRAY', False, 'string'),  # Not supported today
    ('string', 'BYTE_ARRAY', True, b'string'),
]


@mongodb('legacy')
@pytest.mark.parametrize('input,converter_type,improve_types,expected', DATA_TYPES,
                         ids=[f'{i[2]}_{i[1]}' for i in DATA_TYPES])
def test_data_types(sdc_builder, sdc_executor, mongodb, input, converter_type, improve_types, expected):

    if Version(sdc_builder.version) <= Version('4.0.2') and improve_types:
        pytest.skip('Improved Type Conversion is not present on that SDC version')
    if mongodb.atlas:
        pytest.skip("MongoDB stages don't support connect to MongoDB Atlas")

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
    # MongoDB destination uses the CRUD operation in the sdc.operation.type record header attribute when writing
    # to MongoDB. Value 4 specified below is for UPSERT.
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': '1'}]

    mongodb_dest = pipeline_builder.add_stage('MongoDB', type='destination')

    if Version(sdc_builder.version) > Version('4.0.2'):
        mongodb_dest.set_attributes(improve_type_conversion=improve_types)

    mongodb_dest.set_attributes(database=database,
                                collection=collection)

    origin >> converter >> expression_evaluator >> mongodb_dest

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)
    pipeline.configuration["shouldRetry"] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Run pipeline and read from MongoDB to assert
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_dest.database][mongodb_dest.collection].find()]

        assert len(mongodb_documents) == 1
        doc = mongodb_documents[0]

        if converter_type == 'FLOAT' and improve_types:
            assert pytest.approx(doc['value']) == expected
        elif converter_type == 'DECIMAL' and improve_types:
            assert pytest.approx(Decimal(str(doc['value']))) == expected
        else:
            assert doc['value'] == expected

    finally:
        logger.info('Dropping %s database...', mongodb_dest.database)
        mongodb.engine.drop_database(mongodb_dest.database)


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


@mongodb('legacy')
@pytest.mark.parametrize('database_name_category,index', INDEX_DATABASE, ids=[i[0] for i in INDEX_DATABASE])
def test_object_names_database(sdc_builder, sdc_executor, mongodb, database_name_category, index):
    """
    Verify that we can respect all the documented database names possible
    """
    if mongodb.atlas:
        pytest.skip("MongoDB stages don't support connect to MongoDB Atlas")

    database = database_name_category
    collection = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                           raw_data='\n'.join(DATA))

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    # MongoDB destination uses the CRUD operation in the sdc.operation.type record header attribute when writing
    # to MongoDB. Value 4 specified below is for UPSERT.
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': '1'}]

    mongodb_dest = pipeline_builder.add_stage('MongoDB', type='destination')
    mongodb_dest.set_attributes(database=database,
                                collection=collection)

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> record_deduplicator >> expression_evaluator >> mongodb_dest
    record_deduplicator >> trash

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Data is generated in dev_raw_data_source and sent to MongoDB using pipeline.
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(DATA))
        sdc_executor.stop_pipeline(pipeline)

        # Verify data is received correctly using PyMongo.
        # Similar to writing, while reading data, we specify MongoDB database and the collection inside it.
        logger.info('Verifying docs received with PyMongo...')
        assert [item['text'] for item in mongodb.engine[mongodb_dest.database][mongodb_dest.collection].find()] == DATA

    finally:
        logger.info('Dropping %s database...', mongodb_dest.database)
        mongodb.engine.drop_database(mongodb_dest.database)


# Reference https://docs.mongodb.com/manual/reference/limits/
INDEX_COLLECTION = [
    ('max_size', get_random_string(string.ascii_lowercase, 255)),
    ('begin_number', '5' + get_random_string(string.ascii_lowercase, 5)),
    ('plus', get_random_string(string.ascii_lowercase, 5) + '+' + get_random_string(string.ascii_lowercase, 5)),
    ('underscore', get_random_string(string.ascii_lowercase, 5) + '_' + get_random_string(string.ascii_lowercase, 5)),
    ('comma', get_random_string(string.ascii_lowercase, 5) + ',' + get_random_string(string.ascii_lowercase, 5)),
    ('short', 'a'),
]


@mongodb('legacy')
@pytest.mark.parametrize('collection_name_category,index', INDEX_COLLECTION, ids=[i[0] for i in INDEX_COLLECTION])
def test_object_names_collection(sdc_builder, sdc_executor, mongodb, collection_name_category, index):
    """
    Verify that we can respect all the documented collection names possible
    """
    if mongodb.atlas:
        pytest.skip("MongoDB stages don't support connect to MongoDB Atlas")

    database = get_random_string(string.ascii_letters, 5)
    collection = collection_name_category

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                           raw_data='\n'.join(DATA))

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    # MongoDB destination uses the CRUD operation in the sdc.operation.type record header attribute when writing
    # to MongoDB. Value 4 specified below is for UPSERT.
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': '1'}]

    mongodb_dest = pipeline_builder.add_stage('MongoDB', type='destination')
    mongodb_dest.set_attributes(database=database,
                                collection=collection)

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> record_deduplicator >> expression_evaluator >> mongodb_dest
    record_deduplicator >> trash

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Data is generated in dev_raw_data_source and sent to MongoDB using pipeline.
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
def test_dataflow_events(sdc_builder, sdc_executor, mongodb):
    pytest.skip("No events supported in MongoDB destination at this time.")


@mongodb('legacy')
def test_multiple_batches(sdc_builder, sdc_executor, mongodb):
    """
    Test for MongoDB target stage. We verify that the destination work fine with more than one batch.
    """
    if mongodb.atlas:
        pytest.skip("MongoDB stages don't support connect to MongoDB Atlas")

    database = get_random_string(string.ascii_letters, 5)
    collection = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')

    dev_data_generator.set_attributes(batch_size=1,
                                      records_to_be_generated=2,
                                      fields_to_generate=[
                                          {'field': 'stringField', 'type': 'STRING', 'precision': 10, 'scale': 2}])

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    # MongoDB destination uses the CRUD operation in the sdc.operation.type record header attribute when writing
    # to MongoDB. Value 4 specified below is for UPSERT.
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': '1'}]

    mongodb_dest = pipeline_builder.add_stage('MongoDB', type='destination')
    mongodb_dest.set_attributes(database=database,
                                collection=collection)

    wiretap = pipeline_builder.add_wiretap()

    dev_data_generator >> expression_evaluator >> [mongodb_dest, wiretap.destination]

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Assert data in Mongodb is equal to data from Dev data generator
        data_from_mongodb = [doc['stringField'] for doc in
                             mongodb.engine[mongodb_dest.database][mongodb_dest.collection].find()]
        data_from_wiretap = [record.field['stringField'] for record in wiretap.output_records]
        assert data_from_mongodb == data_from_wiretap

    finally:
        logger.info('Dropping %s database...', mongodb_dest.database)
        mongodb.engine.drop_database(mongodb_dest.database)


@mongodb('legacy')
def test_data_format(sdc_builder, sdc_executor, mongodb, keep_data):
    pytest.skip("MongoDB Destination doesn't deal with data formats")


@mongodb('legacy')
def test_push_pull(sdc_builder, sdc_executor, mongodb):
    pytest.skip("We haven't re-implemented this test since Dev Data Generator (push) is part of "
                "test_multiple_batches and Dev Raw Data Source (pull) is part of test_data_types.")
