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

import logging
import string
import copy
import pytest
import json

from streamsets.sdk.utils import Version
from streamsets.testframework.markers import mongodb, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


DATA_TYPES = [
    ('true', 'BOOLEAN', True),
    ('a', 'CHAR', 'a'),
#   ('a', 'BYTE', None), # Not supported today
    (120, 'SHORT', 120),
    (120, 'INTEGER', 120),
    (120, 'LONG', 120),
    (20.1, 'FLOAT', 20.1),
    (20.1, 'DOUBLE', 20.1),
    (20.1, 'DECIMAL', 20.1),
    ('2020-01-01 10:00:00', 'DATE', 1577872800000),
    ('2020-01-01 10:00:00', 'TIME', 1577872800000),
    ('2020-01-01 10:00:00', 'DATETIME', 1577872800000),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', '2020-01-01T10:00:00Z'),
    ('string', 'STRING', 'string'),
#   ('string', 'BYTE_ARRAY', 'string'), # Not supported today
]
@mongodb
@pytest.mark.parametrize('input,converter_type,expected', DATA_TYPES, ids=[i[1] for i in DATA_TYPES])
def test_data_types(sdc_builder, sdc_executor, mongodb, input, converter_type, expected):
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
        assert doc['value'] == expected

    finally:
        logger.info('Dropping %s database...', mongodb_dest.database)
        mongodb.engine.drop_database(mongodb_dest.database)


DATA = ['To be or not to be.',
        'Excellence is not an act, it is a habit.',
        'No pains, no gains.']

# Reference https://docs.mongodb.com/manual/reference/limits/
INDEX_DATABASE = [
    ('max_size', get_random_string(string.ascii_letters, 64).lower()),
    ('plus', get_random_string(string.ascii_letters, 5).lower() + '+' + get_random_string(string.ascii_letters, 5).lower()),
    ('underscore', get_random_string(string.ascii_letters, 5).lower() + '_' + get_random_string(string.ascii_letters, 5).lower()),
    ('comma', get_random_string(string.ascii_letters, 5).lower() + ',' + get_random_string(string.ascii_letters, 5).lower()),
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
    ('max_size', get_random_string(string.ascii_letters, 255).lower()),
    ('begin_number', '5' + get_random_string(string.ascii_letters, 5).lower()),
    ('plus', get_random_string(string.ascii_letters, 5).lower() + '+' + get_random_string(string.ascii_letters, 5).lower()),
    ('underscore', get_random_string(string.ascii_letters, 5).lower() + '_' + get_random_string(string.ascii_letters, 5).lower()),
    ('comma', get_random_string(string.ascii_letters, 5).lower() + ',' + get_random_string(string.ascii_letters, 5).lower()),
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


@mongodb
def test_dataflow_events(sdc_builder, sdc_executor, mongodb):
    pytest.skip("No events supported in ElasticSearch destination at this time.")


@mongodb
def test_multiple_batches(sdc_builder, sdc_executor, mongodb):
    """
    Test for MongoDB target stage. We verify that the destination work fine with more than one batch.
    """
    database = get_random_string(string.ascii_letters, 5)
    collection = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source').set_attributes(data_format='TEXT',
                                                                                           raw_data='\n'.join(DATA),
                                                                                           stop_after_first_batch=False)

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    # MongoDB destination uses the CRUD operation in the sdc.operation.type record header attribute when writing
    # to MongoDB. Value 4 specified below is for UPSERT.
    expression_evaluator.header_attribute_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                          'headerAttributeExpression': '1'}]

    mongodb_dest = pipeline_builder.add_stage('MongoDB', type='destination')
    mongodb_dest.set_attributes(database=database,
                                collection=collection)

    dev_raw_data_source >> expression_evaluator >> mongodb_dest

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Data is generated in dev_raw_data_source and sent to MongoDB using pipeline.
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(20)
        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        num_records = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        logger.info(f"Wrote {num_records} records")

        mongodb_documents = [doc for doc in mongodb.engine[mongodb_dest.database][mongodb_dest.collection].find()]
        assert len(mongodb_documents) == num_records

        assert mongodb_documents[0]['text'] == DATA[0]
        assert mongodb_documents[1]['text'] == DATA[1]
        assert mongodb_documents[2]['text'] == DATA[2]
    finally:
        logger.info('Dropping %s database...', mongodb_dest.database)
        mongodb.engine.drop_database(mongodb_dest.database)


@mongodb
def test_data_format(sdc_builder, sdc_executor, mongodb, keep_data):
    pytest.skip("MongoDB Destination doesn't deal with data formats")


@mongodb
def test_push_pull(sdc_builder, sdc_executor, mongodb):
    """
    We plan to verify that the connector works fine with Dev Raw Data Source and Dev Data Generator, an example of pull
    and push strategies, so as we already verified Dev Raw Data Source, we will use Dev Data Generator here to complete
    the coverage.
    """
    database = get_random_string(string.ascii_letters, 5)
    collection = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')

    dev_data_generator.set_attributes(batch_size=1,
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

    dev_data_generator >> expression_evaluator >> mongodb_dest

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(25)
        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        num_records = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count

        # assert record count to MongoDB the size of the objects put
        mongodb_documents = [doc for doc in mongodb.engine[mongodb_dest.database][mongodb_dest.collection].find()]
        assert len(mongodb_documents) == num_records

    finally:
        logger.info('Dropping %s database...', mongodb_dest.database)
        mongodb.engine.drop_database(mongodb_dest.database)
