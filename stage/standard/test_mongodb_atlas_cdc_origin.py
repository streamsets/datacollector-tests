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

import datetime
import copy
import logging
import string
from bson.decimal128 import Decimal128

import pytest
from streamsets.testframework.markers import mongodb, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

MONGODB_ATLAS_CDC_ORIGIN = 'com_streamsets_pipeline_stage_origin_mongodb_atlas_cdc_MongoDBAtlasCDCDSource'
pytestmark = [mongodb('atlas'), sdc_min_version('5.8.0')]


# BSON types: https://www.mongodb.com/docs/manual/reference/bson-types/
DATA_TYPES = [
    ('true', 'bool', True),
    ('a', 'string', 'a'),
    # ('a', 'byte, 'a'),  # Not supported today
    (120, 'int', 120),
    (120, 'long', 120),
    (20.1, 'double', 20.100000381469727),
    (20.1, 'double', 20.1),
    (20.1, 'decimal', Decimal128('20.10')),
    ('2020-01-01 10:00:00', 'date', datetime.datetime(2020, 1, 1, 10, 0)),
    ("2020-01-01T10:00:00+00:00", 'date', datetime.datetime(2020, 1, 1, 10, 0)),
    ('string', 'array', b'string'),
    ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'uuid', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'.encode('ascii'))
]


@pytest.mark.parametrize('input_value,bson_type,expected', DATA_TYPES, ids=[f'{i[1]}_{i[0]}' for i in DATA_TYPES])
def test_data_types(sdc_builder, sdc_executor, mongodb, input_value, bson_type, expected):
    """ Test all feasible data types MongoDB Atlas CDC origin can read """
    database = get_random_string(string.ascii_letters, 10)
    collection = get_random_string(string.ascii_letters, 10)

    data = [{'value': input_value}]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_atlas_cdc_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_CDC_ORIGIN)
    mongodb_atlas_cdc_origin.set_attributes(read_changes_from='OPLOG',
                                            include_namespaces=[f'{database}.{collection}'])

    # Configure MongoDB Atlas CDC to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_cdc_origin.tls_mode = 'NONE'
        mongodb_atlas_cdc_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_cdc_origin >> wiretap.destination

    pipeline = pipeline_builder.build(title=f'Standard Test MongoDB Atlas CDC - Data Types[bson_type={bson_type}]')\
        .configure_for_environment(mongodb)

    try:
        _write_in_mongodb_atlas(mongodb, database, collection, data)

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(data))
        sdc_executor.stop_pipeline(pipeline)

        assert data == [{'value': record.field['value'].value} for record in wiretap.output_records]

    finally:
        logger.info('Dropping %s database...', database)
        mongodb.engine.drop_database(database)


# Reference https://docs.mongodb.com/manual/reference/limits/
INDEX_NAMES = [
    ('max_size', get_random_string(string.ascii_letters, 63).lower()),
    ('begin_number', '5' + get_random_string(string.ascii_letters, 5).lower()),
    ('plus', get_random_string(string.ascii_letters, 5).lower() + '+' + get_random_string(string.ascii_letters, 5).lower()),
    ('underscore',
    get_random_string(string.ascii_letters, 5).lower() + '_' + get_random_string(string.ascii_letters, 5).lower()),
    ('comma',
     get_random_string(string.ascii_letters, 5).lower() + ',' + get_random_string(string.ascii_letters, 5).lower()),
    ('short', get_random_string(string.ascii_letters, 1).lower()),
]


@pytest.mark.parametrize('name_category,index', INDEX_NAMES, ids=[i[0] for i in INDEX_NAMES])
def test_object_names_database_collection(sdc_builder, sdc_executor, mongodb, name_category, index):
    """
    Verify that we can respect all the documented buckets names possible in 'database.collection'
    """
    database = get_random_string(string.ascii_letters, 5)
    collection = index
    data = [{'value': get_random_string(string.ascii_letters, 5)}]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_atlas_cdc_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_CDC_ORIGIN)
    mongodb_atlas_cdc_origin.set_attributes(read_changes_from='OPLOG',
                                            include_namespaces=[f'{database}.{collection}'])

    # Configure MongoDB Atlas CDC to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_cdc_origin.tls_mode = 'NONE'
        mongodb_atlas_cdc_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_cdc_origin >> wiretap.destination

    pipeline = pipeline_builder.build(title=f'Standard Test MongoDB Atlas CDC - Database and Collection names'
                                            f'[name_category={name_category}]')\
        .configure_for_environment(mongodb)

    try:
        _write_in_mongodb_atlas(mongodb, database, collection, data)

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(data))
        sdc_executor.stop_pipeline(pipeline)

        assert data == [{'value': record.field['value'].value} for record in wiretap.output_records]
    finally:
        logger.info('Dropping %s database...', database)
        mongodb.engine.drop_database(database)


def test_dataflow_events(sdc_builder, sdc_executor, mongodb):
    pytest.skip('MongoDB Atlas CDC does not generate events.')


@pytest.mark.parametrize('batch_size', [50, 5, 3])
def test_multiple_batches(sdc_builder, sdc_executor, mongodb, batch_size):
    """
    Test that we can start our pipeline multiple times adding more objects in between without reading any duplicated
    record neither missing them.
    """
    max_batch_size = batch_size
    database = get_random_string(string.ascii_letters, 5)
    collection = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    mongodb_atlas_cdc_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_CDC_ORIGIN)
    mongodb_atlas_cdc_origin.set_attributes(read_changes_from='OPLOG',
                                            include_namespaces=[f'{database}.{collection}'],
                                            batch_size_in_records=max_batch_size)

    # Configure MongoDB Atlas CDC to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_cdc_origin.tls_mode = 'NONE'
        mongodb_atlas_cdc_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_cdc_origin >> wiretap.destination

    pipeline = pipeline_builder.build(title='Standard Test MongoDB Atlas CDC - Multiple Batches')\
        .configure_for_environment(mongodb)

    try:
        totals_docs = []
        for _ in range(0, 100):
            actual_data = dict(f1=get_random_string(string.ascii_letters, 5))
            totals_docs.append(actual_data)

        _write_in_mongodb_atlas(mongodb, database, collection, totals_docs)

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(totals_docs))
        sdc_executor.stop_pipeline(pipeline)

        records = [dict(f1=record.field['f1']) for record in wiretap.output_records]
        assert len(records) == len(totals_docs)
        assert all(element in records for element in totals_docs)
        assert all(element in totals_docs for element in records)

    finally:
        logger.info('Dropping %s database...', database)
        mongodb.engine.drop_database(database)


def test_data_format(sdc_builder, sdc_executor, mongodb):
    pytest.skip("MongoDB Atlas CDC doesn't deal with data formats")


def test_resume_offset(sdc_builder, sdc_executor, mongodb):
    """
    Test that we can start our pipeline multiple times without reading any duplicated record neither missing them.
    """
    iterations = 1
    records_per_iteration = 10
    database = get_random_string(string.ascii_letters, 5)
    collection = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    mongodb_atlas_cdc_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_CDC_ORIGIN)
    mongodb_atlas_cdc_origin.set_attributes(read_changes_from='OPLOG',
                                            include_namespaces=[f'{database}.{collection}'])

    # Configure MongoDB Atlas CDC to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_cdc_origin.tls_mode = 'NONE'
        mongodb_atlas_cdc_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_cdc_origin >> wiretap.destination

    pipeline = pipeline_builder.build('Standard Test MongoDB Atlas CDC - Resume Offset')\
        .configure_for_environment(mongodb)
    iteration = 0

    try:
        for iteration in range(0, iterations):
            logger.info(f"Iteration: {iteration}")

            def generator():
                data = []
                for i in range(1, records_per_iteration + 1):
                    item = {
                        "data": {"data": {"foo": "bar"}, "metadata": {"created_time": "2018-03-22T02:24:06.945319214Z",
                                                                      "deletion_time": "", "destroyed": False,
                                                                      "version": iteration * records_per_iteration + i}}}
                    data.append(item)
                return data

            _write_in_mongodb_atlas(mongodb, database, collection, generator())

            # Start pipeline and verify the documents using wiretap.
            sdc_executor.add_pipeline(pipeline)
            sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(records_per_iteration)
            sdc_executor.stop_pipeline(pipeline)

            records = wiretap.output_records

            # We should get the right number of records
            assert len(records) == records_per_iteration

            def _sort_response(entry):
                return entry.field['data']['metadata']['version'].value

            records.sort(key=_sort_response)

            expected_number = iteration * records_per_iteration + 1
            for record in records:
                assert record.field['data']['metadata']['version'].value == expected_number
                expected_number = expected_number + 1

    finally:
        logger.info('Dropping %s database...', database)
        mongodb.engine.drop_database(database)


def _write_in_mongodb_atlas(mongodb, database, collection, data):
    # MongoDB Atlas and PyMongo add '_id' to the dictionary entries e.g. docs_in_database
    # when used for inserting in collection. Hence the deep copy.
    docs_in_database = copy.deepcopy(data)

    # Create documents in MongoDB Atlas using PyMongo.
    # First a database is created. Then a collection is created inside that database.
    # Then documents are created in that collection.
    logger.info('Adding documents into %s collection using PyMongo...', collection)
    mongodb_database = mongodb.engine[database]
    mongodb_collection = mongodb_database[collection]
    if isinstance(docs_in_database, list):
        insert_list = [mongodb_collection.insert_one(doc) for doc in docs_in_database]
        assert len(insert_list) == len(docs_in_database)
    else:
        mongodb_collection.insert_one(docs_in_database)
