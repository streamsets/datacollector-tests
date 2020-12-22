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

import copy
import logging
import string

import pytest
from streamsets.testframework.markers import mongodb
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@mongodb
def test_data_types(sdc_builder, sdc_executor, mongodb):
    pytest.skip("""MongoDB origin don't talk to a structured system, so we don't need to test each data type.""")


ORIG_DOCS = [
    {'name': 'Flute'},
    {'name': 'Oboe'},
    {'name': 'Violin'}
]

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
    Verify that we can respect all the documented buckets names possible
    """
    database = database_name_category
    collection = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_origin = pipeline_builder.add_stage('MongoDB', type='origin')
    mongodb_origin.set_attributes(capped_collection=False,
                                  database=database,
                                  collection=collection)

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

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(docs_in_database))
        sdc_executor.stop_pipeline(pipeline)

        assert ORIG_DOCS == [{'name': record.field['name'].value} for record in wiretap.output_records]

    finally:
        logger.info('Dropping %s database...', mongodb_origin.database)
        mongodb.engine.drop_database(mongodb_origin.database)


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
    Verify that we can respect all the documented buckets names possible
    """
    database = get_random_string(string.ascii_letters, 5)
    collection = collection_name_category

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_origin = pipeline_builder.add_stage('MongoDB', type='origin')
    mongodb_origin.set_attributes(capped_collection=False,
                                  database=database,
                                  collection=collection)

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

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(docs_in_database))
        sdc_executor.stop_pipeline(pipeline)

        assert ORIG_DOCS == [{'name': record.field['name'].value} for record in wiretap.output_records]
    finally:
        logger.info('Dropping %s database...', mongodb_origin.database)
        mongodb.engine.drop_database(mongodb_origin.database)


NO_EVENTS_DOCS = [
    {'name': 'Flute'}
]


@mongodb
def test_dataflow_events(sdc_builder, sdc_executor, mongodb):
    """
    Test that an empty origin linked to a Pipeline Finisher Executor which ends the pipeline when
    a no-more-data is received actually emits this event.
    """
    database = get_random_string(string.ascii_letters, 5)
    collection = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_origin = pipeline_builder.add_stage('MongoDB', type='origin')
    mongodb_origin.set_attributes(capped_collection=False,
                                  database=database,
                                  collection=collection,
                                  batch_size_in_records=5)

    # Pipeline Finisher Executor, note the precondition
    pipeline_finished_executor = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finished_executor.set_attributes(stage_record_preconditions=["${record:eventType() == 'no-more-data'}"])

    trash = pipeline_builder.add_stage('Trash')
    wiretap = pipeline_builder.add_wiretap()

    mongodb_origin >> trash
    mongodb_origin >= [pipeline_finished_executor, wiretap.destination]

    pipeline = pipeline_builder.build(title='MongoDB origin no more data event').configure_for_environment(mongodb)
    sdc_executor.add_pipeline(pipeline)

    try:
        docs_in_database = copy.deepcopy(NO_EVENTS_DOCS)

        # Create documents in MongoDB using PyMongo.
        # First a database is created. Then a collection is created inside that database.
        # Then documents are created in that collection.
        logger.info('Adding documents into %s collection using PyMongo...', mongodb_origin.collection)
        mongodb_database = mongodb.engine[mongodb_origin.database]
        mongodb_collection = mongodb_database[mongodb_origin.collection]
        insert_list = [mongodb_collection.insert_one(doc) for doc in docs_in_database]
        assert len(insert_list) == len(docs_in_database)

        # Run the pipeline.
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        history = sdc_executor.get_pipeline_history(pipeline)
        # If we are here this means that a no-more-data was sent

        # Check that the number of records is correct
        input_records = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        output_records = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        assert input_records == 1, 'Observed %d input records (expected 0)' % input_records
        assert output_records == 4, 'Observed %d output records (expected 4)' % output_records

        # We have exactly one output record, check that it is a no-more-data event
        num_event_records = len(wiretap.output_records)
        assert num_event_records == 1, 'Received %d event records (expected 1)' % num_event_records
        event_record = wiretap.output_records[0]
        event_type = event_record.header.values['sdc.event.type']
        assert event_type == 'no-more-data', 'Received %s as event type (expected no-more-data)' % event_type

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            logger.info('Stopping pipeline')
            sdc_executor.stop_pipeline(pipeline)
            raise AssertionError('Pipeline was still running after 60s')


@mongodb
@pytest.mark.parametrize('batch_size', [50, 5, 3])
def test_multiple_batches(sdc_builder, sdc_executor, mongodb, batch_size):
    """
    Test that using multithreaded pipeline we can start our pipeline multiple times adding more objects in between
    without reading any duplicated record neither missing them.
    """
    max_batch_size = batch_size
    database = get_random_string(string.ascii_letters, 5)
    collection = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    mongodb_origin = pipeline_builder.add_stage('MongoDB', type='origin')
    mongodb_origin.set_attributes(capped_collection=False,
                                  database=database,
                                  collection=collection,
                                  batch_size_in_records=max_batch_size)

    wiretap = pipeline_builder.add_wiretap()
    mongodb_origin >> wiretap.destination

    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")
    finisher.stage_record_preconditions = ["${record:eventType() == 'no-more-data'}"]
    mongodb_origin >= finisher

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)
    sdc_executor.add_pipeline(pipeline)

    try:
        totals_docs = []
        for _ in range(0, 100):
            actual_data = dict(f1=get_random_string(string.ascii_letters, 5))
            totals_docs.append(actual_data)

        docs_in_database = copy.deepcopy(totals_docs)

        logger.info('Adding documents into %s collection using PyMongo...', mongodb_origin.collection)
        mongodb_database = mongodb.engine[mongodb_origin.database]
        mongodb_collection = mongodb_database[mongodb_origin.collection]
        insert_list = [mongodb_collection.insert_one(doc) for doc in docs_in_database]
        assert len(insert_list) == len(docs_in_database)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = [dict(f1=record.field['f1']) for record in wiretap.output_records]

        assert len(records) == len(totals_docs)
        assert all(element in records for element in totals_docs)
        assert all(element in totals_docs for element in records)

    finally:
        logger.info('Dropping %s database...', mongodb_origin.database)
        mongodb.engine.drop_database(mongodb_origin.database)


@mongodb
def test_resume_offset(sdc_builder, sdc_executor, mongodb):
    """
    Test that we can start our pipeline multiple times without reading any duplicated record neither missing them.
    """
    iterations = 1
    records_per_iteration = 10
    database = get_random_string(string.ascii_letters, 5)
    collection = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    mongodb_origin = pipeline_builder.add_stage('MongoDB', type='origin')
    mongodb_origin.set_attributes(capped_collection=False,
                                  database=database,
                                  collection=collection,
                                  initial_offset='baa',
                                  offset_field_type='STRING',
                                  offset_field='data.data.foo')

    wiretap = pipeline_builder.add_wiretap()

    mongodb_origin >> wiretap.destination

    pipeline = pipeline_builder.build().configure_for_environment(mongodb)
    sdc_executor.add_pipeline(pipeline)
    iteration = 0

    try:
        for iteration in range(0, iterations):
            logger.info(f"Iteration: {iteration}")

            def generator():
                data = []
                for i in range(1, records_per_iteration + 1):
                    item = {"data": {"data": {"foo": "bar"}, "metadata": {"created_time": "2018-03-22T02:24:06.945319214Z",
                        "deletion_time": "", "destroyed": False, "version": iteration * records_per_iteration + i}}}
                    data.append(item)
                return data

            docs_in_database = copy.deepcopy(generator())

            logger.info('Adding documents into %s collection using PyMongo...', mongodb_origin.collection)
            mongodb_database = mongodb.engine[mongodb_origin.database]
            mongodb_collection = mongodb_database[mongodb_origin.collection]
            insert_list = [mongodb_collection.insert_one(doc) for doc in docs_in_database]
            assert len(insert_list) == len(docs_in_database)

            # Start pipeline and verify the documents using snaphot.
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
        logger.info('Dropping %s database...', mongodb_origin.database)
        mongodb.engine.drop_database(mongodb_origin.database)