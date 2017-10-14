# Copyright 2017 StreamSets Inc.
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The tests in this module follow a pattern of creating pipelines with
:py:obj:`testframework.sdc_models.PipelineBuilder` in one version of SDC and then importing and running them in
another.
"""

import copy
import logging
import time
from string import ascii_letters

from testframework.markers import mongodb
from testframework.utils import get_random_string

logger = logging.getLogger(__name__)

ORIG_DOCS = [
    {'name': 'Flute'},
    {'name': 'Oboe'},
    {'name': 'Violin'}
]

DATA = ['To be or not to be.',
        'Excellence is not an act, it is a habit.',
        'No pains, no gains.']


@mongodb
def test_mongodb_oplog_origin(sdc_builder, sdc_executor, mongodb):
    """
    Insert data in MongoDB and then check if MongoDB Oplog origin captures changes in data from MongoDB correctly.

    The pipeline looks like:
        mongodb_oplog >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    time_now = int(time.time())
    mongodb_oplog = pipeline_builder.add_stage('MongoDB Oplog')
    database_name = get_random_string(ascii_letters, 10)
    # Specify that MongoDB Oplog needs to read changes occuring after time_now.
    mongodb_oplog.set_attributes(collection='oplog.rs', initial_timestamp=time_now, initial_ordinal=1)

    trash = pipeline_builder.add_stage('Trash')
    mongodb_oplog >> trash
    pipeline = pipeline_builder.build().configure_for_environment(mongodb)

    try:
        # Insert documents in MongoDB using PyMongo.
        # First a database is created. Then a collection is created inside that database.
        # Then documents are inserted in that collection.
        mongodb_database = mongodb.engine[database_name]
        mongodb_collection = mongodb_database[get_random_string(ascii_letters, 10)]
        input_rec_count = 6
        inserted_list = mongodb_collection.insert_many([{'x': i} for i in range(input_rec_count)])
        assert len(inserted_list.inserted_ids) == input_rec_count

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        assert len(snapshot[mongodb_oplog].output) == input_rec_count
        for record in list(enumerate(snapshot[mongodb_oplog].output)):
            assert record[1].value['value']['o']['value']['x']['value'] == str(record[0])
            # Verify the operation type is 'i' which is for 'insert' since we inserted the records earlier.
            assert record[1].value['value']['op']['value'] == 'i'
            assert record[1].value['value']['ts']['value']['timestamp']['value'] > time_now

    finally:
        logger.info('Dropping %s database...', database_name)
        mongodb.engine.drop_database(database_name)


@mongodb
def test_mongodb_origin_simple(sdc_builder, sdc_executor, mongodb):
    """
    Create 3 simple documents in MongoDB and confirm that MongoDB origin reads them.

    The pipeline looks like:
        mongodb_origin >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_origin = pipeline_builder.add_stage('MongoDB', type='origin')
    mongodb_origin.set_attributes(is_capped=False,
                                  database=get_random_string(ascii_letters, 5),
                                  collection=get_random_string(ascii_letters, 10))

    trash = pipeline_builder.add_stage('Trash')
    mongodb_origin >> trash
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
        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)
        rows_from_snapshot = [{record.value['value']['name']['sqpath'].lstrip('/'):
                               record.value['value']['name']['value']}
                              for record in snapshot[mongodb_origin].output]

        assert rows_from_snapshot == ORIG_DOCS

    finally:
        logger.info('Dropping %s database...', mongodb_origin.database)
        mongodb.engine.drop_database(mongodb_origin.database)


@mongodb
def test_mongodb_destination(sdc_builder, sdc_executor, mongodb):
    """
    Send simple text into MongoDB destination from Dev Raw Data Source and
        confirm that MongoDB correctly received them using PyMongo.

    The pipeline looks like:
        dev_raw_data_source >> expression_evaluator >> mongodb_dest
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data='\n'.join(DATA))

    expression_evaluator = pipeline_builder.add_stage('Expression Evaluator')
    # MongoDB destination uses the CRUD operation in the sdc.operation.type record header attribute when writing
    # to MongoDB. Value 4 specified below is for UPSERT.
    expression_evaluator.header_expressions = [{'attributeToSet': 'sdc.operation.type',
                                                'headerAttributeExpression': '4'}]

    mongodb_dest = pipeline_builder.add_stage('MongoDB', type='destination')
    mongodb_dest.set_attributes(database=get_random_string(ascii_letters, 5),
                                collection=get_random_string(ascii_letters, 10),
                                unique_key_field='/text')

    dev_raw_data_source >> expression_evaluator >> mongodb_dest
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
