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
import copy
from string import ascii_letters

import pytest
from streamsets.testframework.markers import mongodb, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

MONGODB_ATLAS_ORIGIN = 'com_streamsets_pipeline_stage_origin_mongodb_atlas_MongoDBAtlasDSource'
MONGODB_ATLAS_DESTINATION = 'com_streamsets_pipeline_stage_destination_mongodb_atlas_MongoDBAtlasDTarget'
pytestmark = [mongodb, sdc_min_version('5.7.0')]

DATABASE_NAME = 'DB_PERFORMANCE_TEST_NO_DELETE'
COLLECTION_NAME = 'PERFORMANCE_TEST_NO_DELETE'


def _insert_collection(mongodb, record_count):
    # First check if the database and the collection already exists in MongoDB Atlas. If not, insert 10k documents
    # in the collection 'PERFORMANCE_TEST_NO_DELETE'
    if DATABASE_NAME in mongodb.engine.list_database_names() \
            and COLLECTION_NAME in mongodb.engine[DATABASE_NAME].list_collection_names():
        return

    # MongoDB Atlas and PyMongo add '_id' to the dictionary entries e.g. docs_in_database
    # when used for inserting in collection. Hence the deep copy.
    data = [{'user': get_random_string(ascii_letters, 5), 'name': get_random_string(ascii_letters, 5)}
            for _ in range(record_count)]
    docs_in_database = copy.deepcopy(data)

    # Create documents in MongoDB Atlas using PyMongo.
    # First a database is created. Then a collection is created inside that database.
    # Then documents are created in that collection.
    logger.info('Adding documents into %s collection using PyMongo...', COLLECTION_NAME)
    mongodb_database = mongodb.engine[DATABASE_NAME]
    mongodb_collection = mongodb_database[COLLECTION_NAME]
    insert_list = [mongodb_collection.insert_one(doc) for doc in docs_in_database]
    assert len(insert_list) == len(docs_in_database)


@pytest.mark.parametrize("batch_size", [1_000, 10_000, 20_000])
def test_mongodb_atlas_origin(sdc_builder, sdc_executor, mongodb, batch_size):
    """
    Performance benchmark a simple MongoDB Atlas Origin stage to benchmark stage pipeline.
    The test uses an existing collection in MongoDB Atlas database called PERFORMANCE_TEST with 1000000 documents.
    """
    record_count = 10_000
    _insert_collection(mongodb, record_count)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    benchmark_stages = pipeline_builder.add_benchmark_stages()

    mongodb_atlas_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_ORIGIN)
    mongodb_atlas_origin.set_attributes(database=DATABASE_NAME,
                                        collection=COLLECTION_NAME,
                                        batch_size_in_records=batch_size)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_origin.tls_mode = 'NONE'
        mongodb_atlas_origin.authentication_method = 'NONE'

    mongodb_atlas_origin >> benchmark_stages.destination

    pipeline = pipeline_builder.build(title='Performance Tests for MongoDB Atlas Origin') \
        .configure_for_environment(mongodb)
    sdc_executor.benchmark_pipeline(pipeline, record_count=record_count)


@pytest.mark.parametrize('batch_size', [100, 1_000, 5_000])
def test_mongodb_atlas_destination(sdc_builder, sdc_executor, mongodb, batch_size):
    """
    Performance benchmark a benchmark stage to simple MongoDB Atlas Destination stage pipeline.
    """
    record_count = 10_000

    pipeline_builder = sdc_builder.get_pipeline_builder()

    benchmark_stages = pipeline_builder.add_benchmark_stages()
    benchmark_stages.origin.set_attributes(batch_size_in_recs=batch_size)

    mongodb_atlas_destination = pipeline_builder.add_stage(name=MONGODB_ATLAS_DESTINATION)
    mongodb_atlas_destination.set_attributes(database=get_random_string(ascii_letters, 5),
                                             collection=get_random_string(ascii_letters, 10),
                                             unique_keys=[{'collectionPath': '/f1', 'recordPath': ''}])

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_destination.tls_mode = 'NONE'
        mongodb_atlas_destination.authentication_method = 'NONE'

    benchmark_stages.origin >> mongodb_atlas_destination

    pipeline = pipeline_builder.build(title='Performance Tests for MongoDB Atlas Destination') \
        .configure_for_environment(mongodb)

    try:
        sdc_executor.benchmark_pipeline(pipeline, record_count=record_count)

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_destination.database)
        mongodb.engine.drop_database(mongodb_atlas_destination.database)
