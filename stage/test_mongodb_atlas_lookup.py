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

MONGODB_ATLAS_PROCESSOR = 'com_streamsets_pipeline_stage_processor_mongodb_atlas_MongoDBAtlasDProcessor'
pytestmark = [mongodb, sdc_min_version('5.7.0')]


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


def _write_docs_in_database(mongodb, mongodb_atlas_lookup, data=None):
    # MongoDB and PyMongo add '_id' to the dictionary entries e.g. docs_in_database
    # when used for inserting in collection. Hence the deep copy.
    if data is None:
        data = NESTED_DOCS
    docs_in_database = copy.deepcopy(data)

    # Create documents in MongoDB using PyMongo.
    # First a database is created. Then a collection is created inside that database.
    # Then documents are created in that collection.
    logger.info('Adding documents into %s collection using PyMongo...', mongodb_atlas_lookup.collection)
    mongodb_database = mongodb.engine[mongodb_atlas_lookup.database]
    mongodb_collection = mongodb_database[mongodb_atlas_lookup.collection]

    insert_list = [mongodb_collection.insert_one(doc) for doc in docs_in_database]
    assert len(insert_list) == len(docs_in_database)

    return mongodb_atlas_lookup


def test_mongodb_atlas_lookup_processor(sdc_builder, sdc_executor, mongodb):
    """
    Create 2 nested documents in MongoDB Atlas and confirm that MongoDB Atlas Lookup Processor can find the documents.

    The pipeline looks like:
        dev_raw_data_source >> mongodb_atlas_lookup >> wiretap
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    lookup_data = ['name'] + [row['name'] for row in NESTED_DOCS]
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(lookup_data),
                                       stop_after_first_batch=True)

    mongodb_atlas_lookup = pipeline_builder.add_stage(name=MONGODB_ATLAS_PROCESSOR)
    mongodb_atlas_lookup.set_attributes(database=get_random_string(ascii_letters, 5),
                                        collection=get_random_string(ascii_letters, 10),
                                        result_field='/result',
                                        document_to_sdc_field_mappings=[dict(keyName='name', sdcField='/name')])

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_lookup.tls_mode = 'NONE'
        mongodb_atlas_lookup.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> mongodb_atlas_lookup >> wiretap.destination

    pipeline = pipeline_builder.build(title='Test MongoDB Atlas Lookup').configure_for_environment(mongodb)

    try:
        _write_docs_in_database(mongodb, mongodb_atlas_lookup)

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        assert len(records) == len(NESTED_DOCS)
        for record, actual in zip(records, NESTED_DOCS):
            assert record.get_field_data('/result/location/city') == actual['location']['city']
            assert record.get_field_data('/result/location/state') == actual['location']['state']

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_lookup.database)
        mongodb.engine.drop_database(mongodb_atlas_lookup.database)


def test_mongodb_atlas_lookup_processor_implicit_port(sdc_builder, sdc_executor, mongodb):
    """
    Try to connect to MongoDB Atlas using a connection string with a implicit port. Simple pipeline and check the
    results with wiretap.

    The pipeline looks like:
        dev_raw_data_source >> mongodb_atlas_lookup >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    lookup_data = ['name'] + [row['name'] for row in NESTED_DOCS]
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(lookup_data),
                                       stop_after_first_batch=True)

    mongodb_atlas_lookup = pipeline_builder.add_stage(name=MONGODB_ATLAS_PROCESSOR)
    mongodb_atlas_lookup.set_attributes(database=get_random_string(ascii_letters, 5),
                                        collection=get_random_string(ascii_letters, 10),
                                        result_field='/result',
                                        document_to_sdc_field_mappings=[dict(keyName='name', sdcField='/name')])

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_lookup.tls_mode = 'NONE'
        mongodb_atlas_lookup.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> mongodb_atlas_lookup >> wiretap.destination

    pipeline = pipeline_builder.build(title='Test MongoDB Atlas Lookup Implicit Port').configure_for_environment(mongodb)

    # configure_for_environment will set the connection string, so we need to override it here without the explicit
    # port number. Also, the connection string format depends on the MongoDB version
    if mongodb.atlas:
        connection_string = f'mongodb+srv://{mongodb.hostname}/{mongodb.database}?{mongodb.options}'
    else:
        connection_string = f'mongodb://{mongodb.hostname}/{mongodb.database}?{mongodb.options}'

    mongodb_atlas_lookup.set_attributes(connection_string=connection_string)

    try:
        _write_docs_in_database(mongodb, mongodb_atlas_lookup)

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        assert len(records) == len(NESTED_DOCS)
        for record, actual in zip(records, NESTED_DOCS):
            assert record.get_field_data('/result/location/city') == actual['location']['city']
            assert record.get_field_data('/result/location/state') == actual['location']['state']

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_lookup.database)
        mongodb.engine.drop_database(mongodb_atlas_lookup.database)


def test_mongodb_atlas_lookup_processor_nested_lookup(sdc_builder, sdc_executor, mongodb):
    """
    Create 2 nested documents in MongoDB Atlas and confirm that MongoDB Atlas Lookup Processor can find the documents.

    The pipeline looks like:
        dev_raw_data_source >> mongodb_atlas_lookup >> wiretap
    """

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

    mongodb_atlas_lookup = pipeline_builder.add_stage(name=MONGODB_ATLAS_PROCESSOR)
    mongodb_atlas_lookup.set_attributes(database=get_random_string(ascii_letters, 5),
                                        collection=get_random_string(ascii_letters, 10),
                                        result_field='/result',
                                        document_to_sdc_field_mappings=mapping)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_lookup.tls_mode = 'NONE'
        mongodb_atlas_lookup.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> mongodb_atlas_lookup >> wiretap.destination

    pipeline = pipeline_builder.build(title='Test MongoDB Atlas Lookup Nested Documents').configure_for_environment(mongodb)

    try:
        _write_docs_in_database(mongodb, mongodb_atlas_lookup, NESTED_DOCS)

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        assert len(records) == len(NESTED_DOCS)
        for record, actual in zip(records, NESTED_DOCS):
            assert record.get_field_data('/result/location/city') == actual['location']['city']

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_lookup.database)
        mongodb.engine.drop_database(mongodb_atlas_lookup.database)


@pytest.mark.parametrize('data_format', [
    "SIMPLE",
    "NESTED"
])
def test_mongodb_atlas_lookup_processor_mapping(sdc_builder, sdc_executor, mongodb, data_format):
    """
    Try to map a field from MongoDB Atlas into a field in a record with different header, from a simple document and
    from a nested document and multiple unique keys.

    The pipeline looks like:
        dev_raw_data_source >> mongodb_atlas_lookup >> trash
    """
    if data_format == 'SIMPLE':
        data_to_insert = [{'f1': 'a', 'f2': 'one'}, {'f1': 'b', 'f2': 'two'}]
        mapping = [dict(keyName='f1', sdcField='/h1')]
        lookup_data = ['h1'] + [row['f1'] for row in data_to_insert]
    else:
        data_to_insert = [{'f1': 'a', 'f2': {'g1': '1', 'g2': 'one'}}, {'f1': 'b', 'f2': {'g1': '2', 'g2': 'two'}}]
        mapping = [dict(keyName='f1', sdcField='/h1'), dict(keyName='f2.g1', sdcField='/g1')]
        lookup_data = ['h1,g1'] + [','.join([row['f1'], row['f2']['g1']]) for row in data_to_insert]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(lookup_data),
                                       stop_after_first_batch=True)

    mongodb_atlas_lookup = pipeline_builder.add_stage(name=MONGODB_ATLAS_PROCESSOR)
    mongodb_atlas_lookup.set_attributes(database=get_random_string(ascii_letters, 5),
                                        collection=get_random_string(ascii_letters, 10),
                                        result_field='/result',
                                        document_to_sdc_field_mappings=mapping)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_lookup.tls_mode = 'NONE'
        mongodb_atlas_lookup.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> mongodb_atlas_lookup >> wiretap.destination

    pipeline = pipeline_builder.build(title=f'Test MongoDB Atlas Lookup Mapping [{data_format}]')\
        .configure_for_environment(mongodb)

    try:
        _write_docs_in_database(mongodb, mongodb_atlas_lookup, data_to_insert)

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        assert len(records) == len(data_to_insert)
        for record, actual in zip(records, data_to_insert):
            assert record.get_field_data('/result/f1') == actual['f1']
            if data_format == 'SIMPLE':
                assert record.get_field_data('/result/f2') == actual['f2']
            else:
                assert record.get_field_data('/result/f2/g1') == actual['f2']['g1']
                assert record.get_field_data('/result/f2/g2') == actual['f2']['g2']
    finally:
        logger.info('Dropping %s database...', mongodb_atlas_lookup.database)
        mongodb.engine.drop_database(mongodb_atlas_lookup.database)


@pytest.mark.parametrize('behavior', [
    "FIRST_ONLY",
    "SPLIT_INTO_MULTIPLE_RECORDS"
])
def test_mongodb_atlas_lookup_processor_multiple_values_behaviour(sdc_builder, sdc_executor, mongodb, behavior):
    """
    Blablabla.

    The pipeline looks like:
        dev_raw_data_source >> mongodb_atlas_lookup >> trash
    """
    data_to_insert = [{'f1': 'a', 'f2': 'one'}, {'f1': 'a', 'f2': 'two'}]
    mapping = [dict(keyName='f1', sdcField='/f1')]
    lookup_data = ['f1', data_to_insert[0]['f1']]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(lookup_data),
                                       stop_after_first_batch=True)

    mongodb_atlas_lookup = pipeline_builder.add_stage(name=MONGODB_ATLAS_PROCESSOR)
    mongodb_atlas_lookup.set_attributes(database=get_random_string(ascii_letters, 5),
                                        collection=get_random_string(ascii_letters, 10),
                                        result_field='/result',
                                        document_to_sdc_field_mappings=mapping,
                                        multiple_values_behavior=behavior)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_lookup.tls_mode = 'NONE'
        mongodb_atlas_lookup.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> mongodb_atlas_lookup >> wiretap.destination

    pipeline = pipeline_builder.build(title=f'Test MongoDB Atlas Lookup [multiple_values={behavior}]')\
        .configure_for_environment(mongodb)

    try:
        _write_docs_in_database(mongodb, mongodb_atlas_lookup, data_to_insert)

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        if behavior == 'FIRST_ONLY':
            assert len(records) == 1
            assert records[0].get_field_data('/result/f1') == data_to_insert[0]['f1']
            assert records[0].get_field_data('/result/f2') in [data_to_insert[0]['f2'], data_to_insert[1]['f2']]
        else:
            assert len(records) == 2
            for record, actual in zip(records, data_to_insert):
                assert record.get_field_data('/result/f1') == actual['f1']
                assert record.get_field_data('/result/f2') == actual['f2']

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_lookup.database)
        mongodb.engine.drop_database(mongodb_atlas_lookup.database)


@pytest.mark.parametrize('behavior', ["PASS_RECORD_ON", "SEND_TO_ERROR"])
def test_mongodb_atlas_lookup_processor_missing_values_behaviour(sdc_builder, sdc_executor, mongodb, behavior):
    """
    Blablabla.

    The pipeline looks like:
        dev_raw_data_source >> mongodb_atlas_lookup >> trash
    """
    data_to_insert = [{'f1': 'b', 'f2': 'two'}, {'f1': 'c', 'f2': 'three'}]
    mapping = [dict(keyName='f1', sdcField='/f1')]
    lookup_data = ['f1', 'a']

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(lookup_data),
                                       stop_after_first_batch=True)

    mongodb_atlas_lookup = pipeline_builder.add_stage(name=MONGODB_ATLAS_PROCESSOR)
    mongodb_atlas_lookup.set_attributes(database=get_random_string(ascii_letters, 5),
                                        collection=get_random_string(ascii_letters, 10),
                                        result_field='/result',
                                        document_to_sdc_field_mappings=mapping,
                                        missing_values_behavior=behavior)

    # Configure MongoDB Atlas origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_lookup.tls_mode = 'NONE'
        mongodb_atlas_lookup.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> mongodb_atlas_lookup >> wiretap.destination

    pipeline = pipeline_builder.build(title=f'Test MongoDB Atlas Lookup [multiple_values={behavior}]') \
        .configure_for_environment(mongodb)

    try:
        _write_docs_in_database(mongodb, mongodb_atlas_lookup, data_to_insert)

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        error_records = wiretap.error_records
        if behavior == 'PASS_RECORD_ON':
            assert len(records) == 1
            assert len(error_records) == 0
            assert records[0].get_field_data('/f1') == 'a'
        else:
            assert len(records) == 0
            assert len(error_records) == 1

    finally:
        logger.info('Dropping %s database...', mongodb_atlas_lookup.database)
        mongodb.engine.drop_database(mongodb_atlas_lookup.database)
