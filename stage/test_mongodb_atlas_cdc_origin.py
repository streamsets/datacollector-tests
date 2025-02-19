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
import copy
from datetime import datetime
from pytz import timezone
import time
from string import ascii_letters
from operator import itemgetter

import pytest
from streamsets.testframework.markers import mongodb, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


MONGODB_ATLAS_CDC_ORIGIN = 'com_streamsets_pipeline_stage_origin_mongodb_atlas_cdc_MongoDBAtlasCDCDSource'
pytestmark = [mongodb, sdc_min_version('5.8.0')]


@pytest.mark.parametrize('read_changes_from', [
    'CHANGE_STREAM',
    'OPLOG'
])
def test_mongodb_atlas_cdc_origin(sdc_builder, sdc_executor, mongodb, read_changes_from):
    """
    Read from MongoDB Atlas the records inserted in a collection using Change Stream and Oplog, and confirm that
    MongoDB Atlas CDC origin reads them.

    The pipeline looks like:
        mongodb_atlas_cdc_origin >> wiretap
    """
    if read_changes_from == 'CHANGE_STREAM' and mongodb.version[0] < 4:
        pytest.skip("Initial offset in CHANGE STREAM mode is supported only by MongoDB 4.0 or newer")

    database = get_random_string(string.ascii_letters, 5)
    collection = get_random_string(string.ascii_letters, 5)

    data = [{'f1': i, 'f2': get_random_string(string.ascii_letters, 4)} for i in range(5)]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_atlas_cdc_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_CDC_ORIGIN)
    mongodb_atlas_cdc_origin.set_attributes(read_changes_from=read_changes_from,
                                            include_namespaces=[f'{database}.{collection}'])

    # Configure MongoDB Atlas CDC to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_cdc_origin.tls_mode = 'NONE'
        mongodb_atlas_cdc_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_cdc_origin >> wiretap.destination

    pipeline = pipeline_builder.build(title=f'Test MongoDB Atlas CDC [read_change_from={read_changes_from}]')\
        .configure_for_environment(mongodb)

    try:
        if read_changes_from == 'OPLOG':
            _write_in_mongodb_atlas(mongodb, database, collection, data)

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        if read_changes_from == 'CHANGE_STREAM':
            _write_in_mongodb_atlas(mongodb, database, collection, data)

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(data))
        sdc_executor.stop_pipeline(pipeline)

        # Verify we only read the records insert after the offset value
        records = [{'f1': r.field['f1'], 'f2': r.field['f2']} for r in wiretap.output_records]
        assert len(records) == len(data)
        assert sorted(records, key=itemgetter('f1')) == data

    finally:
        logger.info('Dropping %s database...', database)
        mongodb.engine.drop_database(database)


def test_mongodb_atlas_cdc_origin_initial_offset(sdc_builder, sdc_executor, mongodb):
    """
    Insert two set of records in MongoDB Atlas within a few seconds of each other. Then reads them with Change Stream
    and the initial offset configured before the first set. Confirm that MongoDB Atlas CDC origin only reads the second.

    The pipeline looks like:
        mongodb_atlas_cdc_origin >> wiretap
    """
    if mongodb.version[0] < 4:
        pytest.skip("Initial offset in CHANGE STREAM mode is supported only by MongoDB 4.0 or newer")

    number_of_records = 5
    database = get_random_string(string.ascii_letters, 5)
    collection = get_random_string(string.ascii_letters, 5)

    data_1 = [{'f1': i, 'f2': get_random_string(string.ascii_letters, 4)}
              for i in range(number_of_records)]
    data_2 = [{'f1': i, 'f2': get_random_string(string.ascii_letters, 4)}
              for i in range(number_of_records, number_of_records*2)]

    try:
        # Insert the data with previous offset value into MongoDB Atlas database
        _write_in_mongodb_atlas(mongodb, database, collection, data_1)
        time.sleep(2)   # Wait until all the records are inserted into the MongoDB Atlas CDC

        raw_offset = datetime.now(timezone('GMT'))
        offset = raw_offset.strftime("%Y-%m-%d %H:%M:%S")

        pipeline_builder = sdc_builder.get_pipeline_builder()
        pipeline_builder.add_error_stage('Discard')

        mongodb_atlas_cdc_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_CDC_ORIGIN)
        mongodb_atlas_cdc_origin.set_attributes(include_namespaces=[f'{database}.{collection}'],
                                                initial_offset=offset)

        # Configure MongoDB Atlas CDC to connect to old MongoDB version
        if not mongodb.atlas:
            mongodb_atlas_cdc_origin.tls_mode = 'NONE'
            mongodb_atlas_cdc_origin.authentication_method = 'NONE'

        wiretap = pipeline_builder.add_wiretap()

        mongodb_atlas_cdc_origin >> wiretap.destination

        pipeline = pipeline_builder.build(title=f'Test MongoDB Atlas CDC - Initial Offset')\
            .configure_for_environment(mongodb)

        # Insert the more data into MongoDB Atlas database that we want to read
        _write_in_mongodb_atlas(mongodb, database, collection, data_2)

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', number_of_records)
        sdc_executor.stop_pipeline(pipeline)

        # Verify we only read the records insert after the offset value
        records = [{'f1': r.field['f1'], 'f2': r.field['f2']} for r in wiretap.output_records]
        assert len(records) == number_of_records
        assert sorted(records, key=itemgetter('f1')) == data_2

    finally:
        logger.info('Dropping %s database...', database)
        mongodb.engine.drop_database(database)


@pytest.mark.parametrize('database_collection_values', [
    'ONLY_DATABASE',
    'DATABASE_COLLECTION',
    'MULTI_DATABASES',
    'MULTI_COLLECTIONS'
])
def test_mongodb_atlas_cdc_include_namespaces(sdc_builder, sdc_executor, mongodb, database_collection_values):
    """
    Insert several set of records in differents databases and collections and use the namespace property to read them
    with MongoDB Atlas CDC origin.

    The pipeline looks like:
        mongodb_atlas_cdc_origin >> wiretap
    """
    number_of_records = 5
    total_records = 15
    database_1 = get_random_string(ascii_letters, 5)
    database_2 = get_random_string(ascii_letters, 5)
    collection_1 = get_random_string(ascii_letters, 5)
    collection_2 = get_random_string(ascii_letters, 5)

    data = [{'f1': i, 'f2': get_random_string(string.ascii_letters, 4)}
            for i in range(total_records)]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_atlas_cdc_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_CDC_ORIGIN)
    mongodb_atlas_cdc_origin.set_attributes(read_changes_from='OPLOG')
    if database_collection_values == 'DATABASE_COLLECTION':
        mongodb_atlas_cdc_origin.set_attributes(include_namespaces=[f'{database_1}.{collection_1}'])
        expected_records = data[:number_of_records]
    elif database_collection_values == 'ONLY_DATABASE':
        mongodb_atlas_cdc_origin.set_attributes(include_namespaces=[f'{database_1}'])
        expected_records = data[:number_of_records*2]
    elif database_collection_values == 'MULTI_DATABASES':
        database_2 = get_random_string(ascii_letters, 5)
        mongodb_atlas_cdc_origin.set_attributes(include_namespaces=[f'{database_1}.{collection_1}',
                                                                    f'{database_2}.{collection_1}'])
        expected_records = data[:number_of_records] + data[number_of_records*2:number_of_records*3]
    else:
        collection_2 = get_random_string(ascii_letters, 5)
        mongodb_atlas_cdc_origin.set_attributes(include_namespaces=[f'{database_1}.{collection_1}',
                                                                    f'{database_1}.{collection_2}'])
        expected_records = data[:number_of_records*2]

    # Configure MongoDB Atlas CDC to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_cdc_origin.tls_mode = 'NONE'
        mongodb_atlas_cdc_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_cdc_origin >> wiretap.destination

    pipeline = pipeline_builder.build(title=f'Test MongoDB Atlas CDC ['
                                            f'database_collection_values={database_collection_values}]')\
        .configure_for_environment(mongodb)

    try:
        # Insert data into the collection specified in the stage and into another collection, in the same database
        # in MongoDB Atlas
        _write_in_mongodb_atlas(mongodb, database_1, collection_1, data[:number_of_records])
        _write_in_mongodb_atlas(mongodb, database_1, collection_2, data[number_of_records:number_of_records*2])
        _write_in_mongodb_atlas(mongodb, database_2, collection_1, data[number_of_records*2:number_of_records*3])

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(expected_records), timeout_sec=120)
        sdc_executor.stop_pipeline(pipeline)

        # Verify we read only the records of the specified collection or all the records if not
        records = [{'f1': r.field['f1'], 'f2': r.field['f2']} for r in wiretap.output_records]
        assert len(records) == len(expected_records)
        assert sorted(records, key=itemgetter('f1')) == expected_records

    finally:
        logger.info('Dropping %s and %s database...', database_1, database_2)
        mongodb.engine.drop_database(database_1)
        mongodb.engine.drop_database(database_2)


@pytest.mark.parametrize('read_changes_from', [
    'CHANGE_STREAM',
    'OPLOG'
])
@pytest.mark.parametrize('operation_types,full_record_for_updates', [
    ('INSERT', False),
    ('UPDATE', True),
    ('UPDATE', False),
    ('DELETE', False)
])
def test_mongodb_atlas_cdc_operation_types(sdc_builder, sdc_executor, mongodb, read_changes_from, operation_types,
                                           full_record_for_updates):
    """
    Insert, update and delete different records into MongoDB Atlas and use the list of Operations Types to read the
    changes with MongoDB Atlas CDC origin.

    The pipeline looks like:
        mongodb_atlas_cdc_origin >> wiretap
    """
    if read_changes_from == 'CHANGE_STREAM' and mongodb.version[0] < 4:
        pytest.skip("Initial offset in CHANGE STREAM mode is supported only by MongoDB 4.0 or newer")

    database = get_random_string(string.ascii_letters, 5)
    collection = get_random_string(string.ascii_letters, 5)
    number_of_records = 5

    data = [{'f1': i, 'f2': get_random_string(string.ascii_letters, 4), 'f3': get_random_string(string.ascii_letters, 4)}
            for i in range(number_of_records)]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_atlas_cdc_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_CDC_ORIGIN)
    mongodb_atlas_cdc_origin.set_attributes(read_changes_from=read_changes_from,
                                            include_namespaces=[f'{database}.{collection}'],
                                            operation_types=[operation_types],
                                            get_full_record_for_updates=full_record_for_updates)

    # Configure MongoDB Atlas CDC to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_cdc_origin.tls_mode = 'NONE'
        mongodb_atlas_cdc_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_cdc_origin >> wiretap.destination

    pipeline = pipeline_builder.build(title=f'Test MongoDB Atlas CDC - Operation Type'
                                            f'[read_changes_from={read_changes_from}]'
                                            f'[operation_type={operation_types}]').configure_for_environment(mongodb)

    if operation_types in ['UPDATE', 'DELETE']:
        number_of_records = 1

    try:
        if read_changes_from == 'OPLOG':
            expected_insert, expected_update, expected_delete = _write_operations(mongodb, database, collection, data)

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        if read_changes_from == 'CHANGE_STREAM':
            expected_insert, expected_update, expected_delete = _write_operations(mongodb, database, collection, data)
            time.sleep(5)   # To ensure the pymongo had time to make the operations to the database

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', number_of_records, timeout_sec=300)
        sdc_executor.stop_pipeline(pipeline)

        # Verify we only read the records modified by the operation specified
        records = wiretap.output_records
        assert len(records) == number_of_records
        if operation_types == 'INSERT':
            records_inserted = [{'_id': str(r.field['_id']),
                                 'f1': r.field['f1'],
                                 'f2': r.field['f2'],
                                 'f3': r.field['f3']} for r in records]
            assert sorted(records_inserted, key=itemgetter('f1')) == expected_insert
        elif operation_types == 'UPDATE':
            if full_record_for_updates:
                # Complete the expected record with the full fields from origin data
                expected_update['f1'] = data[1]['f1']
                expected_update['f3'] = data[1]['f3']
                record = {'_id': records[0].get_field_data('/')['_id'],
                          'f1': records[0].get_field_data('/')['f1'],
                          'f2': records[0].get_field_data('/')['f2'],
                          'f3': records[0].get_field_data('/')['f3']}
            else:
                # If the stage don't get the full record for updates, the record will had the '_id' as pk and
                # the keys updated
                expected_update = {'_id': expected_update['_id'], 'f2': expected_update['f2']}
                record = {'_id': records[0].get_field_data('/')['_id'], 'f2': records[0].get_field_data('/')['f2']}
            assert record == expected_update
        else:
            # Check if the '_id' from the document deleted is the same, since the CDC only return the '_id'
            assert records[0].get_field_data('/')['_id'] == expected_delete['_id']

    finally:
        logger.info('Dropping %s database...', database)
        mongodb.engine.drop_database(database)


@sdc_min_version('6.1.0')
@pytest.mark.parametrize('read_changes_from', [
    'CHANGE_STREAM',
    'OPLOG'
])
@pytest.mark.parametrize('full_record_not_found_behavior', ['DISCARD_RECORD', 'UPDATED_FIELDS_RECORD', 'SEND_TO_ERROR'])
def test_mongodb_atlas_cdc_full_record_update_with_a_closely_followed_delete(
        sdc_builder,
        sdc_executor,
        mongodb,
        read_changes_from,
        full_record_not_found_behavior
):
    """
    Update & delete the same record into MongoDB Atlas and read the changes with MongoDB Atlas CDC origin.
    This test wants to check the behaviour when 'Get full record for updates' is active
    and the updated record has been deleted before the pipeline processed it

    The pipeline looks like:
        mongodb_atlas_cdc_origin >> wiretap
    """
    if read_changes_from == 'CHANGE_STREAM' and mongodb.version[0] < 4:
        pytest.skip("Initial offset in CHANGE STREAM mode is supported only by MongoDB 4.0 or newer")

    database = f'test_mongodb_atlas_cdc_origin_{get_random_string(string.ascii_letters, 5)}'
    collection = get_random_string(string.ascii_letters, 5)
    number_of_records = 5

    data = [
        {'f1': i, 'f2': get_random_string(string.ascii_letters, 4), 'f3': get_random_string(string.ascii_letters, 4)}
        for i in range(number_of_records)]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_atlas_cdc_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_CDC_ORIGIN)
    mongodb_atlas_cdc_origin.set_attributes(read_changes_from=read_changes_from,
                                            initial_offset='0',
                                            include_namespaces=[f'{database}.{collection}'],
                                            operation_types=['UPDATE'],
                                            get_full_record_for_updates=True,
                                            full_record_not_found_behavior=full_record_not_found_behavior)

    # Configure MongoDB Atlas CDC to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_cdc_origin.tls_mode = 'NONE'
        mongodb_atlas_cdc_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_cdc_origin >> wiretap.destination

    pipeline = (pipeline_builder.build(title=f'Test MongoDB Atlas CDC - Full record update'
                                             f'[read_changes_from={read_changes_from}]'
                                             f'[full_record_not_found_behaviour={full_record_not_found_behavior}]')
                .configure_for_environment(mongodb))

    try:
        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)

        # Insert data into the database
        _write_in_mongodb_atlas(mongodb, database, collection, [{'f1': 0}])

        data_ids = _get_documents_id(mongodb, database, collection)

        # Update second element of the data already inserted into the database
        _write_in_mongodb_atlas(mongodb, database, collection, {'f1': 0, 'f2': 'Updated'}, 'UPDATE')

        # Delete first element of the data already inserted into the database
        _write_in_mongodb_atlas(mongodb, database, collection, {'f1': 0}, 'DELETE')

        # Adding one more update to have at least one record if the first one is discarded
        _write_in_mongodb_atlas(mongodb, database, collection, [{'f1': 1}])
        _write_in_mongodb_atlas(mongodb, database, collection, {'f1': 1, 'f2': 'Updated'}, 'UPDATE')

        sdc_executor.start_pipeline(pipeline)
        pipeline_status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
        if pipeline_status != 'RUNNING':
            pytest.fail(f"Pipeline status is not RUNNING. The current status is {pipeline_status}")

        if full_record_not_found_behavior == 'DISCARD_RECORD':
            sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
            stage_errors = sdc_executor.get_stage_errors(pipeline, mongodb_atlas_cdc_origin)
            assert len(wiretap.output_records) == 1, f"Expected 1 output record but found {len(wiretap.output_records)}"
            assert len(stage_errors) == 0, f"Expected no error records but found {len(stage_errors)}"
        elif full_record_not_found_behavior == 'UPDATED_FIELDS_RECORD':
            sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 2)
            records = wiretap.output_records
            stage_errors = sdc_executor.get_stage_errors(pipeline, mongodb_atlas_cdc_origin)
            assert len(records) == 2, f"Expected 2 output records but found {len(records)}"
            assert len(stage_errors) == 0, f"Expected no error records but found {len(stage_errors)}"
            # If we are asked for the full record, but it doesn't exist in the DB,
            # we are going to return only the updated fields
            assert records[0].get_field_data('/')['_id'] == str(data_ids[0]['_id'])
            assert 'f1' not in records[0].get_field_data('/')
            assert records[0].get_field_data('/')['f2'] == 'Updated'
        elif full_record_not_found_behavior == 'SEND_TO_ERROR':
            sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1)
            assert len(wiretap.output_records) == 1, f"Expected 1 output record but found {len(wiretap.output_records)}"
            stage_errors = sdc_executor.get_stage_errors(pipeline, mongodb_atlas_cdc_origin)
            assert len(stage_errors) == 1, f"Expected 1 error record but found {len(stage_errors)}"
            assert stage_errors[0].error_code == 'MONGODB_ATLAS_57'
        else:
            pytest.fail('Not supported full_record_not_found_behavior.')
    finally:
        sdc_executor.stop_pipeline(pipeline)
        logger.info('Dropping %s database...', database)
        mongodb.engine.drop_database(database)


@pytest.mark.parametrize('read_changes_from', [
    'CHANGE_STREAM',
    'OPLOG'
])
@pytest.mark.parametrize('auto_flatten_nested_structures', [True, False])
def test_mongodb_atlas_cdc_auto_flatten_nested_documents(sdc_builder, sdc_executor, mongodb,
                                                         read_changes_from, auto_flatten_nested_structures):
    """
    Insert nested records into MongoDB Atlas and use the auto flatten nested structures property to read the records
    with the correct format with MongoDB Atlas CDC origin.

    The pipeline looks like:
        mongodb_atlas_cdc_origin >> wiretap
    """
    if read_changes_from == 'CHANGE_STREAM' and mongodb.version[0] < 4:
        pytest.skip("Initial offset in CHANGE STREAM mode is supported only by MongoDB 4.0 or newer")

    database = get_random_string(string.ascii_letters, 5)
    collection = get_random_string(string.ascii_letters, 5)

    data = [
        {'f1': 1, 'f2': {'g1': 'a', 'g2': 1234}, 'f3': 'Hello World!'},
        {'f1': 2, 'f2': [{'g1': 'a', 'g2': 1234}, {'g1': 'b', 'g2': 5678}], 'f3': 'Hello Moon!'}
    ]
    flatten_expected_data = [
        {'f1': 1, 'f2.g1': 'a', 'f2.g2': 1234, 'f3': 'Hello World!'},
        {'f1': 2, 'f2.0.g1': 'a', 'f2.0.g2': 1234, 'f2.1.g1': 'b', 'f2.1.g2': 5678, 'f3': 'Hello Moon!'}
    ]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_atlas_cdc_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_CDC_ORIGIN)
    mongodb_atlas_cdc_origin.set_attributes(read_changes_from=read_changes_from,
                                            include_namespaces=[f'{database}.{collection}'],
                                            auto_flatten_nested_structures=auto_flatten_nested_structures)

    # Configure MongoDB Atlas CDC to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_cdc_origin.tls_mode = 'NONE'
        mongodb_atlas_cdc_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_cdc_origin >> wiretap.destination

    pipeline = pipeline_builder.build(title=f'Test MongoDB Atlas CDC - Auto flatten nested documents'
                                            f'[auto_flatten_nested_documents={auto_flatten_nested_structures}]')\
        .configure_for_environment(mongodb)

    try:
        if read_changes_from == 'OPLOG':
            # Insert records into MongoDB Atlas
            _write_in_mongodb_atlas(mongodb, database, collection, data)

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        if read_changes_from == 'CHANGE_STREAM':
            # Insert records into MongoDB Atlas
            _write_in_mongodb_atlas(mongodb, database, collection, data)

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(data),  timeout_sec=120)
        sdc_executor.stop_pipeline(pipeline)

        # Verify the format of the record
        records = wiretap.output_records
        map_record = records[0].get_field_data('/')
        list_of_maps_record = records[1].get_field_data('/')
        if auto_flatten_nested_structures:
            assert flatten_expected_data[0] == {'f1': map_record['f1'],
                                                'f2.g1': map_record['f2.g1'],
                                                'f2.g2': map_record['f2.g2'],
                                                'f3': map_record['f3']}

            assert flatten_expected_data[1] == {'f1': list_of_maps_record['f1'],
                                                'f2.0.g1': list_of_maps_record['f2.0.g1'],
                                                'f2.0.g2': list_of_maps_record['f2.0.g2'],
                                                'f2.1.g1': list_of_maps_record['f2.1.g1'],
                                                'f2.1.g2': list_of_maps_record['f2.1.g2'],
                                                'f3': list_of_maps_record['f3']}
        else:
            assert data[0] == {'f1': map_record['f1'], 'f2': map_record['f2'], 'f3': map_record['f3']}
            assert data[1] == {'f1': list_of_maps_record['f1'],
                               'f2': list_of_maps_record['f2'],
                               'f3': list_of_maps_record['f3']}

    finally:
        logger.info('Dropping %s database...', database)
        mongodb.engine.drop_database(database)


@pytest.mark.parametrize('batch_limitation', [
    'BATCH_SIZE',
    'WAIT_TIME'
])
def test_mongodb_atlas_origin_max_batch_time(sdc_builder, sdc_executor, mongodb, batch_limitation):
    """
    Insert a large amount of records into MongoDB Atlas and use the batch size limitation and the wait batch time
    limitation to read the changes with MongoDB Atlas CDC origin.

    The pipeline looks like:
        mongodb_atlas_cdc_origin >> wiretap
    """
    database = get_random_string(string.ascii_letters, 5)
    collection = get_random_string(string.ascii_letters, 5)

    if batch_limitation == 'BATCH_SIZE':
        batch_size = 5
        max_wait_time = 120
    else:
        batch_size = 100
        max_wait_time = 25

    data = [{'f1': i} for i in range(30)]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_atlas_cdc_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_CDC_ORIGIN)
    mongodb_atlas_cdc_origin.set_attributes(read_changes_from='OPLOG',
                                            include_namespaces=[f'{database}.{collection}'],
                                            batch_size_in_records=batch_size,
                                            max_batch_wait_time_in_sec='${' + str(max_wait_time) + ' * SECONDS}')

    # Configure MongoDB Atlas CDC origin to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_cdc_origin.tls_mode = 'NONE'
        mongodb_atlas_cdc_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_cdc_origin >> wiretap.destination

    pipeline = pipeline_builder.build(f'Test MongoDB Atlas CDC - Max Batch Time[batch_limitation={batch_limitation}]')\
        .configure_for_environment(mongodb)

    try:
        # Write data in a MongoDB Atlas database
        _write_in_mongodb_atlas(mongodb, database, collection, data)

        # Start pipeline and verify the documents using snapshot.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        start = time.time()
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(data), timeout_sec=120)
        end = time.time()
        sdc_executor.stop_pipeline(pipeline)
        total_time = (end - start)

        if batch_limitation == 'WAIT_TIME':
            # Execution time is around +-2 seconds around the max batch wait time, which is what we expect it to wait
            assert (max_wait_time*1.3) > total_time > (max_wait_time*0.9)
        else:
            # Execution time is lower the max batch wait time
            assert total_time < max_wait_time

        records = wiretap.output_records
        assert len(records) == len(data)
        assert [data == {'f1': record.field['f1'].value} for record in records]

    finally:
        logger.info('Dropping %s database...', database)
        mongodb.engine.drop_database(database)


@sdc_min_version('6.0.0')
@pytest.mark.parametrize('read_changes_from', [
    'CHANGE_STREAM',
    'OPLOG'
])
def test_mongodb_atlas_cdc_origin_replace(sdc_builder, sdc_executor, mongodb, read_changes_from):
    """
    Update and replace two different documents inserted previously in MongoDB Atlas. Read only the record replaced in a 
    collection using Change Stream and Oplog, and confirm that MongoDB Atlas CDC origin read it.

    The pipeline looks like:
        mongodb_atlas_cdc_origin >> wiretap
    """
    if read_changes_from == 'CHANGE_STREAM' and mongodb.version[0] < 4:
        pytest.skip("Initial offset in CHANGE STREAM mode is supported only by MongoDB 4.0 or newer")

    database = get_random_string(string.ascii_letters, 5)
    collection = get_random_string(string.ascii_letters, 5)
    operation = 'REPLACE'

    data = [{'f1': i, 'f2': get_random_string(string.ascii_letters, 4), 'f3': get_random_string(string.ascii_letters, 2)} for i in range(3)]
    expected_data = {'f1': 1, 'f2':'Replaced'}
    _write_in_mongodb_atlas(mongodb, database, collection, data)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    mongodb_atlas_cdc_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_CDC_ORIGIN)
    mongodb_atlas_cdc_origin.set_attributes(read_changes_from=read_changes_from,
                                            operation_types=[operation],
                                            get_full_record_for_updates=True,
                                            include_namespaces=[f'{database}.{collection}'])

    # Configure MongoDB Atlas CDC to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_cdc_origin.tls_mode = 'NONE'
        mongodb_atlas_cdc_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_cdc_origin >> wiretap.destination

    pipeline = pipeline_builder.build(title=f'Test MongoDB Atlas CDC - Replace [read_change_from={read_changes_from}]')\
        .configure_for_environment(mongodb)

    try:
        if read_changes_from == 'OPLOG':
            # UPDATE a document
            mongodb_database = mongodb.engine[database]
            mongodb_collection = mongodb_database[collection]
            update_filter = {'f1': 0}
            new_value = {"$set": {'f2': 'Updated'}}
            mongodb_collection.update_one(update_filter, new_value)

            # REPLACE a document
            mongodb_database = mongodb.engine[database]
            mongodb_collection = mongodb_database[collection]
            replace_filter = {'f1': 1}
            new_value = copy.deepcopy(expected_data)
            mongodb_collection.replace_one(replace_filter, new_value)

        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        if read_changes_from == 'CHANGE_STREAM':
            # UPDATE a document
            mongodb_database = mongodb.engine[database]
            mongodb_collection = mongodb_database[collection]
            update_filter = {'f1': 0}
            new_value = {"$set": {'f2': 'Updated'}}
            mongodb_collection.update_one(update_filter, new_value)

            # REPLACE a document
            mongodb_database = mongodb.engine[database]
            mongodb_collection = mongodb_database[collection]
            replace_filter = {'f1': 1}
            new_value = copy.deepcopy(expected_data)
            mongodb_collection.replace_one(replace_filter, new_value)

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', 1, timeout_sec=120)
        sdc_executor.stop_pipeline(pipeline)

        # Verify we only read the records insert after the offset value
        records = wiretap.output_records
        assert len(records) == 1
        records = [{'f1': r.field['f1'], 'f2': r.field['f2']} for r in records]
        # Assert replace record
        assert records[0] == expected_data

    finally:
        logger.info('Dropping %s database...', database)
        mongodb.engine.drop_database(database)


def _write_operations(mongodb, database, collection, data):
    # Insert data into the database
    _write_in_mongodb_atlas(mongodb, database, collection, data)

    # Get the _id from MongoDB Atlas
    data_ids = _get_documents_id(mongodb, database, collection)
    data_to_insert = []
    data_to_update = {'f1': 1, 'f2': 'Updated'}
    data_to_delete = {'f1': 0}

    # Update second element of the data already inserted into the database
    _write_in_mongodb_atlas(mongodb, database, collection, data_to_update, 'UPDATE')

    # Delete first element of the data already inserted into the database
    _write_in_mongodb_atlas(mongodb, database, collection, data_to_delete, 'DELETE')

    for doc, data_id in zip(data, sorted(data_ids, key=itemgetter('f1'))):
        data_to_insert.append({'_id': str(data_id['_id']), 'f1': doc['f1'], 'f2': doc['f2'], 'f3': doc['f3']})
        if data_id['f1'] == data_to_update['f1']:
            data_to_update['_id'] = str(data_id['_id'])
        if data_id['f1'] == data_to_delete['f1']:
            data_to_delete['_id'] = str(data_id['_id'])

    return data_to_insert, data_to_update, data_to_delete


def _get_documents_id(mongodb, database, collection):
    logger.info('Get the _id of the document in the %s collection using PyMongo...', collection)
    mongodb_database = mongodb.engine[database]
    mongodb_collection = mongodb_database[collection]
    documents = mongodb_collection.find()
    return [{'f1': document['f1'], '_id': document['_id']} for document in documents]


def _write_in_mongodb_atlas(mongodb, database, collection, raw_data, operation='INSERT'):
    # MongoDB Atlas and PyMongo add '_id' to the dictionary entries e.g. docs_in_database
    # when used for inserting in collection. Hence the deep copy.
    data = copy.deepcopy(raw_data)

    # Create documents in MongoDB Atlas using PyMongo.
    # First a database is created. Then a collection is created inside that database.
    # Then documents are created in that collection.
    logger.info('Adding documents into %s collection using PyMongo...', collection)
    mongodb_database = mongodb.engine[database]
    mongodb_collection = mongodb_database[collection]
    if operation == 'INSERT':
        if isinstance(data, list):
            insert_list = [mongodb_collection.insert_one(doc) for doc in data]
            assert len(insert_list) == len(data)
        else:
            mongodb_collection.insert_one(data)
    elif operation == 'UPDATE':
        update_filter = {'f1': data['f1']}
        new_value = {"$set": {'f2': data['f2']}}
        mongodb_collection.update_one(update_filter, new_value)
    else:
        mongodb_collection.delete_one(data)

@pytest.mark.parametrize('read_changes_from', [
    'CHANGE_STREAM',
])
def test_mongodb_atlas_origin_el_support(sdc_builder, sdc_executor, mongodb, read_changes_from):
    """
    Test EL fields are working: Read from MongoDB Atlas the records inserted in a collection using Change Stream, and confirm that
    MongoDB Atlas CDC origin reads them, when the EL fields configured.

    The pipeline looks like:
        mongodb_atlas_cdc_origin >> wiretap
    """
    if read_changes_from == 'CHANGE_STREAM' and mongodb.version[0] < 4:
        pytest.skip("Initial offset in CHANGE STREAM mode is supported only by MongoDB 4.0 or newer")

    database = get_random_string(string.ascii_letters, 5)
    collection = get_random_string(string.ascii_letters, 5)

    data = [{'f1': i, 'f2': get_random_string(string.ascii_letters, 4)} for i in range(5)]

    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    time_in_el = '${2000 * SECONDS}'
    mongodb_atlas_cdc_origin = pipeline_builder.add_stage(name=MONGODB_ATLAS_CDC_ORIGIN)
    mongodb_atlas_cdc_origin.set_attributes(read_changes_from=read_changes_from,
                                            include_namespaces=[f'{database}.{collection}'],
                                            max_connection_idle_time_in_ms=time_in_el,
                                            max_connection_lifetime_in_ms=time_in_el,
                                            max_connection_wait_time_in_ms=time_in_el,
                                            socket_connect_timeout_in_ms=time_in_el,
                                            socket_read_timeout_in_ms=time_in_el,
                                            server_selection_timeout_in_ms=time_in_el,
                                            )

    # Configure MongoDB Atlas CDC to connect to old MongoDB version
    if not mongodb.atlas:
        mongodb_atlas_cdc_origin.tls_mode = 'NONE'
        mongodb_atlas_cdc_origin.authentication_method = 'NONE'

    wiretap = pipeline_builder.add_wiretap()

    mongodb_atlas_cdc_origin >> wiretap.destination

    pipeline = pipeline_builder.build(title=f'Test MongoDB Atlas CDC [read_change_from={read_changes_from}]')\
        .configure_for_environment(mongodb)

    try:
        # Start pipeline and verify the documents using wiretap.
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)

        # if read_changes_from == 'CHANGE_STREAM':
        _write_in_mongodb_atlas(mongodb, database, collection, data)

        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', len(data))
        sdc_executor.stop_pipeline(pipeline)

        # Verify we only read the records insert after the offset value
        records = [{'f1': r.field['f1'], 'f2': r.field['f2']} for r in wiretap.output_records]
        assert len(records) == len(data)
        assert sorted(records, key=itemgetter('f1')) == data

    except Exception as e:
        error_message = e.message
        assert False, f"The pipeline shouldn't fail. error:{error_message}"

    finally:
        logger.info('Dropping %s database...', database)
        mongodb.engine.drop_database(database)
