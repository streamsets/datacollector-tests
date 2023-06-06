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

import json
import pytest
import time
from streamsets.sdk import sdc_api
from streamsets.testframework.markers import aerospike, sdc_min_version
from streamsets.testframework.utils import get_random_string

AEROSPIKE_CLIENT_DESTINATION = 'Aerospike Client Destination'

pytestmark = [aerospike, sdc_min_version('5.6.0')]


@pytest.mark.parametrize('input_data', [
    { 'integer': 100 },
    { 'double': 0.10 },
    { 'string': 'Text' },
    { 'boolean': True },
])
def test_insert_scalar_types(sdc_builder, sdc_executor, aerospike, input_data):
    record_key = get_random_string()
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(
            data_format='JSON', raw_data=json.dumps(input_data), stop_after_first_batch=True)
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key=record_key,
            on_record_error='STOP_PIPELINE')

    dev_raw_data_source >> aerospike_destination
    producer_dest_pipeline = builder.build(title='Aerospike Destination Scalar Types Test').configure_for_environment(aerospike)

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()
        aerospike_key = ('test', None, record_key)
        (_, _, bins) = aerospike.engine.get(aerospike_key)
        assert all(bins[k] == v for k,v in input_data.items())
    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)


def test_insert_binary_type(sdc_builder, sdc_executor, aerospike):
    record_key = get_random_string()
    builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = builder.add_stage('Dev Data Generator').set_attributes(
            records_to_be_generated=1, 
            fields_to_generate=[{
                    "type": "BYTE_ARRAY",
                    "precision": 10,
                    "scale": 2,
                    "fieldAttributes": [],
                    "field": "binary"
            }])
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key=record_key)
    wiretap = builder.add_wiretap()

    dev_data_generator >> [aerospike_destination, wiretap.destination]

    producer_dest_pipeline = builder.build(title='Aerospike Destination Binary Type Test').configure_for_environment(aerospike)

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()
        (_, _, bins) = aerospike.engine.get(('test', None, record_key))
        output_records = wiretap.output_records
        assert output_records[0].field['binary'] == bins['binary']
    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)


@pytest.mark.parametrize('input_data', [
    {'list_int': [1,2,3]},
    {'list_string': ['a','b','c']},
    {'map': {'a': 10, 'b': True, 'c': 'String'}},
    {'map_of_maps': {'a': 10, 'b': True, 'map': {'a': 10, 'b': [1,2,3] }}},
    {'list_of_maps': [{'a':10}, {'b':20}, {'c': {'d': [1,2,3]}}]}
])
def test_insert_complex_types(sdc_builder, sdc_executor, aerospike, input_data):
    record_key = get_random_string()
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(
            data_format='JSON', raw_data=json.dumps(input_data), stop_after_first_batch=True)
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key=record_key)

    dev_raw_data_source >> aerospike_destination
    producer_dest_pipeline = builder.build(title='Aerospike Destination Complex Types Test').configure_for_environment(aerospike)

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()
        aerospike_key = ('test', None, record_key)
        (_, _, bins) = aerospike.engine.get(aerospike_key)
        assert all(bins[k] == v for k,v in input_data.items())
    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)


@pytest.mark.parametrize('aerospike_data, input_data, expected_data', [
    ( { 'integer': 5 }, { 'integer': 10 }, { 'integer': 10 } ),
    ( { 'boolean': True }, { 'boolean': False }, { 'boolean': False } ),
    ( { 'text': 'Verstappen' }, { 'text': 'Alonso' }, { 'text': 'Alonso' } ),
    ( { 'double': 0.1 }, { 'double': 0.5 }, { 'double': 0.5 } ),
])
def test_create_or_update_scalar(sdc_builder, sdc_executor, aerospike, aerospike_data, input_data, expected_data):
    record_key = get_random_string()
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(
            data_format='JSON', raw_data=json.dumps(input_data), stop_after_first_batch=True)
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key=record_key,
            existing_record_action='UPDATE')

    dev_raw_data_source >> aerospike_destination
    producer_dest_pipeline = builder.build(title='Aerospike Destination Update Scalar Types Test').configure_for_environment(aerospike)

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        aerospike_key = ('test', None, record_key)
        aerospike.engine.put(aerospike_key, aerospike_data)

        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()

        (_, _, bins) = aerospike.engine.get(aerospike_key)
        assert bins.keys() == expected_data.keys()
        assert all(bins[k] == v for k,v in expected_data.items())
    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)


@pytest.mark.parametrize('aerospike_data, input_data, expected_data', [
    ( {  }, { 'integer': 10 }, { 'integer': 10 } ),
    ( { 'integer': 5 }, { 'integer': 10 }, { 'integer': 5 } ),  # As it is set to CREATE_ONLY, the update won't work
])
def test_create_only_scalar(sdc_builder, sdc_executor, aerospike, aerospike_data, input_data, expected_data):
    record_key = get_random_string()
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(
            data_format='JSON', raw_data=json.dumps(input_data), stop_after_first_batch=True)
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key=record_key,
            existing_record_action='CREATE_ONLY',
            merge_map_bins=False,
            append_list_bins=False)
    wiretap = builder.add_wiretap()

    dev_raw_data_source >> [aerospike_destination, wiretap.destination]
    producer_dest_pipeline = builder.build(title='Aerospike Destination Create Only Scalar Types Test').configure_for_environment(aerospike)

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        aerospike_key = ('test', None, record_key)
        if len(aerospike_data) > 0:
            aerospike.engine.put(aerospike_key, aerospike_data)

        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()

        (_, _, bins) = aerospike.engine.get(aerospike_key)
        assert bins.keys() == input_data.keys()

        # As the write policy is set to CREATE_ONLY, the new value was not written
        assert all(bins[k] == v for k,v in expected_data.items())
    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)


@pytest.mark.parametrize('append_list_bins, aerospike_data, input_data, expected_data', [
    ( True, { 'list': [] }, { 'list': [4,5,6] }, { 'list': [4,5,6] } ),
    ( True, { 'list': [1,2,3] }, { 'list': [4,5,6] }, { 'list': [1,2,3,4,5,6] } ),
    ( True, { 'list': [1,2,3] }, { 'list': [4,5,6], 'int': 10 }, { 'list': [1,2,3,4,5,6], 'int': 10 } ),           # Append in list while adding new bin
    ( True, { 'list': [1,2,3], 'int': 0 }, { 'list': [4,5,6], 'int': 10 }, { 'list': [1,2,3,4,5,6], 'int': 10 } ), # Append in list while updating new bin
    ( True, { 'map': {'list': [1,2,3]} }, { 'map': {'list': [4,5,6]} }, { 'map': {'list': [1,2,3,4,5,6]} }),       # Append in list in map
    ( True, { 'map': {'a': 10} }, { 'map': {'list': [1,2,3]} }, { 'map': {'a': 10, 'list': [1,2,3]} }),            # Append in list in map

    ( False, { 'list': [] }, { 'list': [4,5,6] }, { 'list': [4,5,6] } ),
    ( False, { 'list': [1,2,3] }, { 'list': [4,5,6] }, { 'list': [4,5,6] } ),
    ( False, { 'list': [1,2,3] }, { 'list': [4,5,6], 'int': 10 }, { 'list': [4,5,6], 'int': 10 } ),           # Append in list while adding new bin
    ( False, { 'list': [1,2,3], 'int': 0 }, { 'list': [4,5,6], 'int': 10 }, { 'list': [4,5,6], 'int': 10 } ), # Append in list while updating new bin
    ( False, { 'map': {'list': [1,2,3]} }, { 'map': {'list': [4,5,6]} }, { 'map': {'list': [4,5,6]} }),       # Append in list in map
    ( False, { 'map': {'a': 10} }, { 'map': {'list': [1,2,3]} }, { 'map': {'a': 10, 'list': [1,2,3]} }),            # Append in list in map

])
def test_append_list(sdc_builder, sdc_executor, aerospike, append_list_bins, aerospike_data, input_data, expected_data):

    record_key = get_random_string()
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(
            data_format='JSON', raw_data=json.dumps(input_data), stop_after_first_batch=True)
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key=record_key,
            existing_record_action='UPDATE',
            append_list_bins=append_list_bins)

    dev_raw_data_source >> aerospike_destination
    producer_dest_pipeline = builder.build(title='Aerospike Destination Scalar Types Test').configure_for_environment(aerospike)

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        aerospike_key = ('test', None, record_key)
        aerospike.engine.put(aerospike_key, aerospike_data)

        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()

        (_, _, bins) = aerospike.engine.get(aerospike_key)
        assert bins.keys() == expected_data.keys()
        assert all(bins[k] == v for k,v in expected_data.items())
    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)


def test_update_map(sdc_builder, sdc_executor, aerospike):
    data_in_aerospike = {
            'list': {'a': 10, 'b': 20}
    }
    input_data = {
            'list': {'b': 30, 'c': 40}
    }

    record_key = get_random_string()
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(
            data_format='JSON', raw_data=json.dumps(input_data), stop_after_first_batch=True)
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key=record_key,
            existing_record_action='UPDATE',
            merge_map_bins=False)

    dev_raw_data_source >> aerospike_destination
    producer_dest_pipeline = builder.build(title='Aerospike Destination Scalar Types Test').configure_for_environment(aerospike)

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        aerospike_key = ('test', None, record_key)
        aerospike.engine.put(aerospike_key, data_in_aerospike)

        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()

        (_, _, bins) = aerospike.engine.get(aerospike_key)
        assert bins.keys() == input_data.keys()
        assert all(bins[k] == v for k,v in input_data.items())
    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)


@pytest.mark.parametrize('aerospike_data, input_data, expected_data', [
    (   # Adding a new key
        {'a': 10},
        {'b': 20},
        {'a': 10, 'b': 20}
    ),
    (   # Adding a new key and updating other
        {'a': 10, 'b': 20},
        {'b': 30, 'c': 40},
        {'a': 10, 'b': 30, 'c':40}
    ),
    (   # Adding new key with a map
        {'a': 10},
        {'b': {'ba': 20}},
        {'a': 10, 'b': {'ba': 20}}
    ),
    (   # Updating a value of an existing key
        {'a': 10, 'b': 10},
        {'b': 20},
        {'a': 10, 'b': 20}
    ),
    (   # Adding a non existing key and adding and updating values for a nested map
        {'a': 10, 'b': {'ba': '10', 'bb': 20}},
        {'b': {'bb': 30, 'bc': 40}, 'c': 50},
        {'a': 10, 'b': {'ba': '10', 'bb': 30, 'bc': 40}, 'c': 50}
    ),
    (   # Updating a deeply nested value
        {'a': {'b': {'c':{'d':{'e': 10, 'f': {'g':20}}}}}},
        {'a': {'b': {'c':{'d':{'e': 20, 'f': {'g':20}}}}}},
        {'a': {'b': {'c':{'d':{'e': 20, 'f': {'g':20}}}}}},
    ),
    (   # Updating a deeply nested value while the rest remains the same
        {'a': {'b': {'c':{'d':{'e': 10, 'f': {'g':20}}}}}},
        {'a': {'b': {'c':{'d':{'f': {'g':30}}}}}},
        {'a': {'b': {'c':{'d':{'e': 10, 'f': {'g':30}}}}}},
    ),
    (   # 'b' field won't be updated as the merge will fail as it is not a map
        # This makes all the record operations to fail
        {'a': 10, 'b': 10, 'c': {'ca': 30}},
        {'a': 20, 'b': {'ba': 10, 'bb': 20}, 'c': {'cb': 40, 'cc': 50}},
        {'a': 10, 'b': 10, 'c': {'ca': 30}}  
    ),
    (   # In this case b will be replaced by the new value '20' there is no error as
        # The new value is not a map and so the connector won't try to merge it.
        {'a': 10, 'b': {'ba': 20}},
        {'b': 20},
        {'a': 10, 'b': 20}
    ),
])
def test_merge_nested_map(sdc_builder, sdc_executor, aerospike, aerospike_data, input_data, expected_data):
    record_key = get_random_string()
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(
            data_format='JSON', raw_data=json.dumps({ 'map': input_data }), stop_after_first_batch=True)
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key=record_key,
            existing_record_action='UPDATE')

    dev_raw_data_source >> aerospike_destination
    producer_dest_pipeline = builder.build(title='Aerospike Destination Scalar Types Test').configure_for_environment(aerospike)

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        aerospike_key = ('test', None, record_key)
        aerospike.engine.put(aerospike_key, { 'map': aerospike_data })

        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()

        (_, _, bins) = aerospike.engine.get(aerospike_key)
        assert list(bins.keys()) == ['map']
        assert bins['map'] == expected_data

    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)


@pytest.mark.parametrize('durable_delete', [True, False])
@pytest.mark.skip(reason='No way to test it yet as durable deletes shows its effect accross cold starts')
def test_durable_delete(sdc_builder, sdc_executor, aerospike, durable_delete):
    ''' As we are setting the value of the 'value' bin to null and as this is the only bin that the
    aerospike record has, the entire record will be removed '''

    record_key = get_random_string()
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(
            data_format='JSON', raw_data=json.dumps({ 'value': None }), stop_after_first_batch=True)
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key=record_key,
            existing_record_action='UPDATE',
            durable_delete=durable_delete)

    dev_raw_data_source >> aerospike_destination
    producer_dest_pipeline = builder.build(title='Aerospike Destination Scalar Types Test').configure_for_environment(aerospike)

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        aerospike_key = ('test', None, record_key)
        aerospike.engine.put(aerospike_key, { 'value': 100 })

        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()
        (_, _, bins) = aerospike.engine.get(aerospike_key)
    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)


@pytest.mark.parametrize('expiration_mode, record_ttl', [
    ('NEVER_EXPIRE',   0),
    ('DEFAULT',        0),
    ('EXPIRATION_TTL', 1000),
    ('EXPIRATION_TTL', 5000),
    ('NOT_CHANGE_TTL', 0),  # Don't change TTL when record is updated
])
def test_expiration_mode(sdc_builder, sdc_executor, aerospike, expiration_mode, record_ttl):
    record_key = get_random_string()
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(
            data_format='JSON', raw_data=json.dumps({ 'value': 1000 }), stop_after_first_batch=True)
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key=record_key,
            existing_record_action='UPDATE',
            expiration_mode = expiration_mode,
            record_ttl = record_ttl)

    dev_raw_data_source >> aerospike_destination
    producer_dest_pipeline = builder.build(title='Aerospike Destination TTL Test').configure_for_environment(aerospike)

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        aerospike_key = ('test', None, record_key)
        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()
        (_, meta, _) = aerospike.engine.get(aerospike_key)

        if expiration_mode == 'NEVER_EXPIRE':
            assert meta['ttl'] == 4294967295
            time.sleep(5)
            (_, meta, _) = aerospike.engine.get(aerospike_key)
            assert meta['ttl'] == 4294967295

            sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()
            (_, meta_after_update, _) = aerospike.engine.get(aerospike_key)
            assert meta_after_update['ttl'] == 4294967295

        elif expiration_mode == 'DEFAULT':
            assert 30 * 24 * 3600 - 2 < meta['ttl'] <= 30 * 24 * 3600
            time.sleep(5)
            (_, meta, _) = aerospike.engine.get(aerospike_key)
            assert  30 * 24 * 3600 - 7 < meta['ttl'] <= 30 * 24 * 3600 - 5

            # Reset ttl after update
            sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()
            (_, meta_after_update, _) = aerospike.engine.get(aerospike_key)
            assert meta['ttl'] < meta_after_update['ttl']
        elif expiration_mode == 'EXPIRATION_TTL':
            assert record_ttl - 2 < meta['ttl'] <= record_ttl
            time.sleep(5)
            (_, meta, _) = aerospike.engine.get(aerospike_key)
            assert record_ttl - 7 < meta['ttl'] <= record_ttl - 5

            # Reset ttl after update
            sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()
            (_, meta_after_update, _) = aerospike.engine.get(aerospike_key)
            assert meta['ttl'] < meta_after_update['ttl']
        elif expiration_mode == 'NOT_CHANGE_TTL':
            # Record is created with the default TTL but it is not being reset on update
            assert 30 * 24 * 3600 - 2 < meta['ttl'] <= 30 * 24 * 3600
            time.sleep(5)
            (_, meta_before_update, _) = aerospike.engine.get(aerospike_key)
            assert  30 * 24 * 3600 - 7 < meta_before_update['ttl'] <= 30 * 24 * 3600 - 5

            sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()
            (_, meta_after_update, _) = aerospike.engine.get(aerospike_key)

            assert meta_after_update['ttl'] < meta_before_update['ttl']
    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)


@pytest.mark.parametrize('generation_policy, generation, server_generation, record_is_updated', [
    ('NONE',             0, 1, True),
    ('NONE',             5, 1, True),
    ('EXPECT_GEN_EQUAL', 5, 5, True),
    ('EXPECT_GEN_EQUAL', 5, 1, False),
    ('EXPECT_GEN_GT',    5, 4, True),
    ('EXPECT_GEN_GT',    5, 5, False),
    ('EXPECT_GEN_GT',    5, 6, False),
])
def test_generation_policy(sdc_builder, sdc_executor, aerospike, generation_policy, generation, server_generation, record_is_updated):
    record_key = get_random_string()
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(
            data_format='JSON', raw_data=json.dumps({ 'value': 10_000 }), stop_after_first_batch=True)
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key=record_key,
            existing_record_action='UPDATE',
            generation_policy=generation_policy,
            generation=generation)
    wiretap = builder.add_wiretap()

    dev_raw_data_source >> [aerospike_destination, wiretap.destination]
    producer_dest_pipeline = builder.build(title='Aerospike Destination Generation Policy Test').configure_for_environment(aerospike)

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        aerospike_key = ('test', None, record_key)
        for _ in range(server_generation):
            aerospike.engine.put(aerospike_key, { 'value': 100 })

        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()

        (_, _, bins) = aerospike.engine.get(aerospike_key)
        if record_is_updated:
            assert bins['value'] == 10_000
        else:
            error_records = wiretap.error_records
            assert bins['value'] == 100
            assert error_records[0].header.values['aerospike-error-result-code'] == '3'  # AS_ERR_GENERATION
    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)


@pytest.mark.parametrize('existing_record_action, record_exists_in_server, success, expected_error_code', [
    ('CREATE_ONLY',  False, True,   None),
    ('REPLACE',      False, True,   None),
    ('REPLACE',      True,  True,   None),
    ('REPLACE_ONLY', True,  True,   None),
    ('UPDATE',       False, True,   None),
    ('UPDATE',       True,  True,   None),
    ('UPDATE_ONLY',  True,  True,   None),
    ('CREATE_ONLY',  True,  False,  '5'),  # AS_ERR_RECORD_EXISTS # AS_ERR_NOT_FOUND
    ('REPLACE_ONLY', False, False,  '2'),  # AS_ERR_NOT_FOUND
    ('UPDATE_ONLY',  False, False,  '2'),  # AS_ERR_NOT_FOUND
])
def test_existing_record_action(sdc_builder, sdc_executor, aerospike, existing_record_action, record_exists_in_server, success, expected_error_code):
    record_key = get_random_string()
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(
            data_format='JSON', raw_data=json.dumps({ 'value': 10_000 }), stop_after_first_batch=True)
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key=record_key,
            existing_record_action=existing_record_action,
            merge_map_bins=False,
            append_list_bins=False)
    wiretap = builder.add_wiretap()

    dev_raw_data_source >> [aerospike_destination, wiretap.destination]
    producer_dest_pipeline = builder.build(title='Aerospike Destination Existing Record Test').configure_for_environment(aerospike)

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        aerospike_key = ('test', None, record_key)
        if record_exists_in_server:
            aerospike.engine.put(aerospike_key, { 'value': 100, 'other': 200 })

        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()

        try:
            (_, _, bins) = aerospike.engine.get(aerospike_key)
        except Exception:
            bins = None

        if success:
            if record_exists_in_server and 'REPLACE' in existing_record_action:
                assert 'other' not in bins
            elif record_exists_in_server and 'UPDATE' in existing_record_action:
                assert 'other' in bins
                assert bins['other'] == 200
            assert bins['value'] == 10_000
        else:
            error_records = wiretap.error_records
            if record_exists_in_server:
                assert bins['value'] == 100
            else:
                assert bins == None
            assert error_records[0].header.values['aerospike-error-result-code'] == expected_error_code
    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)


@pytest.mark.parametrize('existing_record_action, expected_error, merge_map_bins, append_list_bins', [
    ('REPLACE',      'AEROSPIKE_14', True, False),
    ('REPLACE_ONLY', 'AEROSPIKE_14', True, False),
    ('CREATE_ONLY',  'AEROSPIKE_14', True, False),
    ('REPLACE',      'AEROSPIKE_15', False, True),
    ('REPLACE_ONLY', 'AEROSPIKE_15', False, True),
    ('CREATE_ONLY',  'AEROSPIKE_15', False, True),
    ('REPLACE',      'AEROSPIKE_14', True, True),
    ('REPLACE_ONLY', 'AEROSPIKE_14', True, True),
    ('CREATE_ONLY',  'AEROSPIKE_14', True, True),
])
def test_merge_append_with_no_update_policy(sdc_builder, sdc_executor, aerospike, existing_record_action, expected_error, merge_map_bins, append_list_bins):
    record_key = get_random_string()
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(
            data_format='JSON', raw_data=json.dumps({ 'map': {'b': 20} }), stop_after_first_batch=True)
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key=record_key,
            existing_record_action=existing_record_action,
            merge_map_bins=merge_map_bins,
            append_list_bins=append_list_bins)
    wiretap = builder.add_wiretap()

    dev_raw_data_source >> [aerospike_destination, wiretap.destination]
    producer_dest_pipeline = builder.build(title='Aerospike Destination Merge No Update Test').configure_for_environment(aerospike)

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        aerospike_key = ('test', None, record_key)
        aerospike.engine.put(aerospike_key, { 'map': {'a': 10} })

        with pytest.raises(sdc_api.StartError) as error:
            sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()
        assert expected_error in error.value.message

        (_, _, bins) = aerospike.engine.get(aerospike_key)
        assert list(bins.keys()) == ['map']
        assert bins['map'] == {'a': 10}

    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)


@pytest.mark.parametrize('input_data', [
    ( {'z': 10, 'g': 20, 'a': 30} ),
    ( {'z': 10, 'g': {'h': 20, 'j': 30, 'a': 40}, 'a': 30} ),
    ( {'z': 10, 'j': {'h': 20, 'j': 30, 'a': 40}, 'h': 30} ),
])
def test_ordered_maps(sdc_builder, sdc_executor, aerospike, input_data):
    ''' All maps should be sorted by key '''
    record_key = get_random_string()
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(
            data_format='JSON', raw_data=json.dumps({ 'map': input_data }), stop_after_first_batch=True)
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key=record_key,
            existing_record_action='UPDATE')
    wiretap = builder.add_wiretap()

    dev_raw_data_source >> [aerospike_destination, wiretap.destination]
    producer_dest_pipeline = builder.build(title='Aerospike Destination Ordered Maps Test').configure_for_environment(aerospike)

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        aerospike_key = ('test', None, record_key)
        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()

        (_, _, bins) = aerospike.engine.get(aerospike_key)
        assert [*bins['map']] == sorted([*input_data])
        for k,v in input_data.items():
            if type(v) == map:
                assert [*bins['map'][k]] == sorted([*input_data[k]])
        assert bins['map'] == input_data
    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)


def test_error_in_single_record(sdc_builder, sdc_executor, aerospike):
    builder = sdc_builder.get_pipeline_builder()
    input_batch="""
    {"key": 10, "data": 1000}
    {"key": 20, "data": 1000}
    {"key": 30, "data": 1000}
    {"key": 40, "data": 1000}
    {"key": 50, "data": 1000}
    """
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(
            data_format='JSON', raw_data=input_batch, stop_after_first_batch=True)
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key='${record:value("/key")}',
            existing_record_action='UPDATE_ONLY')  # The records that doesn't exists will fail
    wiretap = builder.add_wiretap()

    dev_raw_data_source >> [aerospike_destination, wiretap.destination]
    producer_dest_pipeline = builder.build(title='Aerospike Destination Error Single Record Test').configure_for_environment(aerospike)

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        aerospike.engine.put(('test', None, 10), {'key': 10, 'data': 10})
        aerospike.engine.put(('test', None, 30), {'key': 30, 'data': 10})
        aerospike.engine.put(('test', None, 40), {'key': 40, 'data': 10})
        aerospike.engine.put(('test', None, 50), {'key': 50, 'data': 10})

        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()

        error_records = wiretap.error_records
        assert len(error_records) == 1
        assert error_records[0].header.values['aerospike-error-result-code'] == '2'

        (_, _, bins) = aerospike.engine.get(('test', None, 10))
        assert bins['key'] == 10 and bins['data'] == 1000
        (_, _, bins) = aerospike.engine.get(('test', None, 30))
        assert bins['key'] == 30 and bins['data'] == 1000
        (_, _, bins) = aerospike.engine.get(('test', None, 40))
        assert bins['key'] == 40 and bins['data'] == 1000
        (_, _, bins) = aerospike.engine.get(('test', None, 50))
        assert bins['key'] == 50 and bins['data'] == 1000

    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)


def test_list_map_root_field(sdc_builder, sdc_executor, aerospike):
    builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = builder.add_stage('Dev Data Generator').set_attributes(
        root_field_type='LIST_MAP',
        records_to_be_generated=1,
        fields_to_generate=[
            {
                "type": 'INTEGER',
                "precision": 10,
                "scale": 2,
                "fieldAttributes": [],
                "field": "key"
            },
            {
                "type": "ADDRESS_STREET_ADDRESS",
                "precision": 10,
                "scale": 2,
                "fieldAttributes": [],
                "field": "payload"
            }
        ])
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key='${record:value("/key")}')
    wiretap = builder.add_wiretap()

    dev_data_generator >> [aerospike_destination, wiretap.destination]
    producer_dest_pipeline = builder.build(title='Aerospike Destination Key EL Test').configure_for_environment(aerospike)

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()
        output_records = wiretap.output_records
        for output_record in output_records:
            generated_key = output_record.field['key'].value
            payload = output_record.field['payload'].value

            (_, _, bins) = aerospike.engine.get(('test', None, generated_key))
            assert bins['payload'] == payload

    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)


def test_list_map_field(sdc_builder, sdc_executor, aerospike):
    builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = builder.add_stage('Dev Data Generator').set_attributes(
        records_to_be_generated=1,
        fields_to_generate=[
            {
                "type": 'INTEGER',
                "precision": 10,
                "scale": 2,
                "fieldAttributes": [],
                "field": "key"
            },
            {
                "type": "ADDRESS_STREET_ADDRESS",
                "precision": 10,
                "scale": 2,
                "fieldAttributes": [],
                "field": "payload"
            }
        ])
    jython = builder.add_stage('Jython Evaluator')
    jython.script = """
for record in sdc.records:
  try:
    record.value['listmap'] = sdc.createMap(True)
    record.value['listmap']['b'] = 200
    record.value['listmap']['a'] = 100
    sdc.output.write(record)
  except Exception as e:
    sdc.error.write(record, str(e))
    """
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key='${record:value("/key")}')
    wiretap = builder.add_wiretap()

    dev_data_generator >> jython >> [aerospike_destination, wiretap.destination]
    producer_dest_pipeline = builder.build(title='Aerospike Destination Key EL Test').configure_for_environment(aerospike)

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()
        output_records = wiretap.output_records
        for output_record in output_records:
            generated_key = output_record.field['key'].value
            payload = output_record.field['payload'].value
            list_map = output_record.field['listmap']

            (_, _, bins) = aerospike.engine.get(('test', None, generated_key))
            assert bins['payload'] == payload
            assert bins['listmap'] == list_map
    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)


def test_check_wrong_username(sdc_builder, sdc_executor, aerospike):
    if not aerospike.use_authentication:
        pytest.skip('This test only applies when using credentials')
    record_key = get_random_string()
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(
            data_format='JSON', raw_data=json.dumps({ 'map': {'b': 20} }), stop_after_first_batch=True)
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key=record_key)
    wiretap = builder.add_wiretap()

    dev_raw_data_source >> [aerospike_destination, wiretap.destination]
    producer_dest_pipeline = builder.build(title='Aerospike Destination Merge No Update Test').configure_for_environment(aerospike)
    aerospike_destination.user = 'fake_user'

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        aerospike_key = ('test', None, record_key)
        aerospike.engine.put(aerospike_key, { 'map': {'a': 10} })

        with pytest.raises(sdc_api.StartError) as error:
            sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()
        assert 'AEROSPIKE_00' in error.value.message
        assert 'Login failed' in error.value.message

        (_, _, bins) = aerospike.engine.get(aerospike_key)
        assert list(bins.keys()) == ['map']
        assert bins['map'] == {'a': 10}

    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)


def test_check_wrong_password(sdc_builder, sdc_executor, aerospike):
    if not aerospike.use_authentication:
        pytest.skip('This test only applies when using credentials')
    record_key = get_random_string()
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(
            data_format='JSON', raw_data=json.dumps({ 'map': {'b': 20} }), stop_after_first_batch=True)
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key=record_key)
    wiretap = builder.add_wiretap()

    dev_raw_data_source >> [aerospike_destination, wiretap.destination]
    producer_dest_pipeline = builder.build(title='Aerospike Destination Merge No Update Test').configure_for_environment(aerospike)
    aerospike_destination.password = 'fake_pass'

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        aerospike_key = ('test', None, record_key)
        aerospike.engine.put(aerospike_key, { 'map': {'a': 10} })

        with pytest.raises(sdc_api.StartError) as error:
            sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()
        assert 'AEROSPIKE_00' in error.value.message
        assert 'Login failed' in error.value.message

        (_, _, bins) = aerospike.engine.get(aerospike_key)
        assert list(bins.keys()) == ['map']
        assert bins['map'] == {'a': 10}

    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)


def test_check_wrong_use_authentication(sdc_builder, sdc_executor, aerospike):
    if not aerospike.use_authentication:
        pytest.skip('This test only applies when using credentials')
    record_key = get_random_string()
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(
            data_format='JSON', raw_data=json.dumps({ 'map': {'b': 20} }), stop_after_first_batch=True)
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key=record_key)
    wiretap = builder.add_wiretap()

    dev_raw_data_source >> [aerospike_destination, wiretap.destination]
    producer_dest_pipeline = builder.build(title='Aerospike Destination Merge No Update Test').configure_for_environment(aerospike)
    aerospike_destination.use_authentication = False

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        aerospike_key = ('test', None, record_key)
        aerospike.engine.put(aerospike_key, { 'map': {'a': 10} })

        with pytest.raises(sdc_api.StartError) as error:
            sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()
        assert 'AEROSPIKE_00' in error.value.message
        assert 'Node name is null' in error.value.message  # Seems unrelated but it is the error message that the aerospike client provides

        (_, _, bins) = aerospike.engine.get(aerospike_key)
        assert list(bins.keys()) == ['map']
        assert bins['map'] == {'a': 10}

    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)


def test_check_wrong_use_tls_with_tls_name_set(sdc_builder, sdc_executor, aerospike):
    if not aerospike.use_tls:
        pytest.skip('This test only applies when using TLS')
    record_key = get_random_string()
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(
            data_format='JSON', raw_data=json.dumps({ 'map': {'b': 20} }), stop_after_first_batch=True)
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key=record_key)
    wiretap = builder.add_wiretap()

    dev_raw_data_source >> [aerospike_destination, wiretap.destination]
    producer_dest_pipeline = builder.build(title='Aerospike Destination Merge No Update Test').configure_for_environment(aerospike)
    aerospike_destination.use_tls = False

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        aerospike_key = ('test', None, record_key)
        aerospike.engine.put(aerospike_key, { 'map': {'a': 10} })

        with pytest.raises(sdc_api.StartError) as error:
            sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()
        assert 'AEROSPIKE_00' in error.value.message
        assert 'Seed host tlsName \'aerocluster\' defined but client tlsPolicy not enabled' in error.value.message

        (_, _, bins) = aerospike.engine.get(aerospike_key)
        assert list(bins.keys()) == ['map']
        assert bins['map'] == {'a': 10}

    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)


def test_check_wrong_tls_name(sdc_builder, sdc_executor, aerospike):
    if not aerospike.use_tls:
        pytest.skip('This test only applies when using TLS')
    record_key = get_random_string()
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(
            data_format='JSON', raw_data=json.dumps({ 'map': {'b': 20} }), stop_after_first_batch=True)
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key=record_key)
    wiretap = builder.add_wiretap()

    dev_raw_data_source >> [aerospike_destination, wiretap.destination]
    producer_dest_pipeline = builder.build(title='Aerospike Destination Merge No Update Test').configure_for_environment(aerospike)
    aerospike_destination.nodes[0]['clusterTlsName'] = 'fake_tls_name'

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        aerospike_key = ('test', None, record_key)
        aerospike.engine.put(aerospike_key, { 'map': {'a': 10} })

        with pytest.raises(sdc_api.StartError) as error:
            sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()
        assert 'AEROSPIKE_00' in error.value.message
        assert 'Invalid TLS name: fake_tls_name' in error.value.message

        (_, _, bins) = aerospike.engine.get(aerospike_key)
        assert list(bins.keys()) == ['map']
        assert bins['map'] == {'a': 10}

    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)


def test_check_wrong_use_tls_with_wrong_node_config(sdc_builder, sdc_executor, aerospike):
    if not aerospike.use_tls:
        pytest.skip('This test only applies when using TLS')
    record_key = get_random_string()
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(
            data_format='JSON', raw_data=json.dumps({ 'map': {'b': 20} }), stop_after_first_batch=True)
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key=record_key)
    wiretap = builder.add_wiretap()

    dev_raw_data_source >> [aerospike_destination, wiretap.destination]
    producer_dest_pipeline = builder.build(title='Aerospike Destination Merge No Update Test').configure_for_environment(aerospike)
    # Usual node configuration when no using TLS
    aerospike_destination.nodes[0]['clusterTlsName'] = ''
    aerospike_destination.nodes[0]['port'] = 3000

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        aerospike_key = ('test', None, record_key)
        aerospike.engine.put(aerospike_key, { 'map': {'a': 10} })

        with pytest.raises(sdc_api.StartError) as error:
            sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()
        assert 'AEROSPIKE_00' in error.value.message
        assert 'Remote host terminated the handshake' in error.value.message

        (_, _, bins) = aerospike.engine.get(aerospike_key)
        assert list(bins.keys()) == ['map']
        assert bins['map'] == {'a': 10}

    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)


def test_check_external_auth_no_tls(sdc_builder, sdc_executor, aerospike):
    record_key = get_random_string()
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(
            data_format='JSON', raw_data=json.dumps({ 'map': {'b': 20} }), stop_after_first_batch=True)
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key=record_key)
    wiretap = builder.add_wiretap()

    dev_raw_data_source >> [aerospike_destination, wiretap.destination]
    producer_dest_pipeline = builder.build(title='Aerospike Destination External with no TLS').configure_for_environment(aerospike)

    aerospike_destination.use_authentication = True
    aerospike_destination.authentication_mode = 'EXTERNAL'
    aerospike_destination.use_tls = False

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        aerospike_key = ('test', None, record_key)
        aerospike.engine.put(aerospike_key, { 'map': {'a': 10} })

        with pytest.raises(sdc_api.StartError) as error:
            sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()
        assert 'AEROSPIKE_01' in error.value.message

        (_, _, bins) = aerospike.engine.get(aerospike_key)
        assert list(bins.keys()) == ['map']
        assert bins['map'] == {'a': 10}

    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)
