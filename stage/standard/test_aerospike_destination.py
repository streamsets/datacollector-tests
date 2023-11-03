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

import re
import json
import pytest
import time
from streamsets.testframework.markers import aerospike, sdc_min_version
from streamsets.testframework.utils import get_random_string

AEROSPIKE_CLIENT_DESTINATION = 'Aerospike Client'

pytestmark = [aerospike, sdc_min_version('5.6.0')]


@pytest.mark.parametrize('sdc_type, input_value, expected_type, expected_value', [
    ('BOOLEAN', True, bool, True),
    ('CHAR', 'a', int, 97),
    ('BYTE', 0x10, int, 16),
    ('SHORT', 100, int, 100),
    ('INTEGER', 100, int, 100),
    ('LONG', 100, int, 100),
    ('FLOAT', 0.10, float, 0.10),
    ('DOUBLE', 0.10, float, 0.10),
    ('STRING', 'Aerospike', str, 'Aerospike'),
    ('BYTE_ARRAY', 'Hello', bytes, b'Hello'),
    ('MAP', {'a': 10}, dict, {'a': 10}),
    ('LIST_MAP', {'a': 10}, dict, {'a': 10}),
    ('LIST', [1,2,3], list, [1,2,3]),
    ('DECIMAL', 100, float, 100.0),
    ('DATE', '2020-01-01 10:00:00', str, 'Wed Jan 01 10:00:00 [A-Z]{3,4} 2020'),
    ('DATETIME', '2020-01-01 10:00:00', str, 'Wed Jan 01 10:00:00 [A-Z]{3,4} 2020'),
    ('TIME', '2020-01-01 10:00:00', str, 'Wed Jan 01 10:00:00 [A-Z]{3,4} 2020'),
    ('ZONED_DATETIME', "2020-01-01T10:00:00+00:00", str, "2020-01-01T10:00:00Z"),
])
def test_data_types(sdc_builder, sdc_executor, aerospike, sdc_type, input_value, expected_type, expected_value):
    record_key = get_random_string()
    builder = sdc_builder.get_pipeline_builder()
    raw_data = json.dumps({"value": input_value})
    source = builder.add_stage('Dev Raw Data Source').set_attributes(
            data_format='JSON',
            raw_data=raw_data,
            stop_after_first_batch=True
    )
    converter = builder.add_stage('Field Type Converter').set_attributes(
            conversion_method='BY_FIELD',
            field_type_converter_configs=[{
                'fields': ['/value'],
                'targetType': sdc_type,
                'dataLocale': 'en,US',
                'dateFormat': 'YYYY_MM_DD_HH_MM_SS',
                'zonedDateTimeFormat': 'ISO_OFFSET_DATE_TIME',
                'scale': 2}]
    )
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key=record_key,
            on_record_error='STOP_PIPELINE'
    )

    source >> converter >> aerospike_destination
    producer_dest_pipeline = builder.build(title='Aerospike Destination Test Data Types').configure_for_environment(aerospike)

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()

        aerospike_key = ('test', None, record_key)
        (_, _, bins) = aerospike.engine.get(aerospike_key)
        assert type(bins['value']) == expected_type
        if expected_type == float:
            assert round(bins['value'], 2) == round(expected_value, 2)
        elif expected_type == str:
            assert re.search(expected_value, bins['value']) != None
        else:
            assert bins['value'] == expected_value
    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)


def test_object_names():
    """ Can not create arbitrary namespaces / sets as it needs to be configured in the server directly and restart the aerospike service """
    pass


def test_dataflow_event():
    """ Aerospike destination does not produce events """
    pass


def test_multiple_batches(sdc_builder, sdc_executor, aerospike):
    total_records = 100
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Dev Data Generator')
    origin.set_attributes(batch_size=10, 
                          records_to_be_generated=total_records, 
                          delay_between_batches=0,
                          fields_to_generate=[
                              {"type": "LONG_SEQUENCE","field": "id"},
                              {"type": "STRING", "field": "payload"}])
    aerospike_destination = builder.add_stage(AEROSPIKE_CLIENT_DESTINATION).set_attributes(
            namespace='test', 
            key='${record:value("/id")}',
            on_record_error='STOP_PIPELINE')
    wiretap = builder.add_wiretap()

    origin >> [aerospike_destination, wiretap.destination]
    producer_dest_pipeline = builder.build(title='Aerospike Standard Multiple Batches').configure_for_environment(aerospike)

    sdc_executor.add_pipeline(producer_dest_pipeline)

    try:
        sdc_executor.start_pipeline(producer_dest_pipeline).wait_for_finished()

        output_records = wiretap.output_records
        assert len(output_records) == total_records
        for output_record in output_records:
            record_id = output_record.field['id'].value
            (_,_,aerospike_record_bins) = aerospike.engine.get(('test', None, record_id))
            assert output_record.field['payload'].value == aerospike_record_bins['payload']
    finally:
        sdc_executor.remove_pipeline(producer_dest_pipeline)


def test_data_formats():
    """ Stage is not using data format library """

def test_multithreading():
    """ Test does not use multithreading """

def test_push_pull():
    pytest.skip(
        "We haven't re-implemented this test since Dev Data Generator (push) is part of test_multiple_batches and "
        "Dev Raw Data Source (pull) is part of test_data_types.")
