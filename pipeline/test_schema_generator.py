# Copyright 2017 StreamSets Inc.
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
import logging
import os
import string
import tempfile
import json
from decimal import Decimal

import pytest
from streamsets.testframework.markers import sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


DATA_TYPES = [
    ('true', 'BOOLEAN', True),
    ('10', 'SHORT', 10),
    ('10', 'INTEGER', 10),
    ('10', 'LONG', 10),
    ('10.0', 'FLOAT', 10.0),
    ('10.0', 'DOUBLE', 10.0),
    ('10.0', 'DECIMAL', Decimal('10.00')),
    ('str', 'STRING', 'str'),
    ('2020-01-01 10:00:00', 'DATE', datetime.datetime(2020, 1, 1, 0, 0)),
    ('2020-01-01 10:00:00', 'TIME', datetime.datetime(1970, 1, 19, 17, 56, 42, 368000)),
    ('2020-01-01 10:00:00', 'DATETIME', datetime.datetime(2020, 1, 1, 10, 0)),
    ({'a': 1}, 'MAP', {'a': 1}),
    ([1, 2], 'LIST', [1, 2]),
]
@sdc_min_version('2.7.1.0')
@pytest.mark.parametrize('input,converter_type,expected_value', DATA_TYPES, ids=[i[1] for i in DATA_TYPES])
def test_schema_generator_types(sdc_builder, input, converter_type, expected_value, sdc_executor):
    # Test write directory
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string(string.ascii_letters, 10))

    # Build pipeline that will generate test record and it's schema
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Dev Raw Data Source')
    origin.data_format = 'JSON'
    origin.raw_data = json.dumps({"value": input})
    origin.stop_after_first_batch = True

    prefix = origin

    if converter_type != 'MAP' and converter_type != 'LIST':
        converter = builder.add_stage('Field Type Converter')
        converter.conversion_method = 'BY_FIELD'
        converter.field_type_converter_configs = [{
            'fields': ['/value'],
            'targetType': converter_type,
            'dataLocale': 'en,US',
            'dateFormat': 'YYYY_MM_DD_HH_MM_SS',
            'zonedDateTimeFormat': 'ISO_OFFSET_DATE_TIME',
            'scale': 2
        }]

        origin >> converter
        prefix = converter

    # Generate schema for that record
    schema_generator = builder.add_stage('Schema Generator')
    schema_generator.expand_types = True
    schema_generator.schema_name = 'test_schema'

    # And store it in local file system
    local_fs = builder.add_stage('Local FS', type='destination')
    local_fs.directory_template = tmp_directory
    local_fs.data_format = 'AVRO'
    local_fs.configuration['configs.dataGeneratorFormatConfig.avroSchemaSource'] = 'HEADER'

    # Finish building the pipeline
    prefix >> schema_generator >> local_fs
    generator_pipeline = builder.build()

    # Build second pipeline that will read generated Avro file
    builder = sdc_builder.get_pipeline_builder()

    directory = builder.add_stage('Directory', type='origin')
    directory.data_format = 'AVRO'
    directory.batch_wait_time_in_secs = 10
    directory.file_name_pattern = 'sdc*'
    directory.files_directory = tmp_directory

    finisher = builder.add_stage("Pipeline Finisher Executor")
    finisher.stage_record_preconditions = ["${record:eventType() == 'no-more-data'}"]
    directory >= finisher

    wiretap = builder.add_wiretap()

    directory >> wiretap.destination
    reader_pipeline = builder.build()

    sdc_executor.add_pipeline(generator_pipeline, reader_pipeline)
    # Start the pipelines one by one
    sdc_executor.start_pipeline(generator_pipeline).wait_for_finished()
    sdc_executor.start_pipeline(reader_pipeline).wait_for_finished()

    records = wiretap.output_records
    assert len(records) == 1
    assert records[0].field['value'] == expected_value


@pytest.mark.parametrize('default_to_null,expected_schema', [
    (
        True,  
        {
            "type":"record",
            "name":"test_schema",
            "doc":"",
            "fields":[
                {"name":"source","type":["null","string"],"default":None},
                {"name":"reconDataList","type":["null", {"type":"array", "items": [ "null", {"type":"map","values":["null", "string"]}]}], "default":None}]
        }
    ),
    (
        False,  
        {
            "type":"record",
            "name":"test_schema",
            "doc":"",
            "fields":[
                {"name":"source","type":["null","string"]},
                {"name":"reconDataList","type":["null", {"type":"array", "items": [ "null", {"type":"map","values":["null", "string"]}]}]}]
        }
    ),

])
def test_default_to_nullable(sdc_builder, sdc_executor, default_to_null, expected_schema):
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Dev Raw Data Source')
    origin.data_format = 'JSON'
    origin.raw_data = json.dumps({
        "source":"BARRACUDA",
        "reconDataList":[
            {"topic":"T_BENCHMARK_FIXING","count":"27","timeColumn":"all_data"},
        ]})
    origin.stop_after_first_batch = True

    # Generate schema for that record
    schema_generator = builder.add_stage('Schema Generator')
    schema_generator.schema_name = 'test_schema'
    schema_generator.nullable_fields = True
    schema_generator.default_to_nullable = default_to_null

    # Finish building the pipeline
    wiretap = builder.add_wiretap()
    origin >> schema_generator >> wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    records = wiretap.output_records
    assert len(records) == 1
    generated_avro_schema = records[0].header.values["avroSchema"]
    assert json.loads(generated_avro_schema) == expected_schema
