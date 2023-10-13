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

import pytest
import json

# pylint: disable=pointless-statement, too-many-locals

DATAPARSER_01 = 'DATAPARSER_01'
DATAPARSER_05 = 'DATAPARSER_05'

def test_invalid_input_field_to_parse(sdc_builder, sdc_executor):
    """Test Data Parser processor with invalid input in field_to_parse configuration.
       The pipeline would look like:

            dev_raw_data_source >> data_parser >> wiretap
    """
    raw_data = '{"key": "value"}'
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    # Set output_type
    data_parser = pipeline_builder.add_stage('Data Parser', type='processor')
    data_parser.set_attributes(field_to_parse='/key',
                               target_field='/',
                               multiple_values_behavior='SPLIT_INTO_MULTIPLE_RECORDS',
                               data_format='JSON')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> data_parser >> wiretap.destination

    try:
        pipeline = pipeline_builder.build()
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Gather wiretap data as a list for verification.
        output_records = wiretap.output_records
        error_records = wiretap.error_records
        assert len(output_records) == 0
        assert len(error_records) == 1
        error_record = error_records[0]
        assert error_record.header['errorCode'] == DATAPARSER_05
    finally:
        sdc_executor.remove_pipeline(pipeline)


def test_error_handling(sdc_builder, sdc_executor):
    """Test Data Parser processor with invalid raw_data '{"foo": bar}' since bar is not enclosed in
       quotes and hence cause error while parsing
       The pipeline would look like:
    
            dev_raw_data_source >> data_parser >> wiretap
    """
    raw_data = '{"foo": bar}'
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)

    # Set output_type
    data_parser = pipeline_builder.add_stage('Data Parser', type='processor')
    data_parser.set_attributes(field_to_parse='/text',
                               target_field='/',
                               multiple_values_behavior='SPLIT_INTO_MULTIPLE_RECORDS',
                               data_format='JSON')

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> data_parser >> wiretap.destination

    try:
        pipeline = pipeline_builder.build()
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Gather wiretap data as a list for verification.
        output_records = wiretap.output_records
        error_records = wiretap.error_records
        assert len(output_records) == 0
        assert len(error_records) == 1
        error_record = error_records[0]
        assert error_record.header['errorCode'] == DATAPARSER_01
    finally:
        sdc_executor.remove_pipeline(pipeline)