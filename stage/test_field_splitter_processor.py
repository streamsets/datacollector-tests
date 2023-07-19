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
import logging
import pytest
import time
from datetime import datetime
from decimal import Decimal
from collections import OrderedDict

logger = logging.getLogger(__name__)

SPLITTER_04 = 'SPLITTER_04'

# pylint: disable=pointless-statement, too-many-locals

def test_field_splitter(sdc_builder, sdc_executor):
    """Test field splitter processor. The pipeline would look like:

        dev_raw_data_source >> field_splitter >> wiretap

    With given config to process 3 records, the first record's /error/text value will split into /error/code and
    /error/message based on separator (,). The second record's /error/text value will split similarly but since it has
    too many split, the extra splits will go into new field called /error/etcMessages.  The third record's /error/text
    value doesn't have enough splits, so /error/message will be null
    """
    raw_data = """
        [ { "error": {
                "text": "GM-302,information that you might need"
            }
          },
          { "error": {
                "text": "ME-3042,message about error,additional information from server,network error,driver error"
            }
          },
          { "error": {
                "text": "RK-42 there's no separator here"
            }
          }
        ]
    """
    raw_list = json.loads(raw_data)
    separator = ','
    split_fields = ['/error/code', '/error/message']
    source_sub_field = 'text'
    etc_sub_field = 'etcMessages'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_splitter = pipeline_builder.add_stage('Field Splitter')
    field_splitter.set_attributes(field_for_remaining_splits=f'/error/{etc_sub_field}',
                                  field_to_split=f'/error/{source_sub_field}', new_split_fields=split_fields,
                                  not_enough_splits='CONTINUE', original_field='REMOVE',
                                  separator=separator, too_many_splits='TO_LIST')
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_splitter >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_splitter pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        record_1 = wiretap.output_records[0].field['error']
        record_2 = wiretap.output_records[1].field['error']
        record_3 = wiretap.output_records[2].field['error']
        # assert we got expected number of splits in record 1
        assert len(raw_list[0]['error']['text'].split(separator)) == len(record_1)
        # assert record 1 data
        raw_record_data = raw_list[0]['error']['text'].split(separator)
        for value in record_1.values():
            assert value.value in raw_record_data
        # assert field_for_remaining_splits in record 2
        raw_record_data = raw_list[1]['error']['text'].split(separator)
        # etc_sub_field will only have a subset of splits and hence need to take out (subtract)
        # the remaining in record 2
        assert len(record_2[etc_sub_field]) == len(raw_record_data) - len(split_fields)
        for data in record_2[etc_sub_field]:
            assert data.value in raw_record_data
        # assert record 3 data
        assert len(record_3) == 2
        assert raw_list[2]['error']['text'] == record_3['code']
        # record 3's message field will be null because there was nothing to put into it
        assert record_3['message'] == None
        # assert original_field being removed
        assert source_sub_field not in record_1 and source_sub_field not in record_2 and source_sub_field not in record_3
    finally:
        sdc_executor.remove_pipeline(pipeline)


@pytest.mark.parametrize('split_fields', [
     ['/a'],
     [],
])
def test_field_splitter_split_to_list(sdc_builder, sdc_executor, split_fields):
    raw_data = """
        [ { "line": "a b"
          },
          { "line": "a b c"
          },
          { "line": "a"
          },
          { "line": ""
          },
          { "line": " "
          },
          { "line": null
          }
        ]
    """
    raw_list = json.loads(raw_data)
    separator = "\\s"

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_splitter = pipeline_builder.add_stage('Field Splitter')
    field_splitter.set_attributes(field_to_split=f'/line', new_split_fields=split_fields,
                                  not_enough_splits='CONTINUE', original_field='REMOVE',
                                  separator=separator, too_many_splits='TO_LIST',
                                  field_for_remaining_splits = '/splits_list')
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_splitter >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_splitter_split_to_list pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        #Verify and assert result
        output_records = wiretap.output_records
        if split_fields == ['/a']:
            assert output_records[0].field['a'] == 'a'
            assert output_records[0].field['splits_list'] == ['b']
            assert output_records[1].field['a'] == 'a'
            assert output_records[1].field['splits_list'] == ['b','c']
            assert output_records[2].field['a'] == 'a'
            assert 'splits_list' not in output_records[2].field
            assert output_records[3].field['a'] == ''
            assert 'splits_list' not in output_records[3].field
            assert output_records[4].field['a'].value is None
            assert 'splits_list' not in output_records[4].field
            assert output_records[5].field['a'].value is None
            assert 'splits_list' not in output_records[5].field
        elif split_fields == []:
            assert output_records[0].field['splits_list'] == ['a', 'b']
            assert output_records[1].field['splits_list'] == ['a','b','c']
            assert output_records[2].field['splits_list'] == ['a']
            assert output_records[3].field['splits_list'] == ['']
            assert 'splits_list' not in output_records[4].field
            assert 'splits_list' not in output_records[5].field
    finally:
        sdc_executor.remove_pipeline(pipeline)


def test_field_splitter_splitting_non_string_field(sdc_builder, sdc_executor):
    raw_data = """
        [ { "line": "a b"
          }
        ]
    """
    raw_list = json.loads(raw_data)
    separator = "\\s"
    split_fields = ['/a','/b']

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_splitter = pipeline_builder.add_stage('Field Splitter')
    field_splitter.set_attributes(field_to_split=f'/', new_split_fields=split_fields,
                                  original_field='KEEP',
                                  separator=separator, too_many_splits='TO_LAST_FIELD')
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_splitter >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_splitter_splitting_non_string_field pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        output_records = wiretap.output_records
        error_records = wiretap.error_records
        assert len(output_records) == 0
        assert len(error_records) == 1
        error_record = error_records[0]
        assert error_record.field == raw_list[0]
        assert error_record.header['errorCode'] == SPLITTER_04
    finally:
        sdc_executor.remove_pipeline(pipeline)


@pytest.mark.parametrize('raw_data, separator', [
    ('[{"line": "test^test"}]', '\\^'),
    ('[{"line": "test*test"}]', '\\*'),
])
def test_field_splitter_splitting_with_symbol(sdc_builder, sdc_executor, raw_data, separator):
    raw_list = json.loads(raw_data)
    split_fields = ['/a','/b']

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data,
                                       stop_after_first_batch=True)
    field_splitter = pipeline_builder.add_stage('Field Splitter')
    field_splitter.set_attributes(field_to_split=f'/line', new_split_fields=split_fields,
                                  original_field='REMOVE',
                                  separator=separator, too_many_splits='TO_LAST_FIELD')
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_splitter >> wiretap.destination
    pipeline = pipeline_builder.build('test_field_splitter_splitting_with_symbol pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        output_records = wiretap.output_records
        assert output_records[0].field['a'] == 'test'
        assert output_records[0].field['b'] == 'test'
    finally:
        sdc_executor.remove_pipeline(pipeline)
