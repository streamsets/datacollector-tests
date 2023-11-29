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

import hashlib
import json
import logging
import pytest
import time
from datetime import datetime
from decimal import Decimal
from collections import ChainMap, OrderedDict

from streamsets.sdk.utils import Version
from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)

# pylint: disable=pointless-statement, too-many-locals

@sdc_min_version('3.1.0.0')
def test_field_replacer(sdc_builder, sdc_executor):
    """Test field replacer processor. The pipeline would look like:

        dev_raw_data_source >> field_replacer >> wiretap

    With the given replacement rules, we do following:
        change value of ssn field to be 'XXX-XX-XXXX',
        set ranking field to null,
        set statistics field to null if it contains 'NA' or 'not_available'
    """
    winners = [dict(ssn='111-11-1111', year='2010', ranking='3', statistics='NA'),
               dict(ssn='111-22-1111', year='2011', ranking='2', statistics='2-3-3'),
               dict(ssn='111-33-1111', year='2012', ranking='1', statistics='not_available')]
    raw_data = ''.join([json.dumps(winner) for winner in winners])

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)
    field_replacer = pipeline_builder.add_stage('Field Replacer')
    field_replacer.replacement_rules = [{'setToNull': False, 'fields': '/ssn', 'replacement': 'XXX-XX-XXXX'},
                                        {'setToNull': True, 'fields': '/ranking'},
                                        {'setToNull': True,
                                         'fields': '/*[${f:value() == "NA" || f:value() == "not_available"}]'}]
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_replacer >> wiretap.destination
    pipeline = pipeline_builder.build('Field Replacer pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        for rec in winners:
            rec['ssn'] = 'XXX-XX-XXXX'
            rec['ranking'] = None
            if rec['statistics'] == 'NA' or rec['statistics'] == 'not_available':
                rec['statistics'] = None
        actual_data = [rec.field for rec in wiretap.output_records]
        assert actual_data == winners
    finally:
        sdc_executor.remove_pipeline(pipeline)

@sdc_min_version('5.3.0')
def test_field_replacer_non_existent_fields(sdc_builder, sdc_executor):
    """Test field replacer processor for non-existent fields. The pipeline would look like:

        dev_raw_data_source >> [field_replacer1, field_replacer2]
        field_replacer1 >> wiretap1
        field_replacer2 >> wiretap2

    With the given replacement rules, we do following:
    Field Replacer 1 -
        change value of 'name' field to be 'unknown',
        skip processing 'level' (a non-existent field),
        set value of 'laps' to null,
        skip processing 'cars' (a non-existent field)

    Field Replacer 2 -
        change value of 'name' field to be 'unknown',
        add field 'level' (a non-existent field) with value 'high',
        set value of 'laps' to null,
        add field 'cars' (a non-existent field) and set it to null
    """
    records = [dict(id=1, name='Harvey', laps=10),
               dict(id=2, name='Mike', laps=8)]
    raw_data = ''.join([json.dumps(record) for record in records])

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)

    field_replacer_1 = pipeline_builder.add_stage('Field Replacer')
    field_replacer_2 = pipeline_builder.add_stage('Field Replacer')

    # Case 1 - Include without processing : Skip non-existent fields
    field_replacer_1.set_attributes(field_does_not_exist='CONTINUE')
    field_replacer_1.replacement_rules = [{'setToNull': False, 'fields': '/name', 'replacement': 'unknown'},
                                          {'setToNull': False, 'fields': '/level', 'replacement': 'high'},
                                          {'setToNull': True, 'fields': '/laps'},
                                          {'setToNull': True, 'fields': '/cars'}]
    wiretap_1 = pipeline_builder.add_wiretap()

    # Case 2 - Add if not exist : Add non-existent fields
    field_replacer_2.set_attributes(field_does_not_exist='ADD_FIELD')
    field_replacer_2.replacement_rules = [{'setToNull': False, 'fields': '/name', 'replacement': 'unknown'},
                                          {'setToNull': False, 'fields': '/level', 'replacement': 'high'},
                                          {'setToNull': True, 'fields': '/laps'},
                                          {'setToNull': True, 'fields': '/cars'}]
    wiretap_2 = pipeline_builder.add_wiretap()

    dev_raw_data_source >> [field_replacer_1, field_replacer_2]
    field_replacer_1 >> wiretap_1.destination
    field_replacer_2 >> wiretap_2.destination

    pipeline = pipeline_builder.build('Field Replacer Pipeline')
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records_1 = records
        records_2 = records

        for rec in records_1:
            rec['name'] = 'unknown'
            rec['laps'] = None
        actual_data_1 = [rec.field for rec in wiretap_1.output_records]
        # assert records_1 data
        assert actual_data_1 == records_1

        for rec in records_2:
            rec['name'] = 'unknown'
            rec['level'] = 'high'
            rec['laps'] = None
            rec['cars'] = None
        actual_data_2 = [rec.field for rec in wiretap_2.output_records]
        # assert records_2 data
        assert actual_data_2 == records_2
    finally:
        sdc_executor.remove_pipeline(pipeline)

@sdc_min_version('3.1.0.0')
def test_field_replacer_self_referencing_expression(sdc_builder, sdc_executor):
    """Field Replacer supports self-referencing expressions and we need to make sure that they work properly and don't
    run into StackOverflowError (which can happen with some optimizations like SDC-14645).
    """
    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('Dev Raw Data Source')
    source.data_format='JSON'
    source.raw_data='{"key": "value"}'
    source.stop_after_first_batch = True

    replacer = builder.add_stage('Field Replacer')
    replacer.replacement_rules = [{'setToNull': False, 'fields': '/new', 'replacement': '${record:value("/")}'}]

    wiretap = builder.add_wiretap()

    source >> replacer >> wiretap.destination
    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert len(wiretap.output_records) == 1
        assert wiretap.output_records[0].get_field_data('/new/key') == 'value'
    finally:
        sdc_executor.remove_pipeline(pipeline)

@sdc_min_version('3.1.0.0')
def test_field_replacer_field_path_expressions(sdc_builder, sdc_executor):
    DATA = {'a': 'a', 'b': 'b', 'c':1}
    EXPECTED_OUTPUT = {'a': 'prefix_a', 'b': 'prefix_b', 'c':1}

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = json.dumps(DATA)
    dev_raw_data_source.stop_after_first_batch = True

    field_replacer = pipeline_builder.add_stage('Field Replacer')
    field_replacer.replacement_rules = [{'setToNull': False,
                                         'fields': '/*[${f:type() == "STRING"}]',
                                         'replacement': 'prefix_${f:value()}'}]

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_replacer >> wiretap.destination

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        record = wiretap.output_records[0]
        assert record.field == EXPECTED_OUTPUT
    finally:
        sdc_executor.remove_pipeline(pipeline)

@sdc_min_version('3.1.0.0')
def test_field_replacer_field_expression_empty_to_error(sdc_builder, sdc_executor):
    DATA = {}
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = json.dumps(DATA)
    dev_raw_data_source.stop_after_first_batch = True

    field_replacer = pipeline_builder.add_stage('Field Replacer')
    field_replacer.replacement_rules = [{'setToNull': False,
                                         'fields': '/*[${f:value() == "SORRY NOT HERE"}]',
                                         'replacement': 'static'}]
    field_replacer.field_does_not_exist = 'TO_ERROR'

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> field_replacer >> wiretap.destination

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert len(wiretap.output_records) == 0
        assert len(wiretap.error_records) == 1
    finally:
        sdc_executor.remove_pipeline(pipeline)