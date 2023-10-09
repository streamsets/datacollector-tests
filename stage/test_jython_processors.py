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

import json
import logging
import textwrap

import pytest
from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)

# pylint: disable=pointless-statement, redefined-outer-name, too-many-locals


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-jython_2_7-lib')

    return hook


def test_jython_evaluator(sdc_builder, sdc_executor):
    """Test Jython Evaluator processor. We test by setting up a template object in initialization script, then
    main processing script which access the template to create a Jython object per record and to create a new record
    attribute. Finally in destroy script, we collate all the records for its new attribute and Jython assert its value.
    The pipeline would look like:

        dev_raw_data_source >> jython_evaluator >> trash
    """
    raw_company_1 = dict(name='StreamSets', floors=3)
    raw_company_2 = dict(name='Example Inc.', floors=1)
    raw_company_3 = dict(name='ASDF', floors=10)
    raw_company_4 = dict(name='QWER Inc.', floors=2)
    raw_data = json.dumps([raw_company_1, raw_company_2, raw_company_3, raw_company_4])

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data,
                                       stop_after_first_batch=True)
    jython_evaluator = pipeline_builder.add_stage('Jython Evaluator', type='processor')
    # in the init script we create a 'Building' template object which can be cloned per each pipeline record processing
    init_script = """
        class Building(object):
            def __init__(self, name=None, floors=None, office_space=None):
                self.name = name
                self.floors = floors
                self.office_space = office_space

        state['building_template'] = Building()
    """
    # in the script for every record processing, we compute if a building has more than 2 floors and
    # if so we create a record attribute 'office_space' to 'true' else 'false'
    script = """
        import copy

        state['buildings'] = []
        for record in records:
            try:
                record.value['office_space'] = (record.value['floors'] > 2)

                building = copy.copy(state['building_template'])
                building.name = record.value['name']
                building.floors = record.value['floors']
                building.office_space = record.value['office_space']

                state['buildings'].append(building)
                output.write(record)
            except Exception as e:
                error.write(record, str(e))
    """
    # in the destroy script, we collate all the building names which have 'office_space' and do a Jython assert
    destroy_script = """
        buildings = state['buildings']
        office_building_names = [building.name for building in buildings
                                 if building.office_space and building.floors > 2]
        assert officeBuildingNames == ['StreamSets', 'ASDF']
    """
    # textwrap.dedent helps to strip leading whitespaces for valid Python indentation
    jython_evaluator.set_attributes(destroy_script=textwrap.dedent(destroy_script),
                                    init_script=textwrap.dedent(init_script),
                                    record_processing_mode='BATCH',
                                    script=textwrap.dedent(script))
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> jython_evaluator >> wiretap.destination
    pipeline = pipeline_builder.build('Jython Evaluator pipeline')

    try:
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # search for a record whose 'name' is raw_company_1['name'] and assert new attribute ('office_space') is created
        # with expected boolean value (where 'floors' > 2)
        record_1 = next(record for record in wiretap.output_records if record.field['name'] == raw_company_1['name'])
        assert record_1.field['office_space'] == (raw_company_1['floors'] > 2)
        record_2 = next(record for record in wiretap.output_records if record.field['name'] == raw_company_2['name'])
        assert record_2.field['office_space'] == (raw_company_2['floors'] > 2)

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        sdc_executor.remove_pipeline(pipeline)


# SDC-11555: Provide ability to use direct SDC record in scripting processors
@sdc_min_version('3.9.0')
def test_sdc_record(sdc_builder, sdc_executor):
    """Iterate over SDC record directly rather then JSR-223 wrapper."""
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Dev Raw Data Source')
    origin.set_attributes(data_format='JSON', raw_data='{"old": "old-value"}', stop_after_first_batch=True)

    jython = builder.add_stage('Jython Evaluator', type='processor')
    jython.record_type = 'SDC_RECORDS'
    jython.init_script = ''
    jython.destroy_script = ''
    jython.script = """
from com.streamsets.pipeline.api import Field

for record in records:
  record.sdcRecord.set('/new', Field.create(Field.Type.STRING, 'new-value'))
  record.sdcRecord.get('/old').setAttribute('attr', 'attr-value')
  output.write(record)
"""

    wiretap = builder.add_wiretap()

    origin >> jython >> wiretap.destination

    pipeline = builder.build()

    try:
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert len(wiretap.output_records) == 1
        assert 'new-value' == wiretap.output_records[0].field['new']
        assert 'attr-value' == wiretap.output_records[0].get_field_data('/old').attributes['attr']

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        sdc_executor.remove_pipeline(pipeline)


@sdc_min_version('3.9.0')
def test_sdc_record_verify_none_typed(sdc_builder, sdc_executor):
    """Asserting field with None value"""
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Dev Raw Data Source')
    origin.set_attributes(data_format='JSON', raw_data='{}', stop_after_first_batch=True)

    jython = builder.add_stage('Jython Evaluator', type='processor')
    jython.record_type = 'SDC_RECORDS'
    jython.init_script = ''
    jython.destroy_script = ''
    jython.script = """
from com.streamsets.pipeline.api import Field

for record in sdc.records:
    record.sdcRecord.set('/null_int', Field.create(None))
    record.sdcRecord.set('/null_long', Field.create(None))
    record.sdcRecord.set('/null_float', Field.create(None))
    record.sdcRecord.set('/null_double', Field.create(None))
    record.sdcRecord.set('/null_date', Field.create(None))
    record.sdcRecord.set('/null_datetime', Field.create(None))
    record.sdcRecord.set('/null_boolean', Field.create(None))
    record.sdcRecord.set('/null_decimal', Field.create(None))
    record.sdcRecord.set('/null_byteArray', Field.create(None))
    record.sdcRecord.set('/null_string', Field.create(None))
    record.sdcRecord.set('/null_list', Field.create(None))
    record.sdcRecord.set('/null_map', Field.create(None))
    record.sdcRecord.set('/null_time', Field.create(None))
    output.write(record)
"""

    wiretap = builder.add_wiretap()

    origin >> jython >> wiretap.destination

    pipeline = builder.build()
    try:
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        output_records = wiretap.output_records
        fields = output_records[0].field
        assert len(output_records) == 1
        assert len(fields) == 13
        for record in fields.keys():
            assert None == fields[record]

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        sdc_executor.remove_pipeline(pipeline)



@sdc_min_version('3.9.0')
def test_sdc_record_change_to_none_typed(sdc_builder, sdc_executor):
    """Asserting fields with none value after they have been changed by the Jython Processor"""
    builder = sdc_builder.get_pipeline_builder()

    raw_data = """
       {
            "null_int": 123, 
            "null_string": "this is string field", 
            "null_decimal": 123.45, 
            "null_list": ["num1", "val2"], 
            "null_map": {"num1": "val2"}
       }
    """

    origin = builder.add_stage('Dev Raw Data Source')
    origin.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)

    jython = builder.add_stage('Jython Evaluator', type='processor')
    jython.record_type = 'SDC_RECORDS'
    jython.init_script = ''
    jython.destroy_script = ''
    jython.script = """
from com.streamsets.pipeline.api import Field

for record in sdc.records:
    record.sdcRecord.set('/null_int', Field.create(None))
    record.sdcRecord.set('/null_string', Field.create(None))
    record.sdcRecord.set('/null_decimal', Field.create(None))
    record.sdcRecord.set('/null_list', Field.create(None))
    record.sdcRecord.set('/null_map', Field.create(None))
    output.write(record)
"""

    wiretap = builder.add_wiretap()

    origin >> jython >> wiretap.destination

    pipeline = builder.build()

    try:
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        output_records = wiretap.output_records
        fields = output_records[0].field
        assert len(output_records) == 1
        assert len(fields) == 5
        for record in fields.keys():
            assert None == fields[record]

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        sdc_executor.remove_pipeline(pipeline)


@sdc_min_version('3.9.0')
def test_sdc_record_get_field(sdc_builder, sdc_executor):
    """Asserting field values after the Jython Processor changes the None values"""
    builder = sdc_builder.get_pipeline_builder()

    raw_data = """
        {
            "null_int": null, 
            "null_string": null, 
            "null_decimal": null, 
            "null_boolean": null
        }
    """

    origin = builder.add_stage('Dev Raw Data Source')
    origin.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)

    jython = builder.add_stage('Jython Evaluator', type='processor')
    jython.record_type = 'SDC_RECORDS'
    jython.init_script = ''
    jython.destroy_script = ''
    jython.script = """
from com.streamsets.pipeline.api import Field

for record in sdc.records:
    record.sdcRecord.set('/null_int', Field.create(Field.Type.INTEGER, 123))
    record.sdcRecord.set('/null_string', Field.create(Field.Type.STRING, "this is string field"))
    record.sdcRecord.set('/null_decimal', Field.create(Field.Type.DECIMAL, 123.45))
    record.sdcRecord.set('/null_boolean', Field.create(Field.Type.BOOLEAN, True))
    output.write(record)
"""

    wiretap = builder.add_wiretap()

    origin >> jython >> wiretap.destination

    pipeline = builder.build()

    try:
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        output_records = wiretap.output_records
        fields = output_records[0].field
        assert len(output_records) == 1
        assert len(fields) == 4
        expected = [123, "this is string field", 123.45, True]
        iterator = 0
        for record in output_records:
            record == expected[iterator]
            iterator = iterator + 1

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        sdc_executor.remove_pipeline(pipeline)


@sdc_min_version('3.9.0')
def test_sdc_record_header_attributes(sdc_builder, sdc_executor):
    """Asserting attribute/header value"""
    builder = sdc_builder.get_pipeline_builder()

    raw_data = """
        {
            "old": "old_value", 
            "new": "new_value"
        }
    """

    origin = builder.add_stage('Dev Raw Data Source')
    origin.set_attributes(data_format='JSON', raw_data=raw_data, stop_after_first_batch=True)

    jython = builder.add_stage('Jython Evaluator', type='processor')
    jython.record_type = 'SDC_RECORDS'
    jython.init_script = ''
    jython.destroy_script = ''
    jython.script = """
from com.streamsets.pipeline.api import Field

for record in sdc.records:
    record.sdcRecord.get('/old').setAttribute('header', 'header_value')
    record.sdcRecord.get('/new').deleteAttribute('header')
    output.write(record)
"""

    wiretap = builder.add_wiretap()

    origin >> jython >> wiretap.destination

    pipeline = builder.build()

    try:
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        output_records = wiretap.output_records
        fields = output_records[0].field
        assert len(output_records) == 1
        assert len(fields) == 2
        assert 'header_value' == wiretap.output_records[0].get_field_data('/old').attributes['header']

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        sdc_executor.remove_pipeline(pipeline)


def test_event_creation(sdc_builder, sdc_executor):
    """Ensure that the process is able to create events."""
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Dev Raw Data Source')
    origin.set_attributes(data_format='JSON', raw_data='{"old": "old-value"}', stop_after_first_batch=True)
    origin.stop_after_first_batch = True

    jython = builder.add_stage('Jython Evaluator', type='processor')
    jython.init_script = ''
    jython.destroy_script = ''
    jython.script = """
event = sdcFunctions.createEvent("event", 1)
event.value = sdcFunctions.createMap(True)
event.value['value'] = "secret"
sdcFunctions.toEvent(event)
"""

    records_wiretap = builder.add_wiretap()
    events_wiretap = builder.add_wiretap()

    origin >> jython >> records_wiretap.destination
    jython >= events_wiretap.destination

    pipeline = builder.build()

    try:
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        assert len(events_wiretap.output_records) == 1
        assert events_wiretap.output_records[0].get_field_data('/value') == 'secret'

    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        sdc_executor.remove_pipeline(pipeline)