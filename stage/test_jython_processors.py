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

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

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
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data)
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
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> jython_evaluator >> trash
    pipeline = pipeline_builder.build('Jython Evaluator pipeline')
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)

    output_records = snapshot[jython_evaluator.instance_name].output  # is a list of output records
    # search for a record whose 'name' is raw_company_1['name'] and assert new attribute ('office_space') is created
    # with expected boolean value (where 'floors' > 2)
    record_1 = next(record for record in output_records if record.field['name'] == raw_company_1['name'])
    assert record_1.field['office_space'] == (raw_company_1['floors'] > 2)
    record_2 = next(record for record in output_records if record.field['name'] == raw_company_2['name'])
    assert record_2.field['office_space'] == (raw_company_2['floors'] > 2)
