# Copyright 2021 StreamSets Inc.
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
from streamsets.testframework.decorators import stub


def test_condition(sdc_builder, sdc_executor):
    try:
        DATA = [dict(name='Al Gore', birthplace='Washington, D.C.'),
                dict(name='George W. Bush', birthplace='New Haven, CT')]
        EXPECTED_CONDITION_1_DATA = DATA[0]
        EXPECTED_CONDITION_2_DATA = DATA[1]

        pipeline_builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS',
                                           raw_data=json.dumps(DATA), stop_after_first_batch=True)
        stream_selector = pipeline_builder.add_stage('Stream Selector')
        dev_identity_1 = pipeline_builder.add_stage('Dev Identity')
        dev_identity_2 = pipeline_builder.add_stage('Dev Identity')
        wiretap_1 = pipeline_builder.add_wiretap()
        wiretap_2 = pipeline_builder.add_wiretap()

        dev_raw_data_source >> stream_selector
        stream_selector >> dev_identity_1 >> wiretap_1.destination
        stream_selector >> dev_identity_2 >> wiretap_2.destination

        # Stream Selector conditions depend on the output lanes, so we set them after connecting the stages.
        stream_selector.condition = [{'outputLane': stream_selector.output_lanes[0],
                                      'predicate': '${record:value("/name") == "Al Gore"}'},
                                     {'outputLane': stream_selector.output_lanes[1],
                                      'predicate': 'default'}]

        pipeline = pipeline_builder.build()
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        condition_1_record = wiretap_1.output_records[0]
        condition_2_record = wiretap_2.output_records[0]
        assert condition_1_record.field == EXPECTED_CONDITION_1_DATA
        assert condition_2_record.field == EXPECTED_CONDITION_2_DATA
    finally:
        status = sdc_executor.get_pipeline_status(pipeline).response.json().get('status')
        if status == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass

