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
        dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=json.dumps(DATA))
        stream_selector = pipeline_builder.add_stage('Stream Selector')
        dev_identity_1 = pipeline_builder.add_stage('Dev Identity')
        dev_identity_2 = pipeline_builder.add_stage('Dev Identity')
        trash = pipeline_builder.add_stage('Trash')

        dev_raw_data_source >> stream_selector
        stream_selector >> dev_identity_1 >> trash
        stream_selector >> dev_identity_2 >> trash

        # Stream Selector conditions depend on the output lanes, so we set them after connecting the stages.
        stream_selector.condition = [{'outputLane': stream_selector.output_lanes[0],
                                      'predicate': '${record:value("/name") == "Al Gore"}'},
                                     {'outputLane': stream_selector.output_lanes[1],
                                      'predicate': 'default'}]

        pipeline = pipeline_builder.build()
        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

        condition_1_record = snapshot[dev_identity_1].output[0]
        condition_2_record = snapshot[dev_identity_2].output[0]
        assert condition_1_record.field == EXPECTED_CONDITION_1_DATA
        assert condition_2_record.field == EXPECTED_CONDITION_2_DATA
    finally:
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

