import json

import pytest
from streamsets.testframework.decorators import stub


def test_fields_to_rename(sdc_builder, sdc_executor):
    try:
        DATA = dict(name='Al Gore', birthplace='Washington, D.C.')
        EXPECTED_RENAMED_DATA = dict(internetInventor='Al Gore', birthplace='Washington, D.C.')

        pipeline_builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.raw_data = json.dumps(DATA)
        field_renamer = pipeline_builder.add_stage('Field Renamer')
        field_renamer.fields_to_rename = [{'fromFieldExpression': '/name', 'toFieldExpression': '/internetInventor'}]
        trash = pipeline_builder.add_stage('Trash')

        dev_raw_data_source >> field_renamer >> trash
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        record = snapshot[field_renamer].output[0]
        assert record.field == EXPECTED_RENAMED_DATA
    finally:
        sdc_executor.stop_pipeline(pipeline)


@stub
@pytest.mark.parametrize('stage_attributes', [{'multiple_source_field_matches': 'CONTINUE'},
                                              {'multiple_source_field_matches': 'TO_ERROR'}])
def test_multiple_source_field_matches(sdc_builder, sdc_executor, stage_attributes):
    pass


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


@stub
@pytest.mark.parametrize('stage_attributes', [{'source_field_does_not_exist': 'CONTINUE'},
                                              {'source_field_does_not_exist': 'TO_ERROR'}])
def test_source_field_does_not_exist(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'target_field_already_exists': 'APPEND_NUMBERS'},
                                              {'target_field_already_exists': 'CONTINUE'},
                                              {'target_field_already_exists': 'REPLACE'},
                                              {'target_field_already_exists': 'TO_ERROR'}])
def test_target_field_already_exists(sdc_builder, sdc_executor, stage_attributes):
    pass

