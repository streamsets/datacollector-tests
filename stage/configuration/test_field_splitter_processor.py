import json

import pytest
from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'too_many_splits': 'TO_LIST'}])
def test_field_for_remaining_splits(sdc_builder, sdc_executor, stage_attributes):
    pass


def test_field_to_split(sdc_builder, sdc_executor):
    try:
        DATA = dict(name='Al Gore', birthplace='Washington, D.C.')
        EXPECTED_SPLIT_DATA = dict(fieldSplit1='Al', fieldSplit2='Gore', birthplace='Washington, D.C.')

        pipeline_builder = sdc_builder.get_pipeline_builder()

        dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
        dev_raw_data_source.raw_data = json.dumps(DATA)
        dev_raw_data_source.data_format = 'JSON'
        field_splitter = pipeline_builder.add_stage('Field Splitter').set_attributes(field_to_split='/name',
                                                                                     separator='[ ]')
        trash = pipeline_builder.add_stage('Trash')

        dev_raw_data_source >> field_splitter >> trash
        pipeline = pipeline_builder.build()

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        record = snapshot[field_splitter].output[0]
        assert record.field == EXPECTED_SPLIT_DATA
    finally:
        sdc_executor.stop_pipeline(pipeline)


@stub
def test_new_split_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'not_enough_splits': 'CONTINUE'}, {'not_enough_splits': 'TO_ERROR'}])
def test_not_enough_splits(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'original_field': 'KEEP'}, {'original_field': 'REMOVE'}])
def test_original_field(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_separator(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'too_many_splits': 'TO_LAST_FIELD'}, {'too_many_splits': 'TO_LIST'}])
def test_too_many_splits(sdc_builder, sdc_executor, stage_attributes):
    pass

