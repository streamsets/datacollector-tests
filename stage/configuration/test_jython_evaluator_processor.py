import pytest

from streamsets.testframework.decorators import stub


@stub
def test_destroy_script(sdc_builder, sdc_executor):
    pass


@stub
def test_init_script(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_parameters_in_script(sdc_builder, sdc_executor):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'record_processing_mode': 'BATCH'}, {'record_processing_mode': 'RECORD'}])
def test_record_processing_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'record_type': 'NATIVE_OBJECTS'}, {'record_type': 'SDC_RECORDS'}])
def test_record_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_script(sdc_builder, sdc_executor):
    pass

