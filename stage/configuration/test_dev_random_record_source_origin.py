import pytest

from streamsets.testframework.decorators import stub


@stub
def test_delay_between_batches(sdc_builder, sdc_executor):
    pass


@stub
def test_fields_to_generate(sdc_builder, sdc_executor):
    pass


@stub
def test_max_records_to_generate(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass

