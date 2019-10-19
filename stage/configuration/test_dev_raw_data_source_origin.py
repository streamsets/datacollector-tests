import pytest

from streamsets.testframework.decorators import stub


@stub
def test_event_data(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_raw_data(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'stop_after_first_batch': False}, {'stop_after_first_batch': True}])
def test_stop_after_first_batch(sdc_builder, sdc_executor, stage_attributes):
    pass

