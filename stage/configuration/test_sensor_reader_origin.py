import pytest

from streamsets.testframework.decorators import stub


@stub
def test_delay_between_batches(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'sensor_device': 'BMxx80'}])
def test_i2c_address(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'sensor_device': 'BCM2835'}])
def test_path_to_pseudo_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'sensor_device': 'BCM2835'}])
def test_scaling_factor(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'sensor_device': 'BCM2835'}, {'sensor_device': 'BMxx80'}])
def test_sensor_device(sdc_builder, sdc_executor, stage_attributes):
    pass

