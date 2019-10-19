import pytest

from streamsets.testframework.decorators import stub


@stub
def test_delay_between_batches(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'fetch_cpu_stats': False}, {'fetch_cpu_stats': True}])
def test_fetch_cpu_stats(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'fetch_disk_stats': False}, {'fetch_disk_stats': True}])
def test_fetch_disk_stats(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'fetch_host_information': False}, {'fetch_host_information': True}])
def test_fetch_host_information(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'fetch_memory_stats': False}, {'fetch_memory_stats': True}])
def test_fetch_memory_stats(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'fetch_network_stats': False}, {'fetch_network_stats': True}])
def test_fetch_network_stats(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'fetch_process_stats': False}, {'fetch_process_stats': True}])
def test_fetch_process_stats(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'fetch_process_stats': True}])
def test_processes(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'fetch_process_stats': True}])
def test_user(sdc_builder, sdc_executor, stage_attributes):
    pass

