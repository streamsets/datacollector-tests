import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'auto_create_database': False}, {'auto_create_database': True}])
def test_auto_create_database(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'consistency_level': 'ALL'},
                                              {'consistency_level': 'ANY'},
                                              {'consistency_level': 'ONE'},
                                              {'consistency_level': 'QUORUM'}])
def test_consistency_level(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_database_name(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'record_mapping': 'CUSTOM'}])
def test_measurement_field(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_password(sdc_builder, sdc_executor):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'record_mapping': 'COLLECTD'}, {'record_mapping': 'CUSTOM'}])
def test_record_mapping(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_retention_policy(sdc_builder, sdc_executor):
    pass


@stub
def test_tag_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'record_mapping': 'CUSTOM'}])
def test_time_field(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'record_mapping': 'CUSTOM', 'time_unit': 'DAYS'},
                                              {'record_mapping': 'CUSTOM', 'time_unit': 'HOURS'},
                                              {'record_mapping': 'CUSTOM', 'time_unit': 'MICROSECONDS'},
                                              {'record_mapping': 'CUSTOM', 'time_unit': 'MILLISECONDS'},
                                              {'record_mapping': 'CUSTOM', 'time_unit': 'MINUTES'},
                                              {'record_mapping': 'CUSTOM', 'time_unit': 'NANOSECONDS'},
                                              {'record_mapping': 'CUSTOM', 'time_unit': 'SECONDS'}])
def test_time_unit(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_url(sdc_builder, sdc_executor):
    pass


@stub
def test_username(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'record_mapping': 'CUSTOM'}])
def test_value_fields(sdc_builder, sdc_executor, stage_attributes):
    pass

