import pytest

from streamsets.testframework.decorators import stub


@stub
def test_admin_operation_timeout_in_milliseconds(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'case_sensitive': False}, {'case_sensitive': True}])
def test_case_sensitive(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_column_to_output_field_mapping(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': False}, {'enable_local_caching': True}])
def test_enable_local_caching(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_table_caching': False}, {'enable_table_caching': True}])
def test_enable_table_caching(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': True,
                                               'eviction_policy_type': 'EXPIRE_AFTER_ACCESS'},
                                              {'enable_local_caching': True,
                                               'eviction_policy_type': 'EXPIRE_AFTER_WRITE'}])
def test_eviction_policy_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': True}])
def test_expiration_time(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ignore_missing_value_in_matching_record': False},
                                              {'ignore_missing_value_in_matching_record': True}])
def test_ignore_missing_value_in_matching_record(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_key_columns_mapping(sdc_builder, sdc_executor):
    pass


@stub
def test_kudu_masters(sdc_builder, sdc_executor):
    pass


@stub
def test_kudu_table_name(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': True}])
def test_maximum_entries_to_cache(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_maximum_number_of_worker_threads(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_table_caching': True}])
def test_maximum_table_entries_to_cache(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'missing_lookup_behavior': 'PASS_RECORD_ON'},
                                              {'missing_lookup_behavior': 'SEND_TO_ERROR'}])
def test_missing_lookup_behavior(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'multiple_values_behavior': 'FIRST_ONLY'},
                                              {'multiple_values_behavior': 'SPLIT_INTO_MULTIPLE_RECORDS'}])
def test_multiple_values_behavior(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_operation_timeout_in_milliseconds(sdc_builder, sdc_executor):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': True, 'time_unit': 'DAYS'},
                                              {'enable_local_caching': True, 'time_unit': 'HOURS'},
                                              {'enable_local_caching': True, 'time_unit': 'MICROSECONDS'},
                                              {'enable_local_caching': True, 'time_unit': 'MILLISECONDS'},
                                              {'enable_local_caching': True, 'time_unit': 'MINUTES'},
                                              {'enable_local_caching': True, 'time_unit': 'NANOSECONDS'},
                                              {'enable_local_caching': True, 'time_unit': 'SECONDS'}])
def test_time_unit(sdc_builder, sdc_executor, stage_attributes):
    pass

