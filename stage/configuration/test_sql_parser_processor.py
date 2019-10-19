import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'add_unsupported_fields_to_records': False},
                                              {'add_unsupported_fields_to_records': True}])
def test_add_unsupported_fields_to_records(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'resolve_schema_from_db': True}])
def test_additional_jdbc_configuration_properties(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'case_sensitive_names': False}, {'case_sensitive_names': True}])
def test_case_sensitive_names(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_date_format(sdc_builder, sdc_executor):
    pass


@stub
def test_db_time_zone(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'resolve_schema_from_db': True}])
def test_jdbc_connection_string(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'resolve_schema_from_db': True, 'use_credentials': True}])
def test_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'resolve_schema_from_db': False}, {'resolve_schema_from_db': True}])
def test_resolve_schema_from_db(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_sql_field(sdc_builder, sdc_executor):
    pass


@stub
def test_target_field(sdc_builder, sdc_executor):
    pass


@stub
def test_timestamp_with_local_timezone_format(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'unsupported_field_type': 'DISCARD'},
                                              {'unsupported_field_type': 'SEND_TO_PIPELINE'},
                                              {'unsupported_field_type': 'TO_ERROR'}])
def test_unsupported_field_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'resolve_schema_from_db': True, 'use_credentials': False},
                                              {'resolve_schema_from_db': True, 'use_credentials': True}])
def test_use_credentials(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'resolve_schema_from_db': True, 'use_credentials': True}])
def test_username(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_zoned_datetime_format(sdc_builder, sdc_executor):
    pass

