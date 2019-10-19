import pytest

from streamsets.testframework.decorators import stub


@stub
def test_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_hbase_configuration(sdc_builder, sdc_executor):
    pass


@stub
def test_hbase_configuration_directory(sdc_builder, sdc_executor):
    pass


@stub
def test_hbase_user(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ignore_invalid_column': False, 'implicit_field_mapping': True},
                                              {'ignore_invalid_column': True, 'implicit_field_mapping': True}])
def test_ignore_invalid_column(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ignore_missing_field': False}, {'ignore_missing_field': True}])
def test_ignore_missing_field(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'implicit_field_mapping': False}, {'implicit_field_mapping': True}])
def test_implicit_field_mapping(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'kerberos_authentication': False}, {'kerberos_authentication': True}])
def test_kerberos_authentication(sdc_builder, sdc_executor, stage_attributes):
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
def test_row_key(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'storage_type': 'BINARY'}, {'storage_type': 'TEXT'}])
def test_storage_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_table_name(sdc_builder, sdc_executor):
    pass


@stub
def test_time_basis(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'validate_table_existence': False}, {'validate_table_existence': True}])
def test_validate_table_existence(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_zookeeper_client_port(sdc_builder, sdc_executor):
    pass


@stub
def test_zookeeper_parent_znode(sdc_builder, sdc_executor):
    pass


@stub
def test_zookeeper_quorum(sdc_builder, sdc_executor):
    pass

