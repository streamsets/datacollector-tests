import pytest

from streamsets.testframework.decorators import stub
from streamsets.testframework.environments.hortonworks import AmbariCluster
from streamsets.testframework.utils import Version


@pytest.fixture(autouse=True)
def hive_check(cluster, sdc_builder):
    # based on SDC-13915
    if (isinstance(cluster, AmbariCluster) and Version(cluster.version) == Version('3.1')
        and Version(sdc_builder.version) < Version('3.8.1')):
        pytest.skip('Hive stages not available on HDP 3.1.0.0 for SDC versions before 3.8.1')


@stub
def test_additional_hadoop_configuration(sdc_builder, sdc_executor):
    pass


@stub
def test_additional_jdbc_configuration_properties(sdc_builder, sdc_executor):
    pass


@stub
def test_column_comment(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'convert_timestamps_to_string': False},
                                              {'convert_timestamps_to_string': True}])
def test_convert_timestamps_to_string(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'data_format': 'AVRO'}, {'data_format': 'PARQUET'}])
def test_data_format(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_data_time_zone(sdc_builder, sdc_executor):
    pass


@stub
def test_database_expression(sdc_builder, sdc_executor):
    pass


@stub
def test_decimal_precision_expression(sdc_builder, sdc_executor):
    pass


@stub
def test_decimal_scale_expression(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'external_table': False}, {'external_table': True}])
def test_external_table(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_hadoop_configuration_directory(sdc_builder, sdc_executor):
    pass


@stub
def test_header_attribute_expressions(sdc_builder, sdc_executor):
    pass


@stub
def test_jdbc_driver_name(sdc_builder, sdc_executor):
    pass


@stub
def test_jdbc_url(sdc_builder, sdc_executor):
    pass


@stub
def test_max_cache_size_in_entries(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_partition_configuration(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'external_table': True}])
def test_partition_path_template(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': True}])
def test_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_table_name(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'external_table': True}])
def test_table_path_template(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_time_basis(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': False}, {'use_credentials': True}])
def test_use_credentials(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': True}])
def test_username(sdc_builder, sdc_executor, stage_attributes):
    pass
