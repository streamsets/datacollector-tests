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
def test_hadoop_configuration_directory(sdc_builder, sdc_executor):
    pass


@stub
def test_jdbc_driver_name(sdc_builder, sdc_executor):
    pass


@stub
def test_jdbc_url(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
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
def test_sql_queries(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'stop_on_query_failure': False}, {'stop_on_query_failure': True}])
def test_stop_on_query_failure(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': False}, {'use_credentials': True}])
def test_use_credentials(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': True}])
def test_username(sdc_builder, sdc_executor, stage_attributes):
    pass
