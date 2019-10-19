import pytest

from streamsets.testframework.decorators import stub


@stub
def test_additional_files(sdc_builder, sdc_executor):
    pass


@stub
def test_additional_jars(sdc_builder, sdc_executor):
    pass


@stub
def test_additional_spark_arguments(sdc_builder, sdc_executor):
    pass


@stub
def test_additional_spark_arguments_and_values(sdc_builder, sdc_executor):
    pass


@stub
def test_application_arguments(sdc_builder, sdc_executor):
    pass


@stub
def test_application_name(sdc_builder, sdc_executor):
    pass


@stub
def test_application_resource(sdc_builder, sdc_executor):
    pass


@stub
def test_custom_java_home(sdc_builder, sdc_executor):
    pass


@stub
def test_custom_spark_home(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'language': 'PYTHON'}])
def test_dependencies(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'deploy_mode': 'CLIENT'}, {'deploy_mode': 'CLUSTER'}])
def test_deploy_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_driver_memory(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'dynamic_allocation': False}, {'dynamic_allocation': True}])
def test_dynamic_allocation(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_verbose_logging': False}, {'enable_verbose_logging': True}])
def test_enable_verbose_logging(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_environment_variables(sdc_builder, sdc_executor):
    pass


@stub
def test_executor_memory(sdc_builder, sdc_executor):
    pass


@stub
def test_kerberos_keytab(sdc_builder, sdc_executor):
    pass


@stub
def test_kerberos_principal(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'language': 'JVM'}, {'language': 'PYTHON'}])
def test_language(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'language': 'JVM'}])
def test_main_class(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'dynamic_allocation': True}])
def test_maximum_number_of_worker_nodes(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'deploy_mode': 'CLUSTER'}])
def test_maximum_time_to_wait_in_ms(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'dynamic_allocation': True}])
def test_minimum_number_of_worker_nodes(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'dynamic_allocation': False}])
def test_number_of_worker_nodes(sdc_builder, sdc_executor, stage_attributes):
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
def test_proxy_user(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass

