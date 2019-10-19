import pytest

from streamsets.testframework.decorators import stub


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
@pytest.mark.parametrize('stage_attributes', [{'stored_as_avro': False}])
def test_hdfs_user(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'stored_as_avro': False}])
def test_schema_folder_location(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'stored_as_avro': False}, {'stored_as_avro': True}])
def test_stored_as_avro(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': False}, {'use_credentials': True}])
def test_use_credentials(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': True}])
def test_username(sdc_builder, sdc_executor, stage_attributes):
    pass

