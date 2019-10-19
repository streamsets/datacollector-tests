import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'credentials_provider': 'JSON'}])
def test_credentials_file_content_in_json(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'credentials_provider': 'JSON_PROVIDER'}])
def test_credentials_file_path_in_json(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'credentials_provider': 'DEFAULT_PROVIDER'},
                                              {'credentials_provider': 'JSON'},
                                              {'credentials_provider': 'JSON_PROVIDER'}])
def test_credentials_provider(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_dataset(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ignore_invalid_column': False}, {'ignore_invalid_column': True}])
def test_ignore_invalid_column(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_insert_id_expression(sdc_builder, sdc_executor):
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
def test_project_id(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_table_cache_size(sdc_builder, sdc_executor):
    pass


@stub
def test_table_name(sdc_builder, sdc_executor):
    pass

