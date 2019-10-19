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
def test_max_batch_size_in_records(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_project_id(sdc_builder, sdc_executor):
    pass


@stub
def test_query(sdc_builder, sdc_executor):
    pass


@stub
def test_query_timeout_in_sec(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_cached_query_results': False}, {'use_cached_query_results': True}])
def test_use_cached_query_results(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_legacy_sql': False}, {'use_legacy_sql': True}])
def test_use_legacy_sql(sdc_builder, sdc_executor, stage_attributes):
    pass

