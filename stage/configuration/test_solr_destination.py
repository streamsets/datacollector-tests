import pytest

from streamsets.testframework.decorators import stub


@stub
def test_auto_generated_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_connection_timeout_in_ms(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'instance_type': 'SOLR_CLOUD'}])
def test_default_collection_name(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'map_fields_automatically': True}])
def test_field_path_for_data(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'map_fields_automatically': False}])
def test_fields(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ignore_optional_fields': False}, {'ignore_optional_fields': True}])
def test_ignore_optional_fields(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'instance_type': 'SINGLE_NODE'}, {'instance_type': 'SOLR_CLOUD'}])
def test_instance_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'kerberos_authentication': False}, {'kerberos_authentication': True}])
def test_kerberos_authentication(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'map_fields_automatically': False}, {'map_fields_automatically': True}])
def test_map_fields_automatically(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'missing_fields': 'DISCARD'},
                                              {'missing_fields': 'STOP_PIPELINE'},
                                              {'missing_fields': 'TO_ERROR'}])
def test_missing_fields(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'record_indexing_mode': 'BATCH'}, {'record_indexing_mode': 'RECORD'}])
def test_record_indexing_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'skip_validation': False}, {'skip_validation': True}])
def test_skip_validation(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_socket_timeout_in_ms(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'soft_commit': False}, {'soft_commit': True}])
def test_soft_commit(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'instance_type': 'SINGLE_NODE'}])
def test_solr_uri(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'wait_flush': False}, {'wait_flush': True}])
def test_wait_flush(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'wait_searcher': False}, {'wait_searcher': True}])
def test_wait_searcher(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'instance_type': 'SOLR_CLOUD'}])
def test_zookeeper_connection_string(sdc_builder, sdc_executor, stage_attributes):
    pass

