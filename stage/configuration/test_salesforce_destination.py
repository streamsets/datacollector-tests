import pytest

from streamsets.testframework.decorators import stub


@stub
def test_api_version(sdc_builder, sdc_executor):
    pass


@stub
def test_auth_endpoint(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'default_operation': 'DELETE'},
                                              {'default_operation': 'INSERT'},
                                              {'default_operation': 'UNDELETE'},
                                              {'default_operation': 'UPDATE'},
                                              {'default_operation': 'UPSERT'}])
def test_default_operation(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_external_id_field(sdc_builder, sdc_executor):
    pass


@stub
def test_field_mapping(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_mutual_authentication': True}])
def test_keystore_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_mutual_authentication': True}])
def test_keystore_key_algorithm(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_mutual_authentication': True}])
def test_keystore_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'keystore_type': 'JKS', 'use_mutual_authentication': True},
                                              {'keystore_type': 'PKCS12', 'use_mutual_authentication': True}])
def test_keystore_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_password(sdc_builder, sdc_executor):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': True}])
def test_proxy_hostname(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'proxy_requires_credentials': True, 'use_proxy': True}])
def test_proxy_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': True}])
def test_proxy_port(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'proxy_requires_credentials': True, 'use_proxy': True}])
def test_proxy_realm(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'proxy_requires_credentials': False, 'use_proxy': True},
                                              {'proxy_requires_credentials': True, 'use_proxy': True}])
def test_proxy_requires_credentials(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'proxy_requires_credentials': True, 'use_proxy': True}])
def test_proxy_username(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_sobject_type(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'unsupported_operation_handling': 'DISCARD'},
                                              {'unsupported_operation_handling': 'SEND_TO_ERROR'},
                                              {'unsupported_operation_handling': 'USE_DEFAULT'}])
def test_unsupported_operation_handling(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_bulk_api': False}, {'use_bulk_api': True}])
def test_use_bulk_api(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_mutual_authentication': False}, {'use_mutual_authentication': True}])
def test_use_mutual_authentication(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_proxy': False}, {'use_proxy': True}])
def test_use_proxy(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_username(sdc_builder, sdc_executor):
    pass

