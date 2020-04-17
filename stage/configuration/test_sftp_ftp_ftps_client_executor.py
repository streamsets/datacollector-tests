import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication': 'NONE'},
                                              {'authentication': 'PASSWORD'},
                                              {'authentication': 'PRIVATE_KEY'}])
def test_authentication(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_connection_timeout(sdc_builder, sdc_executor):
    pass


@stub
def test_data_timeout(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'file_exists_action': 'OVERWRITE', 'task': 'MOVE_FILE'},
                                              {'file_exists_action': 'TO_ERROR', 'task': 'MOVE_FILE'}])
def test_file_exists_action(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_file_name_expression(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_client_certificate_for_ftps': True}])
def test_ftps_client_certificate_keystore_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_client_certificate_for_ftps': True}])
def test_ftps_client_certificate_keystore_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ftps_client_certificate_keystore_type': 'JKS',
                                               'use_client_certificate_for_ftps': True},
                                              {'ftps_client_certificate_keystore_type': 'PKCS12',
                                               'use_client_certificate_for_ftps': True}])
def test_ftps_client_certificate_keystore_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ftps_data_channel_protection_level': 'CLEAR'},
                                              {'ftps_data_channel_protection_level': 'PRIVATE'}])
def test_ftps_data_channel_protection_level(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ftps_mode': 'EXPLICIT'}, {'ftps_mode': 'IMPLICIT'}])
def test_ftps_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ftps_truststore_provider': 'FILE'}])
def test_ftps_truststore_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ftps_truststore_provider': 'FILE'}])
def test_ftps_truststore_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ftps_truststore_provider': 'ALLOW_ALL'},
                                              {'ftps_truststore_provider': 'FILE'},
                                              {'ftps_truststore_provider': 'JVM_DEFAULT'}])
def test_ftps_truststore_provider(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'ftps_truststore_provider': 'FILE', 'ftps_truststore_type': 'JKS'},
                                              {'ftps_truststore_provider': 'FILE', 'ftps_truststore_type': 'PKCS12'}])
def test_ftps_truststore_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'strict_host_checking': True}])
def test_known_hosts_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication': 'PASSWORD'}])
def test_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'path_relative_to_user_home_directory': False},
                                              {'path_relative_to_user_home_directory': True}])
def test_path_relative_to_user_home_directory(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication': 'PRIVATE_KEY', 'private_key_provider': 'PLAIN_TEXT'}])
def test_private_key(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication': 'PRIVATE_KEY', 'private_key_provider': 'FILE'}])
def test_private_key_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication': 'PRIVATE_KEY'}])
def test_private_key_passphrase(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication': 'PRIVATE_KEY', 'private_key_provider': 'FILE'},
                                              {'authentication': 'PRIVATE_KEY', 'private_key_provider': 'PLAIN_TEXT'}])
def test_private_key_provider(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_resource_url(sdc_builder, sdc_executor):
    pass


@stub
def test_socket_timeout(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'strict_host_checking': False}, {'strict_host_checking': True}])
def test_strict_host_checking(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'task': 'MOVE_FILE'}])
def test_target_directory(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'task': 'DELETE_FILE'}, {'task': 'MOVE_FILE'}])
def test_task(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_client_certificate_for_ftps': False},
                                              {'use_client_certificate_for_ftps': True}])
def test_use_client_certificate_for_ftps(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication': 'PASSWORD'}, {'authentication': 'PRIVATE_KEY'}])
def test_username(sdc_builder, sdc_executor, stage_attributes):
    pass

