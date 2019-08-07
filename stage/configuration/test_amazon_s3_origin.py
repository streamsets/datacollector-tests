import pytest


@pytest.mark.skip('Not yet implemented')
def test_configuration_access_key_id(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_bucket(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_connection_timeout(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('task', ['CREATE_NEW_OBJECT'])
@pytest.mark.skip('Not yet implemented')
def test_configuration_content(sdc_builder, sdc_executor, task):
    pass


@pytest.mark.parametrize('task', ['COPY_OBJECT'])
@pytest.mark.parametrize('delete_original_object', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_configuration_delete_original_object(sdc_builder, sdc_executor, task, delete_original_object):
    pass


@pytest.mark.parametrize('region', ['OTHER'])
@pytest.mark.skip('Not yet implemented')
def test_configuration_endpoint(sdc_builder, sdc_executor, region):
    pass


@pytest.mark.parametrize('task', ['COPY_OBJECT'])
@pytest.mark.skip('Not yet implemented')
def test_configuration_new_object_path(sdc_builder, sdc_executor, task):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_object(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('on_record_error', ['DISCARD', 'STOP_PIPELINE', 'TO_ERROR'])
@pytest.mark.skip('Not yet implemented')
def test_configuration_on_record_error(sdc_builder, sdc_executor, on_record_error):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_preconditions(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('use_proxy', [True])
@pytest.mark.skip('Not yet implemented')
def test_configuration_proxy_host(sdc_builder, sdc_executor, use_proxy):
    pass


@pytest.mark.parametrize('use_proxy', [True])
@pytest.mark.skip('Not yet implemented')
def test_configuration_proxy_password(sdc_builder, sdc_executor, use_proxy):
    pass


@pytest.mark.parametrize('use_proxy', [True])
@pytest.mark.skip('Not yet implemented')
def test_configuration_proxy_port(sdc_builder, sdc_executor, use_proxy):
    pass


@pytest.mark.parametrize('use_proxy', [True])
@pytest.mark.skip('Not yet implemented')
def test_configuration_proxy_user(sdc_builder, sdc_executor, use_proxy):
    pass


@pytest.mark.parametrize('region', ['AP_NORTHEAST_1', 'AP_NORTHEAST_2', 'AP_NORTHEAST_3', 'AP_SOUTHEAST_1', 'AP_SOUTHEAST_2', 'AP_SOUTH_1', 'CA_CENTRAL_1', 'CN_NORTHWEST_1', 'CN_NORTH_1', 'EU_CENTRAL_1', 'EU_WEST_1', 'EU_WEST_2', 'EU_WEST_3', 'OTHER', 'SA_EAST_1', 'US_EAST_1', 'US_EAST_2', 'US_GOV_WEST_1', 'US_WEST_1', 'US_WEST_2'])
@pytest.mark.skip('Not yet implemented')
def test_configuration_region(sdc_builder, sdc_executor, region):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_required_fields(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_retry_count(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_secret_access_key(sdc_builder, sdc_executor):
    pass


@pytest.mark.skip('Not yet implemented')
def test_configuration_socket_timeout(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('task', ['CHANGE_EXISTING_OBJECT'])
@pytest.mark.skip('Not yet implemented')
def test_configuration_tags(sdc_builder, sdc_executor, task):
    pass


@pytest.mark.parametrize('task', ['CHANGE_EXISTING_OBJECT', 'COPY_OBJECT', 'CREATE_NEW_OBJECT'])
@pytest.mark.skip('Not yet implemented')
def test_configuration_task(sdc_builder, sdc_executor, task):
    pass


@pytest.mark.parametrize('use_proxy', [False, True])
@pytest.mark.skip('Not yet implemented')
def test_configuration_use_proxy(sdc_builder, sdc_executor, use_proxy):
    pass
