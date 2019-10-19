import pytest

from streamsets.testframework.decorators import stub


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'AWSSIGV4', 'use_security': True}])
def test_access_key_id(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_additional_http_params(sdc_builder, sdc_executor):
    pass


@stub
def test_additional_properties(sdc_builder, sdc_executor):
    pass


@stub
def test_cluster_http_uris(sdc_builder, sdc_executor):
    pass


@stub
def test_data_charset(sdc_builder, sdc_executor):
    pass


@stub
def test_data_time_zone(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'default_operation': 'CREATE'},
                                              {'default_operation': 'DELETE'},
                                              {'default_operation': 'INDEX'},
                                              {'default_operation': 'MERGE'},
                                              {'default_operation': 'UPDATE'}])
def test_default_operation(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'detect_additional_nodes_in_cluster': False},
                                              {'detect_additional_nodes_in_cluster': True}])
def test_detect_additional_nodes_in_cluster(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_document_id(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'AWSSIGV4', 'region': 'OTHER', 'use_security': True}])
def test_endpoint(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_index(sdc_builder, sdc_executor):
    pass


@stub
def test_mapping(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'AWSSIGV4', 'use_security': True},
                                              {'mode': 'BASIC', 'use_security': True}])
def test_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_parent_id(sdc_builder, sdc_executor):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'AWSSIGV4', 'region': 'AP_NORTHEAST_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'AP_NORTHEAST_2', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'AP_NORTHEAST_3', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'AP_SOUTHEAST_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'AP_SOUTHEAST_2', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'AP_SOUTH_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'CA_CENTRAL_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'CN_NORTHWEST_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'CN_NORTH_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'EU_CENTRAL_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'EU_WEST_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'EU_WEST_2', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'EU_WEST_3', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'OTHER', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'SA_EAST_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'US_EAST_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'US_EAST_2', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'US_GOV_WEST_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'US_WEST_1', 'use_security': True},
                                              {'mode': 'AWSSIGV4', 'region': 'US_WEST_2', 'use_security': True}])
def test_region(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_routing(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'AWSSIGV4', 'use_security': True}])
def test_secret_access_key(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'BASIC', 'use_security': True}])
def test_security_username_and_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_security': True}])
def test_ssl_truststore_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_security': True}])
def test_ssl_truststore_path(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_time_basis(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'unsupported_operation_handling': 'DISCARD'},
                                              {'unsupported_operation_handling': 'SEND_TO_ERROR'},
                                              {'unsupported_operation_handling': 'USE_DEFAULT'}])
def test_unsupported_operation_handling(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_security': False}, {'use_security': True}])
def test_use_security(sdc_builder, sdc_executor, stage_attributes):
    pass

