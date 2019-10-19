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
def test_cluster_http_uris(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'delete_scroll_on_pipeline_stop': False},
                                              {'delete_scroll_on_pipeline_stop': True}])
def test_delete_scroll_on_pipeline_stop(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'detect_additional_nodes_in_cluster': False},
                                              {'detect_additional_nodes_in_cluster': True}])
def test_detect_additional_nodes_in_cluster(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'AWSSIGV4', 'region': 'OTHER', 'use_security': True}])
def test_endpoint(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'incremental_mode': False}, {'incremental_mode': True}])
def test_incremental_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_index(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'incremental_mode': True}])
def test_initial_offset(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_mapping(sdc_builder, sdc_executor):
    pass


@stub
def test_max_batch_size(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mode': 'AWSSIGV4', 'use_security': True},
                                              {'mode': 'BASIC', 'use_security': True}])
def test_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_number_of_slices(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'incremental_mode': True}])
def test_offset_field(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_query(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'incremental_mode': True}])
def test_query_interval(sdc_builder, sdc_executor, stage_attributes):
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
def test_scroll_timeout(sdc_builder, sdc_executor):
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
@pytest.mark.parametrize('stage_attributes', [{'use_security': False}, {'use_security': True}])
def test_use_security(sdc_builder, sdc_executor, stage_attributes):
    pass

