import pytest

from streamsets.testframework.decorators import stub


@stub
def test_api_version(sdc_builder, sdc_executor):
    pass


@stub
def test_auth_endpoint(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'create_salesforce_attributes': False},
                                              {'create_salesforce_attributes': True}])
def test_create_salesforce_attributes(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': False}, {'enable_local_caching': True}])
def test_enable_local_caching(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': True,
                                               'eviction_policy_type': 'EXPIRE_AFTER_ACCESS'},
                                              {'enable_local_caching': True,
                                               'eviction_policy_type': 'EXPIRE_AFTER_WRITE'}])
def test_eviction_policy_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': True}])
def test_expiration_time(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_field_mappings(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'lookup_mode': 'RETRIEVE'}])
def test_id_field(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'include_deleted_records': False, 'lookup_mode': 'QUERY'},
                                              {'include_deleted_records': True, 'lookup_mode': 'QUERY'}])
def test_include_deleted_records(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'lookup_mode': 'QUERY'}, {'lookup_mode': 'RETRIEVE'}])
def test_lookup_mode(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': True}])
def test_maximum_entries_to_cache(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'mismatched_types_behavior': 'PRESERVE_DATA'},
                                              {'mismatched_types_behavior': 'ROUND_DATA'},
                                              {'mismatched_types_behavior': 'TRUNCATE_DATA'}])
def test_mismatched_types_behavior(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'missing_values_behavior': 'PASS_RECORD_ON'},
                                              {'missing_values_behavior': 'SEND_TO_ERROR'}])
def test_missing_values_behavior(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'multiple_values_behavior': 'FIRST_ONLY'},
                                              {'multiple_values_behavior': 'SPLIT_INTO_MULTIPLE_RECORDS'}])
def test_multiple_values_behavior(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'lookup_mode': 'RETRIEVE'}])
def test_object_type(sdc_builder, sdc_executor, stage_attributes):
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
@pytest.mark.parametrize('stage_attributes', [{'create_salesforce_attributes': True}])
def test_salesforce_attribute_prefix(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'lookup_mode': 'RETRIEVE'}])
def test_salesforce_fields(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'lookup_mode': 'QUERY'}])
def test_soql_query(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'enable_local_caching': True, 'time_unit': 'DAYS'},
                                              {'enable_local_caching': True, 'time_unit': 'HOURS'},
                                              {'enable_local_caching': True, 'time_unit': 'MICROSECONDS'},
                                              {'enable_local_caching': True, 'time_unit': 'MILLISECONDS'},
                                              {'enable_local_caching': True, 'time_unit': 'MINUTES'},
                                              {'enable_local_caching': True, 'time_unit': 'NANOSECONDS'},
                                              {'enable_local_caching': True, 'time_unit': 'SECONDS'}])
def test_time_unit(sdc_builder, sdc_executor, stage_attributes):
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

