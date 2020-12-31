import logging
import pytest

from streamsets.testframework.markers import salesforce, sdc_min_version
from streamsets.testframework.decorators import stub
from ..utils.utils_salesforce import set_up_random, TEST_DATA, get_dev_raw_data_source, _insert_data_and_verify_using_wiretap

logger = logging.getLogger(__name__)

@salesforce
@pytest.fixture(autouse=True)
def _set_up_random(salesforce):
    set_up_random(salesforce)

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


@salesforce
@sdc_min_version('3.16.0')
@pytest.mark.parametrize('use_bulk_api', [True, False])
@pytest.mark.parametrize('stage_attributes', [{'multiple_values_behavior': 'FIRST_ONLY'},
                                              {'multiple_values_behavior': 'ALL_AS_LIST'},
                                              {'multiple_values_behavior': 'SPLIT_INTO_MULTIPLE_RECORDS'}])
def test_multiple_values_behavior(sdc_builder, sdc_executor, salesforce, stage_attributes, use_bulk_api):
    """
    The pipeline looks like:
        dev_raw_data_source >> salesforce_lookup >> wiretap

    Args:
        sdc_builder (:py:class:`streamsets.testframework.Platform`): Platform instance
        sdc_executor (:py:class:`streamsets.sdk.DataCollector`): Data Collector executor instance
        salesforce (:py:class:`testframework.environments.SalesforceInstance`): Salesforce environment
        stage_attributes (:obj:`dict`): Attributes to use in test
        use_bulk_api (:obj:`bool`): Whether or not to use the Salesforce Bulk API
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    data_to_insert = TEST_DATA['DATA_TO_INSERT']

    lookup_data = ['Email'] + [row['Email'] for row in data_to_insert]
    dev_raw_data_source = get_dev_raw_data_source(pipeline_builder, lookup_data)

    salesforce_lookup = pipeline_builder.add_stage('Salesforce Lookup')
    # Non-selective query matches all three contacts
    query_str = ("SELECT Id, FirstName, LastName, LeadSource FROM Contact "
                 f"WHERE LastName='{TEST_DATA['STR_15_RANDOM']}' ORDER BY FirstName")

    salesforce_lookup.set_attributes(**stage_attributes,
                                     soql_query=query_str,
                                     use_bulk_api=use_bulk_api)

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> salesforce_lookup >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(salesforce)
    sdc_executor.add_pipeline(pipeline)

    if stage_attributes['multiple_values_behavior'] == 'SPLIT_INTO_MULTIPLE_RECORDS':
        lookup_expected_data = [{'FirstName': 'Test1', 'LastName': TEST_DATA['STR_15_RANDOM'], 'Email': 'xtest1@example.com',
                                 'LeadSource': 'Advertisement'},
                                {'FirstName': 'Test2', 'LastName': TEST_DATA['STR_15_RANDOM'], 'Email': 'xtest1@example.com',
                                 'LeadSource': 'Partner'},
                                {'FirstName': 'Test3', 'LastName': TEST_DATA['STR_15_RANDOM'], 'Email': 'xtest1@example.com',
                                 'LeadSource': 'Web'},
                                {'FirstName': 'Test1', 'LastName': TEST_DATA['STR_15_RANDOM'], 'Email': 'xtest2@example.com',
                                 'LeadSource': 'Advertisement'},
                                {'FirstName': 'Test2', 'LastName': TEST_DATA['STR_15_RANDOM'], 'Email': 'xtest2@example.com',
                                 'LeadSource': 'Partner'},
                                {'FirstName': 'Test3', 'LastName': TEST_DATA['STR_15_RANDOM'], 'Email': 'xtest2@example.com',
                                 'LeadSource': 'Web'},
                                {'FirstName': 'Test1', 'LastName': TEST_DATA['STR_15_RANDOM'], 'Email': 'xtest3@example.com',
                                 'LeadSource': 'Advertisement'},
                                {'FirstName': 'Test2', 'LastName': TEST_DATA['STR_15_RANDOM'], 'Email': 'xtest3@example.com',
                                 'LeadSource': 'Partner'},
                                {'FirstName': 'Test3', 'LastName': TEST_DATA['STR_15_RANDOM'], 'Email': 'xtest3@example.com',
                                 'LeadSource': 'Web'}]
    elif stage_attributes['multiple_values_behavior'] == 'ALL_AS_LIST':
        lookup_expected_data = [{'FirstName': ['Test1', 'Test2', 'Test3'],
                                 'LastName': [TEST_DATA['STR_15_RANDOM'], TEST_DATA['STR_15_RANDOM'], TEST_DATA['STR_15_RANDOM']],
                                 'Email': 'xtest1@example.com', 'LeadSource': ['Advertisement', 'Partner', 'Web']},
                                {'FirstName': ['Test1', 'Test2', 'Test3'],
                                 'LastName': [TEST_DATA['STR_15_RANDOM'], TEST_DATA['STR_15_RANDOM'], TEST_DATA['STR_15_RANDOM']],
                                 'Email': 'xtest2@example.com', 'LeadSource': ['Advertisement', 'Partner', 'Web']},
                                {'FirstName': ['Test1', 'Test2', 'Test3'],
                                 'LastName': [TEST_DATA['STR_15_RANDOM'], TEST_DATA['STR_15_RANDOM'], TEST_DATA['STR_15_RANDOM']],
                                 'Email': 'xtest3@example.com', 'LeadSource': ['Advertisement', 'Partner', 'Web']}]
    else:  # multiple_values_behavior == FIRST_ONLY
        lookup_expected_data = [{'FirstName': 'Test1', 'LastName': TEST_DATA['STR_15_RANDOM'], 'Email': 'xtest1@example.com',
                                 'LeadSource': 'Advertisement'},
                                {'FirstName': 'Test1', 'LastName': TEST_DATA['STR_15_RANDOM'], 'Email': 'xtest2@example.com',
                                 'LeadSource': 'Advertisement'},
                                {'FirstName': 'Test1', 'LastName': TEST_DATA['STR_15_RANDOM'], 'Email': 'xtest3@example.com',
                                 'LeadSource': 'Advertisement'}]

    _insert_data_and_verify_using_wiretap(sdc_executor, pipeline, wiretap, lookup_expected_data,
                                          salesforce, data_to_insert, False)


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

