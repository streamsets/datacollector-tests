# Copyright 2021 StreamSets Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest

import json
import logging
import string

from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import cassandra
from streamsets.testframework.markers import sdc_min_version
from streamsets.testframework.utils import get_random_string


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_provider': 'DSE_PLAINTEXT'},
                                              {'authentication_provider': 'KERBEROS'},
                                              {'authentication_provider': 'NONE'},
                                              {'authentication_provider': 'PLAINTEXT'}])
def test_authentication_provider(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'batch_type': 'LOGGED'}, {'batch_type': 'UNLOGGED'}])
def test_batch_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_cassandra_contact_points(sdc_builder, sdc_executor):
    pass


@stub
def test_cassandra_port(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_cipher_suites': False, 'use_tls': True}])
def test_cipher_suites(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'compression': 'LZ4'},
                                              {'compression': 'NONE'},
                                              {'compression': 'SNAPPY'}])
def test_compression(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_connection_timeout(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'consistency_level': 'ALL'},
                                              {'consistency_level': 'ANY'},
                                              {'consistency_level': 'EACH_QUORUM'},
                                              {'consistency_level': 'LOCAL_ONE'},
                                              {'consistency_level': 'LOCAL_QUORUM'},
                                              {'consistency_level': 'LOCAL_SERIAL'},
                                              {'consistency_level': 'ONE'},
                                              {'consistency_level': 'QUORUM'},
                                              {'consistency_level': 'SERIAL'},
                                              {'consistency_level': 'THREE'},
                                              {'consistency_level': 'TWO'}])
def test_consistency_level(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'disable_batch_insert': False}, {'disable_batch_insert': True}])
def test_disable_batch_insert(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_field_to_column_mapping(sdc_builder, sdc_executor):
    pass


@cassandra
@sdc_min_version('3.12.0')
@pytest.mark.parametrize('qualified_table_name', ['CORRECT', 'INCORRECT', 'NONEXISTENT'])
def test_fully_qualified_table_name(sdc_builder, sdc_executor, cassandra, qualified_table_name):
    """Test for Cassandra stage table name configuration. Support to test Kerberos or plain text only. Tests:
    correct name creation and look up, incorrect name creation, and correct creation but looking for nonexistent name.
    The pipeline looks like:

        dev_raw_data_source >> cassandra_destination
    """
    raw_dict = [dict(contact=dict(name='Jane Smith', phone=2124050000, zip_code=27023)),
                dict(contact=dict(name='San', phone=2120998998, zip_code=14305))]
    raw_data = json.dumps(raw_dict)

    cassandra_keyspace = get_random_string(string.ascii_letters, 10)
    cassandra_table = 'contact'

    if qualified_table_name == 'CORRECT':
        fully_qualified_table_name = f'{cassandra_keyspace}.{cassandra_table}'
    elif qualified_table_name == 'INCORRECT':
        fully_qualified_table_name = f'{cassandra_keyspace}:{cassandra_table}'
    elif qualified_table_name == 'NONEXISTENT':
        cassandra_wrong_keyspace = get_random_string(string.ascii_letters, 10)
        cassandra_wrong_table = 'notcontacts'
        fully_qualified_table_name = f'{cassandra_wrong_keyspace}.{cassandra_wrong_table}'

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data,
                                       stop_after_first_batch=True)
    cassandra_destination = builder.add_stage('Cassandra', type='destination')
    cassandra_destination.set_attributes(field_to_column_mapping=[
        {'field': '/contact/name', 'columnName': 'name'},
        {'field': '/contact/zip_code', 'columnName': 'zip_code'},
        {'field': '/contact/phone', 'columnName': 'phone'}],
        fully_qualified_table_name=fully_qualified_table_name,
        protocol_version='V4')
    if cassandra.kerberos_enabled:
        cassandra_destination.set_attributes(authentication_provider='KERBEROS')
    else:
        cassandra_destination.set_attributes(authentication_provider='PLAINTEXT', password=cassandra.password,
                                             username=cassandra.username)

    dev_raw_data_source >> cassandra_destination
    pipeline = builder.build(title='Cassandra Destination pipeline').configure_for_environment(cassandra)
    sdc_executor.add_pipeline(pipeline)

    try:
        client = cassandra.client
        cluster = client.cluster
        session = client.session

        # create Cassandra required tables before pipeline can put data
        session.execute(f"CREATE KEYSPACE {cassandra_keyspace} WITH replication = "
                        f"{{'class': 'SimpleStrategy', 'replication_factor': 1}}")
        session.execute(f'CREATE TABLE {cassandra_keyspace}.{cassandra_table} '
                        f'(name text PRIMARY KEY, zip_code int, phone int)')

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # read data from Cassandra and assert to what pipeline has ingested
        rows = session.execute(f'SELECT * FROM {cassandra_keyspace}.{cassandra_table}').current_rows
        assert len(rows) == len(raw_dict)
        for index, row in enumerate(rows):
            for key, value in raw_dict[index]['contact'].items():
                assert getattr(row, key) == value
    except Exception as e:
        if qualified_table_name == 'INCORRECT':
            assert 'CASSANDRA_02' in str(e)
        elif qualified_table_name == 'NONEXISTENT':
            assert 'CASSANDRA_12' in str(e)
    finally:
        # drop table and keyspace from Cassandra
        session.execute(f'DROP TABLE {cassandra_keyspace}.{cassandra_table}')
        session.execute(f'DROP KEYSPACE {cassandra_keyspace}')
        cluster.shutdown()


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
def test_keystore_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
def test_keystore_key_algorithm(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
def test_keystore_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'keystore_type': 'JKS', 'use_tls': True},
                                              {'keystore_type': 'PKCS12', 'use_tls': True}])
def test_keystore_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'log_slow_queries': False}, {'log_slow_queries': True}])
def test_log_slow_queries(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_max_batch_size(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_provider': 'DSE_PLAINTEXT'},
                                              {'authentication_provider': 'PLAINTEXT'}])
def test_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'protocol_version': 'V1'},
                                              {'protocol_version': 'V2'},
                                              {'protocol_version': 'V3'},
                                              {'protocol_version': 'V4'},
                                              {'protocol_version': 'V5'}])
def test_protocol_version(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_read_timeout(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'disable_batch_insert': True}])
def test_request_timeout(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'log_slow_queries': True}])
def test_slow_query_logging_threshold(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_protocols': False, 'use_tls': True}])
def test_transport_protocols(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
def test_truststore_file(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
def test_truststore_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': True}])
def test_truststore_trust_algorithm(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'truststore_type': 'JKS', 'use_tls': True},
                                              {'truststore_type': 'PKCS12', 'use_tls': True}])
def test_truststore_type(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_cipher_suites': False, 'use_tls': True},
                                              {'use_default_cipher_suites': True, 'use_tls': True}])
def test_use_default_cipher_suites(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_default_protocols': False, 'use_tls': True},
                                              {'use_default_protocols': True, 'use_tls': True}])
def test_use_default_protocols(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_tls': False}, {'use_tls': True}])
def test_use_tls(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'authentication_provider': 'DSE_PLAINTEXT'},
                                              {'authentication_provider': 'PLAINTEXT'}])
def test_username(sdc_builder, sdc_executor, stage_attributes):
    pass

