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
import logging

import pytest
from streamsets.testframework.decorators import stub
from streamsets.testframework.markers import category

logger = logging.getLogger(__name__)


pytestmark = pytest.mark.database('mysql')


@stub
@category('advanced')
def test_mysql_binary_log_batch_wait_time_in_ms(sdc_builder, sdc_executor, database):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'ssl_mode': 'VERIFY_CA'}, {'ssl_mode': 'VERIFY_IDENTITY'}])
def test_mysql_binary_log_ca_certificate_pem(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('advanced')
def test_mysql_binary_log_connect_timeout_in_ms(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_mysql_binary_log_connection_health_test_query(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'enable_keepalive_thread': False}, {'enable_keepalive_thread': True}])
def test_mysql_binary_log_enable_keepalive_thread(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('basic')
def test_mysql_binary_log_hostname(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_mysql_binary_log_ignore_tables(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_mysql_binary_log_include_tables(sdc_builder, sdc_executor, database):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'start_from_beginning': False}])
def test_mysql_binary_log_initial_offset(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('advanced')
def test_mysql_binary_log_jdbc_driver_class_name(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
@pytest.mark.parametrize('stage_attributes', [{'enable_keepalive_thread': True}])
def test_mysql_binary_log_keepalive_interval_in_ms(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('advanced')
def test_mysql_binary_log_max_batch_size_in_records(sdc_builder, sdc_executor, database):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_mysql_binary_log_on_record_error(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('basic')
def test_mysql_binary_log_password(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_mysql_binary_log_port(sdc_builder, sdc_executor, database):
    pass


@stub
@category('advanced')
def test_mysql_binary_log_server_id(sdc_builder, sdc_executor, database):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'ssl_mode': 'DISABLED'},
                                              {'ssl_mode': 'REQUIRED'},
                                              {'ssl_mode': 'VERIFY_CA'},
                                              {'ssl_mode': 'VERIFY_IDENTITY'}])
def test_mysql_binary_log_ssl_mode(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('basic')
@pytest.mark.parametrize('stage_attributes', [{'start_from_beginning': False}, {'start_from_beginning': True}])
def test_mysql_binary_log_start_from_beginning(sdc_builder, sdc_executor, database, stage_attributes):
    pass


@stub
@category('basic')
def test_mysql_binary_log_username(sdc_builder, sdc_executor, database):
    pass
