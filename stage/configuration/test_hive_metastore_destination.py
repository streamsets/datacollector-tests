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

from streamsets.testframework.decorators import stub
from streamsets.testframework.environments.hortonworks import AmbariCluster
from streamsets.testframework.utils import Version


@pytest.fixture(autouse=True)
def hive_check(cluster, sdc_builder):
    # based on SDC-13915
    if (isinstance(cluster, AmbariCluster) and Version(cluster.version) == Version('3.1')
        and Version(sdc_builder.version) < Version('3.8.1')):
        pytest.skip('Hive stages not available on HDP 3.1.0.0 for SDC versions before 3.8.1')


@stub
def test_additional_hadoop_configuration(sdc_builder, sdc_executor):
    pass


@stub
def test_additional_jdbc_configuration_properties(sdc_builder, sdc_executor):
    pass


@stub
def test_hadoop_configuration_directory(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'stored_as_avro': False}])
def test_hdfs_user(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_header_attribute_expressions(sdc_builder, sdc_executor):
    pass


@stub
def test_jdbc_driver_name(sdc_builder, sdc_executor):
    pass


@stub
def test_jdbc_url(sdc_builder, sdc_executor):
    pass


@stub
def test_max_cache_size_in_entries(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': True}])
def test_password(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'stored_as_avro': False}])
def test_schema_folder_location(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'stored_as_avro': False}, {'stored_as_avro': True}])
def test_stored_as_avro(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': False}, {'use_credentials': True}])
def test_use_credentials(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'use_credentials': True}])
def test_username(sdc_builder, sdc_executor, stage_attributes):
    pass
