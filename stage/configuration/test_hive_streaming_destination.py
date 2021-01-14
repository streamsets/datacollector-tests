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
@pytest.mark.parametrize('stage_attributes', [{'create_partitions': False}, {'create_partitions': True}])
def test_create_partitions(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_field_to_column_mapping(sdc_builder, sdc_executor):
    pass


@stub
def test_hive_configuration(sdc_builder, sdc_executor):
    pass


@stub
def test_hive_configuration_directory(sdc_builder, sdc_executor):
    pass


@stub
def test_hive_metastore_thrift_url(sdc_builder, sdc_executor):
    pass


@stub
def test_max_record_size_in_kb(sdc_builder, sdc_executor):
    pass


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass


@stub
def test_schema(sdc_builder, sdc_executor):
    pass


@stub
def test_table(sdc_builder, sdc_executor):
    pass


@stub
def test_transaction_batch_size(sdc_builder, sdc_executor):
    pass
