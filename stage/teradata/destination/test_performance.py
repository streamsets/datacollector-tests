# Copyright 2024 StreamSets Inc.
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
import string

import pytest

from stage.utils.common import cleanup
from . import (
    pytestmark,
    stage_name,
    teradata_manager,
    LOCAL_STAGING_LOCATION,
    DEFAULT_EXTERNAL_STAGING_LOCATION,
    ALL_STAGING_FILE_FORMATS
)
from stage.utils.data_loading_stages import (
    data_loading_pipeline_handler_creator,
    data_loading_multithread_record_creator
)


def _test_performance(teradata, cleanup, teradata_manager, stage_name, data_loading_pipeline_handler_creator,
                      data_loading_multithread_record_creator, staging_location, staging_file_format,
                      number_of_threads, records_to_be_generated, batch_size):
    """
        Performance test. Similar to test_multithreading.
    """
    # describing the tables helps us to clean them up
    [teradata_manager.describe_table(table_name=table_name) for table_name in data_loading_multithread_record_creator.table_names]
    authorization = teradata_manager.create_authorization()

    teradata_attributes = {
        'staging_file_format': staging_file_format,
        'staging_location': staging_location,
        'table': data_loading_multithread_record_creator.generate_table_expression('ID'),
        'authorization_name': authorization.name,
        'auto_create_table': True,
        'enable_data_drift': True,
        'maximum_connection_threads': number_of_threads
    }

    pipeline_handler = data_loading_pipeline_handler_creator.create()
    benchmark_stages = pipeline_handler.add_benchmark_stages(number_of_threads=number_of_threads,
                                                             batch_size_in_recs=batch_size)

    teradata_destination = pipeline_handler.destination(attributes=teradata_attributes)

    benchmark_stages.origin >> teradata_destination

    pipeline_handler.build(teradata).benchmark(record_count=records_to_be_generated)


@pytest.mark.parametrize('staging_location', LOCAL_STAGING_LOCATION)
@pytest.mark.parametrize('staging_file_format', ['CSV'])
@pytest.mark.parametrize('records_to_be_generated', [2_000_000])
@pytest.mark.parametrize('number_of_threads, number_of_tables, batch_size', [
    [1, 1, 100_000],
    # [1, 3, 200_000],
    # [1, 5, 200_000],
])
def test_fastload_performance(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                              data_loading_multithread_record_creator, data_loading_pipeline_handler_creator,
                              staging_location, staging_file_format, number_of_threads, number_of_tables,
                              records_to_be_generated, batch_size):
    """
        Performance test. While it is possible to FastLoad and multithread, we get the following:
        '[SQLState HY000] Too many load/unload tasks running: try again later'. We should avoid multithreading while
         using FastLoad in favor or larger batch sizes.
    """

    _test_performance(teradata, cleanup, teradata_manager, stage_name, data_loading_pipeline_handler_creator,
                      data_loading_multithread_record_creator, staging_location, staging_file_format,
                      number_of_threads, records_to_be_generated, batch_size)


@pytest.mark.parametrize('staging_location', DEFAULT_EXTERNAL_STAGING_LOCATION)
@pytest.mark.parametrize('staging_file_format', ALL_STAGING_FILE_FORMATS)
@pytest.mark.parametrize('records_to_be_generated', [2_000_000])
@pytest.mark.parametrize('number_of_threads, number_of_tables, batch_size', [
    [1, 1, 20_000],
    [3, 3, 50_000],
    [5, 5, 100_000],
])
def test_external_performance(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                              data_loading_multithread_record_creator, data_loading_pipeline_handler_creator,
                              staging_location, staging_file_format, number_of_threads, number_of_tables,
                              records_to_be_generated, batch_size):
    """
        Assert multithreading behaves properly. We test using ADLS_GEN2 to avoid too much repetition, as
        this test is expensive.
    """
    _test_performance(teradata, cleanup, teradata_manager, stage_name, data_loading_pipeline_handler_creator,
                      data_loading_multithread_record_creator, staging_location, staging_file_format,
                      number_of_threads, records_to_be_generated, batch_size)
