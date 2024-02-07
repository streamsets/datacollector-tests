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


def _test_multithreading(teradata, cleanup, teradata_manager, stage_name, data_loading_pipeline_handler_creator,
                         data_loading_multithread_record_creator, staging_location, staging_file_format,
                         number_of_threads, records_to_be_generated):
    """
        Assert multithreading behaves properly. We create N tables with the format STF_TABLE_N_rand,
        and assign each record one table by doing %N to the INTEGER field in them.
    """
    tables = [teradata_manager.describe_table(table_name=table_name) for table_name in data_loading_multithread_record_creator.table_names]
    authorization = teradata_manager.create_authorization()

    teradata_attributes = {
        'maximum_number_of_retries': 2,  # needed to retry on concurrent load collisions
        'staging_file_format': staging_file_format,
        'staging_location': staging_location,
        'table': data_loading_multithread_record_creator.generate_table_expression(),
        'authorization_name': authorization.name,
        'auto_create_table': True,
        'enable_data_drift': True,
        'maximum_connection_threads': number_of_threads
    }

    pipeline_handler = data_loading_pipeline_handler_creator.create()
    dev_data_generator = pipeline_handler.dev_data_generator(
        records_to_be_generated=records_to_be_generated,
        number_of_threads=number_of_threads
    )
    teradata_destination = pipeline_handler.destination(attributes=teradata_attributes)
    wiretap = pipeline_handler.wiretap()

    dev_data_generator >> [teradata_destination, wiretap.destination]

    pipeline_handler.build(teradata).run(timeout_sec=600)
    wiretap_data_per_table = data_loading_multithread_record_creator.get_wiretap_data_per_table(wiretap)

    for table in tables:
        data_from_database = teradata_manager.select_from_table(table)
        data_from_database = data_loading_multithread_record_creator.sort_by_column_values(data_from_database)
        wiretap_data = wiretap_data_per_table[table.name]
        wiretap_data = data_loading_multithread_record_creator.sort_by_column_values(wiretap_data)
        assert data_from_database == wiretap_data


@pytest.mark.parametrize('staging_location', LOCAL_STAGING_LOCATION)
@pytest.mark.parametrize('staging_file_format', ['CSV'])
@pytest.mark.parametrize('number_of_threads, number_of_tables, records_to_be_generated', [
    [1, 1, 2000],
    # [2, 2, 4000],
    # [4, 4, 8000],
    # [8, 4, 8000],
    # [4, 8, 8000],
])
def test_fastload_multithread(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                              data_loading_multithread_record_creator, data_loading_pipeline_handler_creator,
                              staging_location, staging_file_format, number_of_threads, number_of_tables,
                              records_to_be_generated):
    """
        Assert multithreading behaves properly
    """
    pytest.skip('While it is possible to FastLoad and multithread, we get the following: '
                '[SQLState HY000] Too many load/unload tasks running: try again later. '
                'We should avoid multithreading while using FastLoad in favor or larger batch sizes.')

    _test_multithreading(teradata, cleanup, teradata_manager, stage_name, data_loading_pipeline_handler_creator,
                         data_loading_multithread_record_creator, staging_location, staging_file_format,
                         number_of_threads, records_to_be_generated)


@pytest.mark.parametrize('staging_location', DEFAULT_EXTERNAL_STAGING_LOCATION)
@pytest.mark.parametrize('staging_file_format', ALL_STAGING_FILE_FORMATS)
@pytest.mark.parametrize('number_of_threads, number_of_tables, records_to_be_generated', [
    [1, 1, 2000],
    [2, 2, 4000],
    [4, 4, 8000],
    [8, 4, 8000],
    [4, 8, 8000],
])
def test_external_multithread(sdc_builder, sdc_executor, teradata, cleanup, teradata_manager, stage_name,
                              data_loading_multithread_record_creator, data_loading_pipeline_handler_creator,
                              staging_location, staging_file_format, number_of_threads, number_of_tables,
                              records_to_be_generated):
    """
        Assert multithreading behaves properly. We test using ADLS_GEN2 to avoid too much repetition, as
        this test is expensive.
    """
    _test_multithreading(teradata, cleanup, teradata_manager, stage_name, data_loading_pipeline_handler_creator,
                         data_loading_multithread_record_creator, staging_location, staging_file_format,
                         number_of_threads, records_to_be_generated)
