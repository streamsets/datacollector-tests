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
from streamsets.testframework.markers import database


@database('sqlserver')
def test_defaults(sdc_builder, sdc_executor, database, origin_table):
    """Benchmark SQL Server Change Tracking origin with default settings"""

    if not database.is_ct_enabled:
        pytest.skip('Test only runs against SQL Server with CT enabled.')

    number_of_records = 5_000_000

    pipeline_builder = sdc_builder.get_pipeline_builder()
    benchmark_stages = pipeline_builder.add_benchmark_stages()

    sql_server_change_tracking_client = pipeline_builder.add_stage('SQL Server Change Tracking Client')
    sql_server_change_tracking_client.set_attributes(
        table_configs=[{'initialOffset': 0, 'schema': 'dbo', 'tablePattern': f'{origin_table.name}'}])

    sql_server_change_tracking_client >> benchmark_stages.destination

    pipeline = pipeline_builder.build('SQL Server Change Tracking').configure_for_environment(database)

    enable_ct_sql = f'ALTER TABLE {origin_table.name} ENABLE change_tracking WITH (track_columns_updated = on)'
    origin_table.load_records(number_of_records, run_stmt_after_create_table=enable_ct_sql)

    sdc_executor.benchmark_pipeline(pipeline, record_count=number_of_records, skip_metrics=['file_descriptors'])
