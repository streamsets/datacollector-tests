# Copyright 2020 StreamSets Inc.
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

"""
The tests in this module are for running high-volume pipelines, for the purpose of performance testing.
"""

import uuid
import pytest
import sqlalchemy
import string
from streamsets.testframework.markers import database
from streamsets.testframework.utils import get_random_string, logger


@database('sqlserver')
def test_sql_server_change_tracking_origin_default(sdc_builder, sdc_executor, database, benchmark):
    """
        Performance benchmark a simple SQL Server Change Tracking to trash pipeline.

        sqlserver_change_tracking >> trash
    """
    if not database.is_ct_enabled:
        pytest.skip('Test only runs against SQL Server with CT enabled.')

    number_of_records = 10000

    table_name, rows_in_database, table = _create_table(number_of_records)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    sql_server_change_tracking = pipeline_builder.add_stage('SQL Server Change Tracking Client')
    sql_server_change_tracking.set_attributes(
        table_configs=[{'initialOffset': 0, 'schema': 'dbo', 'tablePattern': f'{table_name}'}])

    trash = pipeline_builder.add_stage('Trash')

    sql_server_change_tracking >> trash
    pipeline = pipeline_builder.build('SQL Server Change Tracking').configure_for_environment(database)

    try:
        table.create(database.engine)

        connection = database.engine.connect()

        # enable change tracking on table
        connection.execute(f'ALTER TABLE {table_name} ENABLE change_tracking WITH (track_columns_updated = on)')

        # insert sample data
        logger.info('Adding %s rows into %s...', len(rows_in_database), table_name)
        connection.execute(table.insert(), rows_in_database)

        def benchmark_pipeline(executor, pipeline):
            pipeline.id = str(uuid.uuid4())
            executor.add_pipeline(pipeline)
            executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(rows_in_database),
                                                                                     timeout_sec=3600)
            executor.stop_pipeline(pipeline).wait_for_stopped()
            executor.remove_pipeline(pipeline)

        benchmark.pedantic(benchmark_pipeline, args=(sdc_executor, pipeline), rounds=2)

    finally:
        logger.info('Dropping table')
        connection.execute(table.delete(), rows_in_database)
        table.drop(database.engine)


def _create_table(num_rows):
    table_name = get_random_string(string.ascii_lowercase, 20)

    table = sqlalchemy.Table(table_name,
                             sqlalchemy.MetaData(),
                             sqlalchemy.Column('ID', sqlalchemy.Integer, primary_key=True),
                             sqlalchemy.Column('FIELD1', sqlalchemy.String(255)),
                             sqlalchemy.Column('FIELD2', sqlalchemy.String(255)),
                             sqlalchemy.Column('FIELD3', sqlalchemy.String(255)),
                             sqlalchemy.Column('FIELD4', sqlalchemy.String(255)),
                             sqlalchemy.Column('FIELD5', sqlalchemy.String(255)),
                             sqlalchemy.Column('FIELD6', sqlalchemy.String(255)),
                             sqlalchemy.Column('FIELD7', sqlalchemy.String(255)),
                             sqlalchemy.Column('FIELD8', sqlalchemy.String(255)),
                             sqlalchemy.Column('FIELD9', sqlalchemy.String(255)),
                             sqlalchemy.Column('FIELD10', sqlalchemy.String(255)),
                             sqlalchemy.Column('FIELD11', sqlalchemy.String(255)),
                             sqlalchemy.Column('FIELD12', sqlalchemy.String(255)),
                             sqlalchemy.Column('FIELD13', sqlalchemy.String(255)),
                             sqlalchemy.Column('FIELD14', sqlalchemy.String(255)),
                             sqlalchemy.Column('FIELD15', sqlalchemy.String(255)),
                             sqlalchemy.Column('FIELD16', sqlalchemy.String(255)),
                             sqlalchemy.Column('FIELD17', sqlalchemy.String(255)),
                             sqlalchemy.Column('FIELD18', sqlalchemy.String(255)),
                             sqlalchemy.Column('FIELD19', sqlalchemy.String(255)),
                             sqlalchemy.Column('FIELD20', sqlalchemy.String(255)))

    rows_in_database = [{'id': i, 'FIELD1': get_random_string(string.ascii_lowercase, 20),
                         'FIELD2': get_random_string(string.ascii_lowercase, 20),
                         'FIELD3': get_random_string(string.ascii_lowercase, 20),
                         'FIELD4': get_random_string(string.ascii_lowercase, 20),
                         'FIELD5': get_random_string(string.ascii_lowercase, 20),
                         'FIELD6': get_random_string(string.ascii_lowercase, 20),
                         'FIELD7': get_random_string(string.ascii_lowercase, 20),
                         'FIELD8': get_random_string(string.ascii_lowercase, 20),
                         'FIELD9': get_random_string(string.ascii_lowercase, 20),
                         'FIELD10': get_random_string(string.ascii_lowercase, 20),
                         'FIELD11': get_random_string(string.ascii_lowercase, 20),
                         'FIELD12': get_random_string(string.ascii_lowercase, 20),
                         'FIELD13': get_random_string(string.ascii_lowercase, 20),
                         'FIELD14': get_random_string(string.ascii_lowercase, 20),
                         'FIELD15': get_random_string(string.ascii_lowercase, 20),
                         'FIELD16': get_random_string(string.ascii_lowercase, 20),
                         'FIELD17': get_random_string(string.ascii_lowercase, 20),
                         'FIELD18': get_random_string(string.ascii_lowercase, 20),
                         'FIELD19': get_random_string(string.ascii_lowercase, 20),
                         'FIELD20': get_random_string(string.ascii_lowercase, 20)} for i in range(0, num_rows)]

    return table_name, rows_in_database, table
