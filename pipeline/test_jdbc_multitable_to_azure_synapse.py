# Copyright 2023 StreamSets Inc.
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
import sqlalchemy
import string

from streamsets.testframework.markers import azure, database, sdc_min_version
from streamsets.testframework.utils import get_random_string
from stage.utils.utils_azure_synapse import STAGE_NAME, delete_table, stop_pipeline

pytestmark = [azure('synapse'), database, sdc_min_version('5.5.0')]

logger = logging.getLogger(__name__)


@pytest.mark.parametrize('no_of_threads', [1, 2, 3])
def test_jdbc_multitable_synapse_destination(sdc_builder, sdc_executor, database, azure, no_of_threads):
    """
    Test for JDBC Multitable consumer to Azure Synapse target stage.
    We do so by creating three tables, reading them with JDBC Multitable and then
    Azure Synapse destination creates them in Azure Synapse.
    Finally, data is read from database for assertion.
    A pipeline finisher is added to stop the pipeline when there is no more data.

    The pipeline looks like:
    jdbc_multitable >> azure_synapse_destination
    jdbc_multitable >= pipeline_finisher
    """
    rows_in_table = [
        [{'id': i, 'name': f'Roger Federer{i}'} for i in range(1, 100)],
        [{'id': i, 'name': f'Martin Del Potro{i}'} for i in range(1, 100)],
        [{'id': i, 'name': f'David Nalbandian{i}'} for i in range(1, 100)]
    ]
    src_table_prefix = get_random_string(string.ascii_lowercase, 6)
    table_names = [f'{src_table_prefix}_{get_random_string()}' for _ in range(3)]

    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(
        table_configs=[{"tablePattern": f'%{src_table_prefix}%'}],
        number_of_threads=no_of_threads,
        maximum_pool_size=no_of_threads
    )

    azure_synapse_destination = pipeline_builder.add_stage(name=STAGE_NAME)
    azure_synapse_destination.set_attributes(
        auto_create_table=True,
        table="${record:attribute('jdbc.tables')}",
        enable_data_drift=True,
        ignore_missing_fields=True,
        ignore_fields_with_invalid_types=True,
        purge_stage_file_after_loading=True,
        stage_file_prefix=f'stf_{get_random_string()}'
    )

    pipeline_finisher = pipeline_builder.add_stage('Pipeline Finisher Executor')
    pipeline_finisher.set_attributes(
        preconditions=['${record:eventType() == \'no-more-data\'}'],
        on_record_error='DISCARD'
    )

    jdbc_multitable_consumer >> azure_synapse_destination
    jdbc_multitable_consumer >= pipeline_finisher
    pipeline = pipeline_builder.build().configure_for_environment(database, azure)
    sdc_executor.add_pipeline(pipeline)

    table = []
    for i in range(3):
        metadata = sqlalchemy.MetaData()
        table.append(sqlalchemy.Table(
            table_names[i],
            metadata,
            sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column('name', sqlalchemy.String(32)))
        )
        logger.info('Creating table %s in %s database ...', table_names[i], database.type)
        table[i].create(database.engine)
        logger.info('Adding three rows into %s database ...', database.type)
        connection = database.engine.connect()
        connection.execute(table[i].insert(), rows_in_table[i])

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        engine = azure.synapse.engine
        for i in range(3):
            stmt = f'select * from [{azure.synapse_database_schema}].[{table_names[i]}]'
            result = engine.execute(stmt)
            data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
            result.close()
            assert data_from_database == [(row['id'], row['name']) for row in rows_in_table[i]]
    finally:
        logger.info('Dropping table %s in %s database...', table_names, database.type)
        try:
            stop_pipeline(sdc_executor, pipeline)
            for i in range(3):
                table[i].drop(database.engine)
                delete_table(engine, table_names[i], azure.synapse_database_schema)
        except Exception as ex:
            logger.error(ex)
