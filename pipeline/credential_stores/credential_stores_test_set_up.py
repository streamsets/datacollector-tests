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

import logging

DEFAULT_ID = 1
DEFAULT_NAME = 'Bulbasaur'

DEFAULT_USERNAME_FIELD = 'username'
DEFAULT_PASSWORD_FIELD = 'password'

logger = logging.getLogger(__name__)


def _create_test_pipeline(sdc_builder, sdc_executor, database, credential_store, table_name, username, password):
    logger.info('Creating the pipeline...')
    pipeline_builder = sdc_builder.get_pipeline_builder()

    jdbc_multitable_consumer = pipeline_builder.add_stage('JDBC Multitable Consumer')
    jdbc_multitable_consumer.set_attributes(
        table_configs=[{"tablePattern": f'%{table_name}%'}]
    )

    wiretap = pipeline_builder.add_wiretap()

    jdbc_multitable_consumer >> wiretap.destination
    pipeline = pipeline_builder.build().configure_for_environment(database, credential_store)
    sdc_executor.add_pipeline(pipeline)

    logger.info('Updating the pipeline to use the credential store secrets')
    jdbc_multitable_consumer = pipeline.stages.get(label=jdbc_multitable_consumer.label)
    jdbc_multitable_consumer.set_attributes(
        username=username,
        password=password
    )
    sdc_executor.update_pipeline(pipeline)

    return pipeline, wiretap


def _create_and_populate_table(database, table_name):
    logger.info(f'Creating table \'{table_name}\'...')
    connection = database.engine.connect()
    connection.execute(f'CREATE TABLE {table_name}(id int, name varchar(32), primary key (id))')

    logger.info('Adding a record into the table...')
    connection.execute(f'INSERT INTO {table_name} VALUES ({DEFAULT_ID}, \'{DEFAULT_NAME}\')')


def _check_pipeline_records(records):
    # We should have at least one record
    assert len(records) == 1
    record = records[0]

    assert 'id' in record.field
    assert record.field['id'] == DEFAULT_ID
    assert 'name' in record.field
    assert record.field['name'] == DEFAULT_NAME


def _drop_table(connection, table_name):
    if connection is not None:
        logger.info(f'Dropping table {table_name}...')
        connection.execute(f'DROP TABLE IF EXISTS {table_name}')
        connection.close()


def _stop_pipeline(sdc_executor, pipeline):
    if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
        sdc_executor.stop_pipeline(pipeline)
