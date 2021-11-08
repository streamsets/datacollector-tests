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

import pytest
import os
import tempfile
from time import sleep

import sqlalchemy
import logging
import string

from streamsets.testframework.environments.databases import OracleDatabase
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


def test_pipeline_downgrade(sdc_executor):
    """Ensure that when user tries to downgrade pipeline, we issue a proper error message."""
    builder = sdc_executor.get_pipeline_builder()

    generator = builder.add_stage(label='Dev Data Generator')
    trash = builder.add_stage(label='Trash')
    generator >> trash
    pipeline = builder.build()
    # We manually alter the pipeline version to some really high number
    # TLKT-561: PipelineInfo doesn't seem to be exposed in the APIs
    pipeline._data['pipelineConfig']['info']['sdcVersion'] = '99.99.99'

    sdc_executor.add_pipeline(pipeline)

    with pytest.raises(Exception) as e:
        sdc_executor.validate_pipeline(pipeline)

    assert 'VALIDATION_0096' in e.value.issues


def test_two_outputs_to_same_stage(sdc_executor):
    """Ensure proper error is thrown when two or more outputs from a single stage leads to the same stage."""
    builder = sdc_executor.get_pipeline_builder()

    generator = builder.add_stage(label='Dev Data Generator')

    selector = builder.add_stage('Stream Selector')

    trash = builder.add_stage(label='Trash')

    generator >> selector >> trash
    selector >> trash

    selector.condition = [dict(outputLane=selector.output_lanes[0], predicate='${1 == 1}'),
                          dict(outputLane=selector.output_lanes[1], predicate='default')]
    pipeline = builder.build()

    sdc_executor.add_pipeline(pipeline)

    with pytest.raises(Exception) as e:
        sdc_executor.validate_pipeline(pipeline)

    assert 'VALIDATION_0039' in e.value.issues


@sdc_min_version('3.4.0')
@database
def test_pipeline_preview_with_test_stage(sdc_builder, sdc_executor, database):
    """Test preview with test origin."""
    if isinstance(database, OracleDatabase):
        pytest.skip('This test does not support oracle and its upper casing of column names.')

    metadata = sqlalchemy.MetaData()
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column('name', sqlalchemy.String(32))
    )
    table.create(database.engine)

    builder = sdc_builder.get_pipeline_builder()
    generator = builder.add_stage(label='Dev Data Generator')
    trash = builder.add_stage(label='Trash')

    jdbc = builder.add_test_origin_stage(label='JDBC Query Consumer')
    jdbc.incremental_mode = True
    jdbc.sql_query = 'SELECT * FROM {0} WHERE '.format(table_name) + 'id > ${OFFSET} ORDER BY id'
    jdbc.initial_offset = '0'
    jdbc.offset_column = 'id'

    generator >> trash
    pipeline = builder.build().configure_for_environment(database)

    sdc_executor.add_pipeline(pipeline)

    try:
        connection = database.engine.connect()
        connection.execute(table.insert(), [{'id': 1}])

        preview = sdc_executor.run_pipeline_preview(pipeline, test_origin=True).preview
        assert preview is not None
        assert preview.issues.issues_count == 0

        assert len(preview[jdbc].output) == 1
        assert preview[jdbc].output[0].field['id'].value == 1
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@sdc_min_version('4.2.0')
def test_stage_errors_counter(sdc_builder, sdc_executor):
    """This tests the accounting of stage errors happening outside of batch context in a pipeline level."""

    raw_data_1 = 'Hello!'
    raw_data_2 = 'Hello!Hello!'
    tmp_directory = os.path.join(tempfile.gettempdir(), get_random_string())
    sdc_executor.execute_shell(f'mkdir -p {tmp_directory}')

    builder = sdc_builder.get_pipeline_builder()
    directory = builder.add_stage('Directory', type='origin')
    directory.set_attributes(data_format='JSON', file_name_pattern='sdc*.txt', file_name_pattern_mode='GLOB',
                             file_post_processing='DELETE', files_directory=tmp_directory, read_order='TIMESTAMP',
                             number_of_threads=2)
    trash = builder.add_stage(label='Trash')

    directory >> trash
    pipeline = builder.build()
    try:
        sdc_executor.write_file(os.path.join(tmp_directory, 'sdc1.txt'), raw_data_1)
        sdc_executor.write_file(os.path.join(tmp_directory, 'sdc2.txt'), raw_data_2)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline)
        # There is no metric to stop the pipeline, as we only have stage errors
        sleep(30)
        sdc_executor.stop_pipeline(pipeline)

        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('stage.Directory_01.stageErrors.counter').count == 2
        # this counter will probably change after the errors have their own metric
        assert history.latest.metrics.counter('pipeline.batchErrorMessages.counter').count == 2
    finally:
        if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        sdc_executor.execute_shell(f'rm -fr {tmp_directory}')
