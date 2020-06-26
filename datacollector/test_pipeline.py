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

import sqlalchemy
import logging
import string

from streamsets.testframework.environments.databases import OracleDatabase
from streamsets.testframework.markers import database
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
