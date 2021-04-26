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

import logging
import string

from streamsets.testframework.markers import database
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@database('postgresql')
def test_non_matching_types(sdc_builder, sdc_executor, database, keep_data):
    """Ensure proper error when a pre-existing table contains type mapping that is not valid."""
    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()
    try:
        # We don't support "money" in the Metadata processor
        connection.execute(f"""
            CREATE TABLE {table_name}(
                id int primary key,
                code money
            )
        """)

        builder = sdc_builder.get_pipeline_builder()

        source = builder.add_stage('Dev Raw Data Source')
        source.stop_after_first_batch = True
        source.data_format = 'JSON'
        source.raw_data = '{"id":1, "code": 2}'

        processor = builder.add_stage('PostgreSQL Metadata')
        processor.table_name = table_name

        wiretap = builder.add_wiretap()

        source >> processor >> wiretap.destination

        # Create & run the pipeline
        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # The record should be sent to error with proper error code
        errors = wiretap.error_records
        assert len(errors) == 1
        assert errors[0].header['errorCode'] == 'JDBC_303'
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            connection.execute(f"DROP TABLE IF EXISTS {table_name}")
