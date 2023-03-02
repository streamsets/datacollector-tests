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

import json
import logging
import string

from streamsets.testframework.markers import azure, ftp, sdc_min_version
from streamsets.testframework.utils import get_random_string
from stage.utils.utils_azure_synapse import STAGE_NAME, delete_table, stop_pipeline

pytestmark = sdc_min_version('5.5.0')

logger = logging.getLogger(__name__)

# The name SFTP/FTP/FTPS Client can not be used to create the stage
FTP_ORIGIN_CLIENT_NAME = 'com_streamsets_pipeline_stage_origin_remote_RemoteDownloadDSource'


@ftp
@azure('synapse')
def test_fpt_synapse_destination(sdc_builder, sdc_executor, ftp, azure):
    """
    Test for FTP consumer to Azure Synapse target stage.
    We do so by creating multiple files, reading them with FTP then
    Azure Synapse destination creates them in Azure Synapse.
    Finally, data is read from database for assertion.
    A pipeline finisher is added to stop the pipeline when there is no more data.

    The pipeline looks like:
    FTP >> azure_synapse_destination
    FTP >= pipeline_finisher
    """

    rows_in_file = [
        [{'id': i, 'name': f'Roger Federer{i}'} for i in range(1, 200)],
        [{'id': i, 'name': f'Martin Del Potro{i}'} for i in range(1, 200)],
        [{'id': i, 'name': f'David Nalbandian{i}'} for i in range(1, 200)]
    ]

    client = ftp.client
    client.cwd('/')

    src_table_prefix = get_random_string(string.ascii_lowercase, 6)
    table_names = [f'{src_table_prefix}_{get_random_string()}' for _ in range(3)]

    for i in range(3):
        raw_data = ''.join(json.dumps(record) for record in rows_in_file[i])
        ftp.put_string(table_names[i], raw_data)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    origin = pipeline_builder.add_stage(name=FTP_ORIGIN_CLIENT_NAME)
    origin.set_attributes(
        file_name_pattern=f'{src_table_prefix}*',
        data_format='JSON'
    )

    azure_synapse_destination = pipeline_builder.add_stage(name=STAGE_NAME)
    azure_synapse_destination.set_attributes(
        auto_create_table=True,
        table="${record:attribute('filename')}",
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

    origin >> azure_synapse_destination
    origin >= pipeline_finisher

    pipeline = pipeline_builder.build().configure_for_environment(ftp, azure)
    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        engine = azure.synapse.engine
        for i in range(3):
            stmt = f'select * from [{azure.synapse_database_schema}].[{table_names[i]}]'
            result = engine.execute(stmt)
            data_from_database = sorted(result.fetchall(), key=lambda row: row[0])
            result.close()
            assert data_from_database == [(row['id'], row['name']) for row in rows_in_file[i]]
    finally:
        logger.info('Dropping files in ftp server')
        try:
            stop_pipeline(sdc_executor, pipeline)
            client.cwd('/')
            for i in range(3):
                client.delete(f'/{table_names[i]}')
            client.quit()
        except Exception as ex:
            logger.error(ex)
        logger.info('Dropping files in synapse')
        try:
            for table_name in table_names:
                delete_table(azure.synapse.engine, table_name, azure.synapse_database_schema)
        except Exception as ex:
            logger.error(ex)
