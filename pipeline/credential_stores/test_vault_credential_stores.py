####
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
import pytest
import string
import sqlalchemy

from hvac.exceptions import Forbidden
from streamsets.testframework.markers import vault, database
from streamsets.testframework.utils import get_random_string

pytestmark = [pytest.mark.sdc_min_version('6.1.0'), pytest.mark.database('mysql')]

logger = logging.getLogger(__name__)

@vault('kv2')
@database
@pytest.mark.parametrize( "secret_path", ["mysql", "mysecrets/mysql"])
@pytest.mark.parametrize( "mount_point", ["myengine", "myengine/secret", "nonexistent"])
def test_hashicorp_vault_kv2(sdc_builder, sdc_executor, credential_store, database, secret_path, mount_point):
    """
    Test to verify that the Hashicorp Vault logged in with Azure Auth works for kv version 2.
    """
    connect_mysql_using_hashicorp_vault(sdc_builder, sdc_executor, credential_store, database, secret_path, mount_point)


@vault('kv2','custom_mount')
@database
@pytest.mark.parametrize( "secret_path", ["mysql", "mysecrets/mysql"])
@pytest.mark.parametrize( "mount_point", ["myengine/secret", "myengine/secret/creds", "myengine/nonexistent"])
def test_azure_auth_hashicorp_vault_kv2_with_mount_point(sdc_builder, sdc_executor, credential_store, database, secret_path, mount_point):
    """
    Test to verify that the Hashicorp Vault logged in with Azure Auth works for kv version 2 with custom mount point.
    """
    connect_mysql_using_hashicorp_vault(sdc_builder, sdc_executor, credential_store, database, secret_path, mount_point)


def connect_mysql_using_hashicorp_vault(sdc_builder, sdc_executor, credential_store, database, secret_path, mount_point):
    """
    Tests to try connecting to MySQL Database by fetching credentials from Hashicorp Vault using given mount point
    and secret path.
    """

    # Set username and password in the Credential Store
    if mount_point in ("nonexistent", "myengine/nonexistent"):
        with pytest.raises(ValueError | Forbidden) as e:
            credential_store.set_secret(f'{mount_point}/{secret_path}', 'root', 'user')
        assert ('The secret path must start with the mount point defined' in str(e.value) or 'permission denied' in str(e.value))
        pass
    else:
        credential_store.set_secret(f'{mount_point}/{secret_path}', 'root', 'user')
        credential_store.set_secret(f'{mount_point}/{secret_path}', 'root', 'pass')

    num_records = 8
    input_data = [{'id': i, 'name': get_random_string()} for i in range(1, num_records + 1)]
    table_name = get_random_string(string.ascii_lowercase, 20)
    sql_query = f'SELECT * FROM {table_name} ORDER BY id ASC'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    origin = pipeline_builder.add_stage('JDBC Query Consumer')
    origin.set_attributes(incremental_mode=False,
                          sql_query=sql_query,
                          use_credentials=True,
                          username="".join(['${credential:get("vault", "all", "', f'{mount_point}/{secret_path}&user', '")}']),
                          password="".join(['${credential:get("vault", "all", "', f'{mount_point}/{secret_path}&pass', '")}']),
                          )

    wiretap = pipeline_builder.add_wiretap()
    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")

    origin >> wiretap.destination
    origin >= finisher
    pipeline = pipeline_builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        # Create and populate table
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table = sqlalchemy.Table(table_name,
                                 sqlalchemy.MetaData(),
                                 sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                                 sqlalchemy.Column('name', sqlalchemy.String(32)))
        table.create(database.engine)
        connection = database.engine.connect()
        connection.execute(table.insert(), input_data)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        sdc_records = [record.field for record in wiretap.output_records]
        assert sdc_records == input_data
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)
