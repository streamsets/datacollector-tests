# Copyright 2017 StreamSets Inc.
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The tests in this module follow a pattern of creating pipelines with
:py:obj:`testframework.sdc_models.PipelineBuilder` in one version of SDC and then importing and running them in
another.
"""

import json
import logging
import string

from testframework.markers import cassandra
from testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# pylint: disable=not-an-iterable, pointless-statement, redefined-outer-name, too-many-locals


@cassandra
def test_cassandra_destination(sdc_builder, sdc_executor, cassandra):
    """Test for Cassandra destination stage. Support to test Kerberos or plain text only. The pipeline looks like:

        dev_raw_data_source >> cassandra_destination
    """
    raw_dict = [dict(contact=dict(name='Jane Smith', phone=2124050000, zip_code=27023)),
                dict(contact=dict(name='San', phone=2120998998, zip_code=14305))]
    raw_data = json.dumps(raw_dict)
    cassandra_keyspace = get_random_string(string.ascii_letters, 10)
    cassandra_table = 'contact'

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', data_format_config='ARRAY_OBJECTS', raw_data=raw_data)
    cassandra_destination = builder.add_stage('Cassandra', type='destination')
    cassandra_destination.set_attributes(field_to_column_mapping=[
        {'field': '/contact/name', 'columnName': 'name'},
        {'field': '/contact/zip_code', 'columnName': 'zip_code'},
        {'field': '/contact/phone', 'columnName': 'phone'}],
                                         fully_qualified_table_name=f'{cassandra_keyspace}.{cassandra_table}',
                                         protocol_version='V4')
    if cassandra.kerberos_enabled:
        cassandra_destination.set_attributes(authentication_provider='KERBEROS')
    else:
        cassandra_destination.set_attributes(authentication_provider='PLAINTEXT', password=cassandra.password,
                                             username=cassandra.username)

    dev_raw_data_source >> cassandra_destination
    pipeline = builder.build(title='Cassandra Destination pipeline').configure_for_environment(cassandra)
    sdc_executor.add_pipeline(pipeline)

    try:
        client = cassandra.client
        cluster = client.cluster
        session = client.session

        # create Cassandra required tables before pipeline can put data
        session.execute(f"CREATE KEYSPACE {cassandra_keyspace} WITH replication = "
                        f"{{'class': 'SimpleStrategy', 'replication_factor': 1}}")
        session.execute(f'CREATE TABLE {cassandra_keyspace}.{cassandra_table} '
                        f'(name text PRIMARY KEY, zip_code int, phone int)')

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)
        sdc_executor.stop_pipeline(pipeline)

        # read data from Cassandra and assert to what pipeline has ingested
        rows = session.execute(f'SELECT * FROM {cassandra_keyspace}.{cassandra_table}').current_rows
        assert len(rows) == len(raw_dict)
        for index, row in enumerate(rows):
            for key, value in raw_dict[index]['contact'].items():
                assert getattr(row, key) == value
    finally:
        # drop table and keyspace from Cassandra
        session.execute(f'DROP TABLE {cassandra_keyspace}.{cassandra_table}')
        session.execute(f'DROP KEYSPACE {cassandra_keyspace}')
        cluster.shutdown()
