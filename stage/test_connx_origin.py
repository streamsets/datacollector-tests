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

import os
import pytest
import string
import logging
from streamsets.testframework.markers import sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.connx, sdc_min_version('5.4.0')]

class Connx:
    cfg = {
            "gateway": "daecnxstream01.eur.ad.sag",
            "database_name": "VSAM",
            "port": 7500,
            "username": "username",
            "password": "password",
            "use_ssl": True
    }
    connection_string = f"jdbc:connx:DD={cfg['database_name']};Gateway={cfg['gateway']};Port={cfg['port']};ssl={'True' if cfg['use_ssl'] else 'False'}"
    driver_path = os.path.dirname(os.path.realpath(__file__)) + "/connxjdbc.jar"
    driver_class = "com.Connx.jdbc.TCJdbc.TCJdbcDriver" 

    def get_connection(self):
        import jaydebeapi  # Moving the import here temporary
        return jaydebeapi.connect(self.driver_class, self.connection_string, [self.cfg['username'], self.cfg['password']], self.driver_path)


@pytest.fixture
def connx():
    return Connx()


def test_connx_origin_event(sdc_builder, sdc_executor, connx):
    num_records = 10
    input_data = [{'id': i, 'name': get_random_string()} for i in range(1, num_records + 1)]
    table_name = get_random_string(string.ascii_lowercase, 20)
    sql_query = f'SELECT * FROM {table_name} ORDER BY id ASC'

    # Create pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    origin = pipeline_builder.add_stage('ConnX')
    origin.set_attributes(incremental_mode=False,
                          sql_query=sql_query,
                          max_batch_size_in_records=num_records,
                          use_credentials=True,
                          **connx.cfg)

    wiretap = pipeline_builder.add_wiretap()
    trash = pipeline_builder.add_stage('Trash')

    origin >> trash
    origin >= wiretap.destination

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    try:
        with connx.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"create table {table_name}(ID int, NAME varchar(100))")
                for row in input_data:
                    cursor.execute(f"insert into {table_name}(ID, NAME) values ({row['id']}, '{row['name']}')")

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(num_records)
        sdc_executor.stop_pipeline(pipeline)

        event_records = wiretap.output_records
        assert 2 == len(event_records)
        assert 'jdbc-query-success' == event_records[0].header.values['sdc.event.type']
        assert 'no-more-data' == event_records[1].header.values['sdc.event.type']
    finally:
        logger.info('Dropping table %s ...', table_name)
        with connx.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"drop table {table_name}")


@pytest.mark.parametrize("use_connection_string", [True, False])
def test_connx_single_read_use_connection_string(sdc_builder, sdc_executor, connx, use_connection_string):
    num_records = 10
    input_data = [{'id': i, 'name': get_random_string()} for i in range(1, num_records + 1)]
    table_name = get_random_string(string.ascii_lowercase, 20)
    sql_query = f'SELECT * FROM {table_name} ORDER BY id ASC'

    # Create pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    origin = pipeline_builder.add_stage('ConnX')
    origin.set_attributes(incremental_mode=False,
                          sql_query=sql_query,
                          max_batch_size_in_records=num_records,
                          use_credentials=True,
                          use_connection_string=use_connection_string)
    if use_connection_string:
        origin.set_attributes(connx_jdbc_connection_string=connx.connection_string,
                              username=connx.cfg["username"],
                              password=connx.cfg["password"],
                              use_ssl=connx.cfg["use_ssl"])
    else:
        origin.set_attributes(**connx.cfg)

    wiretap = pipeline_builder.add_wiretap()
    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")

    origin >> wiretap.destination
    origin >= finisher

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    try:
        with connx.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"create table {table_name}(ID int, NAME varchar(100))")
                for row in input_data:
                    cursor.execute(f"insert into {table_name}(ID, NAME) values ({row['id']}, '{row['name']}')")

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        assert [{"id": record.field['ID'], "name": record.field['NAME']} for record in wiretap.output_records] == input_data
    finally:
        logger.info('Dropping table %s ...', table_name)
        with connx.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"drop table {table_name}")


def test_connx_origin_full_mode(sdc_builder, sdc_executor, connx):
    num_records = 10
    input_data = [{'id': i, 'name': get_random_string()} for i in range(1, num_records + 1)]
    table_name = get_random_string(string.ascii_lowercase, 20)
    sql_query = f'SELECT * FROM {table_name} ORDER BY id ASC'

    # Create pipeline
    pipeline_builder = sdc_builder.get_pipeline_builder()
    origin = pipeline_builder.add_stage('ConnX')
    origin.set_attributes(incremental_mode=False,
                          sql_query=sql_query,
                          max_batch_size_in_records=num_records,
                          use_credentials=True,
                          **connx.cfg)

    wiretap = pipeline_builder.add_wiretap()
    finisher = pipeline_builder.add_stage("Pipeline Finisher Executor")

    origin >> wiretap.destination
    origin >= finisher

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    try:
        with connx.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"create table {table_name}(ID int, NAME varchar(100))")
                for row in input_data:
                    cursor.execute(f"insert into {table_name}(ID, NAME) values ({row['id']}, '{row['name']}')")


        # Run the pipeline and check the stage consumed all the expected records. Repeat several times to
        # ensure non-incremental mode works as expected after restarting the pipeline.
        for _ in range(2):
            wiretap.reset()
            sdc_executor.start_pipeline(pipeline).wait_for_finished()
            assert [{"id": record.field['ID'], "name": record.field['NAME']} for record in wiretap.output_records] == input_data
    finally:
        logger.info('Dropping table %s ...', table_name)
        with connx.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"drop table {table_name}")


def test_connx_origin_incremental_mode(sdc_builder, sdc_executor, connx):
    num_records = 10
    input_data = [{'id': i, 'name': get_random_string()} for i in range(1, num_records + 1)]
    table_name = get_random_string(string.ascii_lowercase, 20)
    sql_query = f'SELECT * FROM {table_name} WHERE id > ${{OFFSET}} ORDER BY id ASC LIMIT 5'

    pipeline_builder = sdc_builder.get_pipeline_builder()
    origin = pipeline_builder.add_stage('ConnX')
    origin.set_attributes(incremental_mode=True,
                          initial_offset="0",
                          offset_column="id",
                          sql_query=sql_query,
                          max_batch_size_in_records=2,
                          use_credentials=True,
                          **connx.cfg)

    wiretap = pipeline_builder.add_wiretap()
    wiretap_events = pipeline_builder.add_wiretap()

    origin >> wiretap.destination
    origin >= wiretap_events.destination

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    try:
        with connx.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"create table {table_name}(ID int, NAME varchar(100))")
                for row in input_data:
                    cursor.execute(f"insert into {table_name}(ID, NAME) values ({row['id']}, '{row['name']}')")

        # The batch size is 2 and the total of rows in the table is 10, so the stage needs to issue 5 queries with the
        # correct offset in order to get all the rows
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', num_records)
        sdc_executor.stop_pipeline(pipeline)

        events = wiretap_events.output_records
        assert 2 == len(events)
        assert f'SELECT * FROM {table_name} WHERE id > 0 ORDER BY id ASC LIMIT 5' == events[0].field['query']
        assert f'SELECT * FROM {table_name} WHERE id > 5 ORDER BY id ASC LIMIT 5' == events[1].field['query']
        assert [{"id": record.field['ID'], "name": record.field['NAME']} for record in wiretap.output_records] == input_data
    finally:
        logger.info('Dropping table %s ...', table_name)
        with connx.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"drop table {table_name}")