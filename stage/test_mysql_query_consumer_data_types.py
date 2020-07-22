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

import pytest
import sqlalchemy
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


# https://dev.mysql.com/doc/refman/8.0/en/data-types.html
# We don't support BIT generally (the driver is doing funky 'random' mappings on certain versions
DATA_TYPES = [
    ('TINYINT', '-128', 'SHORT', -128),
    ('TINYINT UNSIGNED', '255', 'SHORT', 255),
    ('SMALLINT', '-32768', 'SHORT', -32768),
    ('SMALLINT UNSIGNED', '65535', 'SHORT', -1),  # Support for unsigned isn't entirely correct!
    ('MEDIUMINT', '-8388608', 'INTEGER', '-8388608'),
    ('MEDIUMINT UNSIGNED', '16777215', 'INTEGER', '16777215'),
    ('INT', '-2147483648', 'INTEGER', '-2147483648'),
    ('INT UNSIGNED', '4294967295', 'INTEGER', '-1'),  # Support for unsigned isn't entirely correct!
    ('BIGINT', '-9223372036854775807', 'LONG', '-9223372036854775807'),
    ('BIGINT UNSIGNED', '18446744073709551615', 'LONG', '-1'),  # Support for unsigned isn't entirely correct!
    ('DECIMAL(5, 2)', '5.20', 'DECIMAL', '5.20'),
    ('NUMERIC(5, 2)', '5.20', 'DECIMAL', '5.20'),
    ('FLOAT', '5.2', 'FLOAT', '5.2'),
    ('DOUBLE', '5.2', 'DOUBLE', '5.2'),
    # ('BIT(8)',"b'01010101'", 'BYTE_ARRAY', 'VQ=='),
    ('DATE', "'2019-01-01'", 'DATE', 1546300800000),
    ('DATETIME', "'2019-01-01 5:00:00'", 'DATETIME', 1546318800000),
    ('TIMESTAMP', "'2019-01-01 5:00:00'", 'DATETIME', 1546318800000),
    ('TIME', "'5:00:00'", 'TIME', 18000000),
    ('YEAR', "'2019'", 'DATE', 1546300800000),
    ('CHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('VARCHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('BINARY(5)', "'Hello'", 'BYTE_ARRAY', 'SGVsbG8='),
    ('VARBINARY(5)', "'Hello'", 'BYTE_ARRAY', 'SGVsbG8='),
    ('BLOB', "'Hello'", 'BYTE_ARRAY', 'SGVsbG8='),
    ('TEXT', "'Hello'", 'STRING', 'Hello'),
    ("ENUM('a', 'b')", "'a'", 'STRING', 'a'),
    ("set('a', 'b')", "'a,b'", 'STRING', 'a,b'),
    ("POINT", "POINT(1, 1)", 'BYTE_ARRAY', 'AAAAAAEBAAAAAAAAAAAA8D8AAAAAAADwPw=='),
    ("LINESTRING", "LineString(Point(0,0), Point(10,10), Point(20,25), Point(50,60))", 'BYTE_ARRAY',
     'AAAAAAECAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAkQAAAAAAAACRAAAAAAAAANEAAAAAAAAA5QAAAAAAAAElAAAAAAAAATkA='),
    ("JSON", "'{\"a\":\"b\"}'", 'STRING', '{\"a\": \"b\"}'),
]


@sdc_min_version('3.18.0')
@database('mysql')
@pytest.mark.parametrize('sql_type,insert_fragment,expected_type,expected_value', DATA_TYPES)
def test_data_types(sdc_builder, sdc_executor, database, sql_type,
                    insert_fragment, expected_type, expected_value):
    """Test all feasible types using query consumer"""

    pytest.skip("Branded JDBC stages are disabled in SDC")
    # Set up table
    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()
    set_up_table(connection, table_name, sql_type, insert_fragment)

    # Build stage & pipeline
    builder = sdc_builder.get_pipeline_builder()
    primary_key = 'id'
    origin = builder.add_stage('MySQL Query Consumer')
    origin.sql_query = f'SELECT * FROM {table_name} where {primary_key} > 0 order by {primary_key}'
    origin.offset_column = primary_key
    origin.incremental_mode = False
    origin.on_unknown_type = 'CONVERT_TO_STRING'

    trash = builder.add_stage('Trash')
    origin >> trash
    pipeline = builder.build().configure_for_environment(database)

    sdc_executor.add_pipeline(pipeline)

    try:
        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        assert len(snapshot[origin].output) == 2
        record = snapshot[origin].output[0]
        null_record = snapshot[origin].output[1]

        assert record.field['data_column'].type == expected_type
        assert null_record.field['data_column'].type == expected_type

        assert record.field['data_column']._data['value'] == expected_value
        assert null_record.field['data_column'] == None
    finally:
        logger.info('Cleaning up resources')
        _clean_up(database, table_name)


def set_up_table(connection, table_name, sql_type, insert_fragment):
    # Create table
    connection.execute(f'CREATE TABLE {table_name} (id int primary key, data_column {sql_type} NULL)')
    # Add rows with actual & null values
    connection.execute(f'INSERT INTO {table_name} VALUES(1, {insert_fragment})')
    connection.execute(f'INSERT INTO {table_name} VALUES(2, NULL)')


def _clean_up(database, table_name):
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(table_name, metadata)
    logger.info('Dropping table %s in %s database...', table_name, database.type)
    table.drop(database.engine)
