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
import time

import pytest
import sqlalchemy
from streamsets.sdk.utils import Version
from streamsets.testframework.environments.databases import MemSqlDatabase
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

@pytest.fixture(scope='module')
def sdc_builder_hook():
    def hook(data_collector):
        data_collector.SDC_JAVA_OPTS = '-Xmx2048m -Xms2048m'
    return hook

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# https://docs.oracle.com/cd/B28359_01/server.111/b28318/datatype.htm#CNCPT1821
# We don't support UriType (requires difficult workaround in JDBC)
DATA_TYPES_ORACLE = [
    ('number', '1', 'DECIMAL', '1'),
    ('char(2)', "'AB'", 'STRING', 'AB'),
    ('varchar(4)', "'ABCD'", 'STRING', 'ABCD'),
    ('varchar2(4)', "'NVAR'", 'STRING', 'NVAR'),
    ('nchar(3)', "'NCH'", 'STRING', 'NCH'),
    ('nvarchar2(4)', "'NVAR'", 'STRING', 'NVAR'),
    ('binary_float', '1.0', 'FLOAT', '1.0'),
    ('binary_double', '2.0', 'DOUBLE', '2.0'),
    ('date', "TO_DATE('1998-1-1 6:22:33', 'YYYY-MM-DD HH24:MI:SS')", 'DATETIME', 883635753000),
    ('timestamp', "TIMESTAMP'1998-1-2 6:00:00'", 'DATETIME', 883720800000),
    ('timestamp with time zone', "TIMESTAMP'1998-1-3 6:00:00-5:00'", 'ZONED_DATETIME', '1998-01-03T06:00:00-05:00'),
    ('timestamp with local time zone', "TIMESTAMP'1998-1-4 6:00:00-5:00'", 'ZONED_DATETIME', '1998-01-04T11:00:00Z'),
    ('long', "'LONG'", 'STRING', 'LONG'),
    ('blob', "utl_raw.cast_to_raw('BLOB')", 'BYTE_ARRAY', 'QkxPQg=='),
    ('clob', "'CLOB'", 'STRING', 'CLOB'),
    ('nclob', "'NCLOB'", 'STRING', 'NCLOB'),
    ('XMLType', "xmltype('<a></a>')", 'STRING', '<a/>')
]
@sdc_min_version('3.0.0.0')
@database('oracle')
@pytest.mark.parametrize('sql_type,insert_fragment,expected_type,expected_value', DATA_TYPES_ORACLE, ids=[i[0] for i in DATA_TYPES_ORACLE])
def test_data_types_oracle(sdc_builder, sdc_executor, database, sql_type, insert_fragment, expected_type, expected_value, keep_data):
    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()

    try:
        # Create table
        connection.execute(f"""
            CREATE TABLE {table_name}(
                id number primary key,
                data_column {sql_type} NULL
            )
        """)

        # And insert a row with actual value
        connection.execute(f"INSERT INTO {table_name} VALUES(1, {insert_fragment})")
        # And a null
        connection.execute(f"INSERT INTO {table_name} VALUES(2, NULL)")

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('JDBC Multitable Consumer')
        origin.table_configs = [{"tablePattern": f'%{table_name}%'}]
        origin.on_unknown_type = 'CONVERT_TO_STRING'

        trash = builder.add_stage('Trash')

        origin >> trash

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        assert len(snapshot[origin].output) == 2
        record = snapshot[origin].output[0]
        null_record = snapshot[origin].output[1]

        # TLKT-177: Add ability for field to return raw value
        # Since we are controlling types, we want to check explicit values inside the record rather the the python
        # wrappers.

        assert record.field['DATA_COLUMN'].type == expected_type
        assert null_record.field['DATA_COLUMN'].type == expected_type

        assert null_record.field['DATA_COLUMN'] == None
        if sql_type == 'XMLType':
            assert record.field['DATA_COLUMN']._data['value'].strip() == expected_value
        else:
            assert record.field['DATA_COLUMN']._data['value'] == expected_value
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            connection.execute(f"DROP TABLE {table_name}")


# https://dev.mysql.com/doc/refman/8.0/en/data-types.html
# We don't support BIT generally (the driver is doing funky 'random' mappings on certain versions)
DATA_TYPES_MYSQL = [
    ('TINYINT', '-128', 'SHORT', -128),
    ('TINYINT UNSIGNED', '255', 'SHORT', 255),
    ('SMALLINT', '-32768', 'SHORT', -32768),
    ('SMALLINT UNSIGNED', '65535', 'INTEGER', '65535'),
    ('MEDIUMINT', '-8388608', 'INTEGER', '-8388608'),
    ('MEDIUMINT UNSIGNED', '16777215', 'LONG', '16777215'),
    ('INT', '-2147483648', 'INTEGER', '-2147483648'),
    ('INT UNSIGNED', '4294967295', 'LONG', '4294967295'),
    ('BIGINT', '-9223372036854775807', 'LONG', '-9223372036854775807'),
    ('BIGINT UNSIGNED', '18446744073709551615', 'DECIMAL', '18446744073709551615'),
    ('DECIMAL(5, 2)', '5.20', 'DECIMAL', '5.20'),
    ('NUMERIC(5, 2)', '5.20', 'DECIMAL', '5.20'),
    ('FLOAT', '5.2', 'FLOAT', '5.2'),
    ('DOUBLE', '5.2', 'DOUBLE', '5.2'),
    #    ('BIT(8)',"b'01010101'", 'BYTE_ARRAY', 'VQ=='),
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
    ("POLYGON",
     "Polygon(LineString(Point(0,0),Point(10,0),Point(10,10),Point(0,10),Point(0,0)),LineString(Point(5,5),Point(7,5),Point(7,7),Point(5,7),Point(5,5)))",
     'BYTE_ARRAY',
     'AAAAAAEDAAAAAgAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAJEAAAAAAAAAAAAAAAAAAACRAAAAAAAAAJEAAAAAAAAAAAAAAAAAAACRAAAAAAAAAAAAAAAAAAAAAAAUAAAAAAAAAAAAUQAAAAAAAABRAAAAAAAAAHEAAAAAAAAAUQAAAAAAAABxAAAAAAAAAHEAAAAAAAAAUQAAAAAAAABxAAAAAAAAAFEAAAAAAAAAUQA=='),
    ("JSON", "'{\"a\":\"b\"}'", 'STRING', '{\"a\": \"b\"}'),
]
@sdc_min_version('3.0.0.0')
@database('mysql')
@pytest.mark.parametrize('sql_type,insert_fragment,expected_type,expected_value', DATA_TYPES_MYSQL, ids=[i[0] for i in DATA_TYPES_MYSQL])
def test_data_types_mysql(sdc_builder, sdc_executor, database, sql_type, insert_fragment, expected_type, expected_value, keep_data):
    """Test all feasible Mysql types."""
    if isinstance(database, MemSqlDatabase):
        pytest.skip("Standard Tests are currently only written for MySQL and not for MemSQL (sadly STF threads both DBs the same way)")
    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()
    try:
        # Create table
        connection.execute(f"""
            CREATE TABLE {table_name}(
                id int primary key,
                data_column {sql_type} NULL
            )
        """)

        # And insert a row with actual value
        connection.execute(f"INSERT INTO {table_name} VALUES(1, {insert_fragment})")
        # And a null
        connection.execute(f"INSERT INTO {table_name} VALUES(2, NULL)")

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('JDBC Multitable Consumer')
        origin.table_configs = [{"tablePattern": f'%{table_name}%'}]
        origin.on_unknown_type = 'CONVERT_TO_STRING'

        trash = builder.add_stage('Trash')

        origin >> trash

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        assert len(snapshot[origin].output) == 2
        record = snapshot[origin].output[0]
        null_record = snapshot[origin].output[1]

        # Since we are controlling types, we want to check explicit values inside the record rather the the python
        # wrappers.
        # TLKT-177: Add ability for field to return raw value

        assert record.field['data_column'].type == expected_type
        assert null_record.field['data_column'].type == expected_type

        assert record.field['data_column']._data['value'] == expected_value
        assert null_record.field['data_column'] == None
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            connection.execute(f"DROP TABLE {table_name}")


# https://www.postgresql.org/docs/11/datatype.html
# Not testing 'serial' family explicitly as that is just an alias
# Not supporting tsvector tsquery as that doesn't seem fit for us
# bit(n) is not supported
# xml is not supported
# domain types (as a category are not supported)
# pg_lsn not supported
DATA_TYPES_POSTGRESQL = [
    ('smallint', '-32768', 'SHORT', -32768),
    ('integer', '2147483647', 'INTEGER', '2147483647'),
    ('bigint', '-9223372036854775808', 'LONG', '-9223372036854775808'),
    ('decimal(5,2)', '5.20', 'DECIMAL', '5.20'),
    ('numeric(5,2)', '5.20', 'DECIMAL', '5.20'),
    ('real', '5.20', 'FLOAT', '5.2'),
    ('double precision', '5.20', 'DOUBLE', '5.2'),
    ('money', '12.34', 'DOUBLE', '12.34'),
    ('char(5)', "'Hello'", 'STRING', 'Hello'),
    ('varchar(5)', "'Hello'", 'STRING', 'Hello'),
    ('text', "'Hello'", 'STRING', 'Hello'),
    ('bytea', "'\\xDEADBEEF'", 'BYTE_ARRAY', '3q2+7w=='),
    ('timestamp', "'2003-04-12 04:05:06'", 'DATETIME', 1050120306000),
    ('timestamp with time zone', "'2003-04-12 04:05:06 America/New_York'", 'DATETIME', 1050134706000),
    # For PostgreSQL, we don't create ZONED_DATETIME
    ('date', "'2019-01-01'", 'DATE', 1546300800000),
    ('time', "'5:00:00'", 'TIME', 18000000),
    ('time with time zone', "'04:05:06-08:00'", 'TIME', 43506000),
    ('interval', "INTERVAL '1' YEAR", 'STRING', '1 years 0 mons 0 days 0 hours 0 mins 0.0 secs'),
    ('boolean', "true", 'BOOLEAN', True),
    ('ai', "'sad'", 'STRING', 'sad'),
    ('point', "'(1, 1)'", 'STRING', '(1.0,1.0)'),
    ('line', "'{1, 1, 1}'", 'STRING', '{1.0,1.0,1.0}'),
    ('lseg', "'((1,1)(2,2))'", 'STRING', '[(1.0,1.0),(2.0,2.0)]'),
    ('box', "'(1,1)(2,2)'", 'STRING', '(2.0,2.0),(1.0,1.0)'),
    ('path', "'((1,1),(2,2))'", 'STRING', '((1.0,1.0),(2.0,2.0))'),
    ('polygon', "'((1,1),(2,2))'", 'STRING', '((1.0,1.0),(2.0,2.0))'),
    ('circle', "'<(1,1),5>'", 'STRING', '<(1.0,1.0),5.0>'),
    ('inet', "'127.0.0.1/16'", 'STRING', '127.0.0.1/16'),
    ('cidr', "'127.0.0.0/16'", 'STRING', '127.0.0.0/16'),
    ('macaddr', "'08:00:2b:01:02:03'", 'STRING', '08:00:2b:01:02:03'),
#    ('macaddr8', "'08:00:2b:01:02:03'", 'STRING', '08:00:2b:ff:fe:01:02:03'), # Not supported
#    ('bit(8)', "b'10101010'", 'BYTE_ARRAY', '08:00:2b:ff:fe:01:02:03'), # Doesn't work at all today
    ('bit varying(3)', "b'101'", 'STRING', '101'),
    ('uuid', "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'", 'STRING', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),
#    ('xml', "'<foo>bar</foo>'", 'STRING', ''), # Doesn't work properly today
    ("json", "'{\"a\":\"b\"}'", 'STRING', '{"a":"b"}'),
    ("jsonb", "'{\"a\":\"b\"}'", 'STRING', '{"a": "b"}'),
    ("integer[3][3]", "'{{1,2,3},{4,5,6},{7,8,9}}'", 'STRING', '{{1,2,3},{4,5,6},{7,8,9}}'),
    ("ct", "ROW(1, 2)", 'STRING', '(1,2)'),
    ("int4range", "'[1,2)'", 'STRING', '[1,2)'),
    ("int8range", "'[1,2)'", 'STRING', '[1,2)'),
    ("numrange", "'[1,2)'", 'STRING', '[1,2)'),
    ("tsrange", "'[2010-01-01 14:30, 2010-01-01 15:30)'", 'STRING', '["2010-01-01 14:30:00","2010-01-01 15:30:00")'),
    ("tstzrange", "'[2010-01-01 14:30 America/New_York, 2010-01-01 15:30 America/New_York)'", 'STRING',
     '["2010-01-01 19:30:00+00","2010-01-01 20:30:00+00")'),
    ("daterange", "'[2010-01-01, 2010-01-02)'", 'STRING', '[2010-01-01,2010-01-02)'),
]
@sdc_min_version('3.0.0.0')
@database('postgresql')
@pytest.mark.parametrize('sql_type,insert_fragment,expected_type,expected_value', DATA_TYPES_POSTGRESQL, ids=[i[0] for i in DATA_TYPES_POSTGRESQL])
def test_data_types_postgresql(sdc_builder, sdc_executor, database, sql_type, insert_fragment, expected_type, expected_value, keep_data):
    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()
    try:
        # Create enum type conditionally
        connection.execute(f"""
            DO
            $$
            BEGIN
              IF NOT EXISTS (SELECT * FROM pg_type typ
                                      INNER JOIN pg_namespace nsp ON nsp.oid = typ.typnamespace
                                      WHERE nsp.nspname = current_schema() AND typ.typname = 'ai') THEN
                CREATE TYPE ai AS ENUM ('sad', 'ok', 'happy');
              END IF;
            END;
            $$
            LANGUAGE plpgsql;
        """)

        # Create enum complex type conditionally
        connection.execute(f"""
            DO
            $$
            BEGIN
              IF NOT EXISTS (SELECT * FROM pg_type typ
                                      INNER JOIN pg_namespace nsp ON nsp.oid = typ.typnamespace
                                      WHERE nsp.nspname = current_schema() AND typ.typname = 'ct') THEN
                CREATE TYPE ct AS (a int, b int);
              END IF;
            END;
            $$
            LANGUAGE plpgsql;
        """)

        # Create table
        connection.execute(f"""
            CREATE TABLE {table_name}(
                id int primary key,
                data_column {sql_type} NULL
            )
        """)

        # And insert a row with actual value
        connection.execute(f"INSERT INTO {table_name} VALUES(1, {insert_fragment})")
        # And a null
        connection.execute(f"INSERT INTO {table_name} VALUES(2, NULL)")

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('JDBC Multitable Consumer')
        origin.table_configs = [{"tablePattern": f'%{table_name}%'}]
        origin.on_unknown_type = 'CONVERT_TO_STRING'

        trash = builder.add_stage('Trash')

        origin >> trash

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        assert len(snapshot[origin].output) == 2
        record = snapshot[origin].output[0]
        null_record = snapshot[origin].output[1]

        # Since we are controlling types, we want to check explicit values inside the record rather the the python
        # wrappers.
        # TLKT-177: Add ability for field to return raw value

        assert record.field['data_column'].type == expected_type
        assert null_record.field['data_column'].type == expected_type

        assert record.field['data_column']._data['value'] == expected_value
        assert null_record.field['data_column'] == None
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            connection.execute(f"DROP TABLE {table_name}")


# https://docs.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql?view=sql-server-2017
# hiearchyid types not supported
# Geometry and geography not supported
DATA_TYPES_SQLSERVER = [
    ('DATE', "'2019-01-01'", 'DATE', 1546300800000),
    ('DATETIME', "'2004-05-23T14:25:10'", 'DATETIME', 1085322310000),
    ('DATETIME2', "'2004-05-23T14:25:10'", 'DATETIME', 1085322310000),
    ('DATETIMEOFFSET', "'2004-05-23 14:25:10.3456 -08:00'", 'DEPENDS_ON_VERSION', 'depends_on_version'),
    ('SMALLDATETIME', "'2004-05-23T14:25:10'", 'DATETIME', 1085322300000),
    ('TIME', "'14:25:10'", 'TIME', 51910000),
    ('BIT', "1", 'BOOLEAN', True),
    ('DECIMAL(5,2)', '5.20', 'DECIMAL', '5.20'),
    ('NUMERIC(5,2)', '5.20', 'DECIMAL', '5.20'),
    ('REAL', '5.20', 'FLOAT', '5.2'),
    ('FLOAT', '5.20', 'DOUBLE', '5.2'),
    ('TINYINT', '255', 'SHORT', 255),
    ('SMALLINT', '-32768', 'SHORT', -32768),
    ('INT', '-2147483648', 'INTEGER', '-2147483648'),
    ('BIGINT', '-9223372036854775807', 'LONG', '-9223372036854775807'),
    ('MONEY', '255.60', 'DECIMAL', '255.6000'),
    ('SMALLMONEY', '255.60', 'DECIMAL', '255.6000'),
    ('BINARY(5)', "CAST('Hello' AS BINARY(5))", 'BYTE_ARRAY', 'SGVsbG8='),
    ('VARBINARY(5)', "CAST('Hello' AS VARBINARY(5))", 'BYTE_ARRAY', 'SGVsbG8='),
    ('CHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('VARCHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('NCHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('NVARCHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('TEXT', "'Hello'", 'STRING', 'Hello'),
    ('NTEXT', "'Hello'", 'STRING', 'Hello'),
    ('IMAGE', "CAST('Hello' AS IMAGE)", 'BYTE_ARRAY', 'SGVsbG8='),
#    ('GEOGRAPHY',"geography::STGeomFromText('LINESTRING(-122.360 47.656, -122.343 47.656 )', 4326)", 'BYTE_ARRAY', '5hAAAAEUhxbZzvfTR0DXo3A9CpdewIcW2c7300dAy6FFtvOVXsA='), # Not supported
#    ('GEOMETRY',"geometry::STGeomFromText('LINESTRING (100 100, 20 180, 180 180)', 0)", 'BYTE_ARRAY', 'AAAAAAEEAwAAAAAAAAAAAFlAAAAAAAAAWUAAAAAAAAA0QAAAAAAAgGZAAAAAAACAZkAAAAAAAIBmQAEAAAABAAAAAAEAAAD/////AAAAAAI='), # Not supported
    ('XML', "'<a></a>'", 'STRING', '<a/>')
]
@sdc_min_version('3.0.0.0')
@database('sqlserver')
@pytest.mark.parametrize('sql_type,insert_fragment,expected_type,expected_value', DATA_TYPES_SQLSERVER, ids=[i[0] for i in DATA_TYPES_SQLSERVER])
def test_data_types_sqlserver(sdc_builder, sdc_executor, database, sql_type, insert_fragment, expected_type, expected_value, keep_data):
    """Test all feasible SQL Server types."""
    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()
    try:
        # Create table
        connection.execute(f"""
            CREATE TABLE {table_name}(
                id int primary key,
                data_column {sql_type} NULL
            )
        """)

        # And insert a row with actual value
        connection.execute(f"INSERT INTO {table_name} VALUES(1, {insert_fragment})")
        # And a null
        connection.execute(f"INSERT INTO {table_name} VALUES(2, NULL)")

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('JDBC Multitable Consumer')
        origin.table_configs = [{"tablePattern": f'%{table_name}%'}]

        trash = builder.add_stage('Trash')

        # As a part of SDC-10125, DATETIMEOFFSET is natively supported in SDC, and is converted into ZONED_DATETIME
        if sql_type == 'DATETIMEOFFSET':
            if Version(sdc_executor.version) >= Version('3.14.0'):
                expected_type = 'ZONED_DATETIME'
                expected_value = '2004-05-23T14:25:10.3456-08:00'
            else:
                expected_type = 'STRING'
                expected_value = '2004-05-23 14:25:10.3456 -08:00'
                # This unknown_type_action setting is required, otherwise DATETIMEOFFSET tests for SDC < 3.14 will fail.
                origin.on_unknown_type = 'CONVERT_TO_STRING'

        origin >> trash

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        assert len(snapshot[origin].output) == 2
        record = snapshot[origin].output[0]
        null_record = snapshot[origin].output[1]

        # Since we are controlling types, we want to check explicit values inside the record rather the the python
        # wrappers.
        # TLKT-177: Add ability for field to return raw value

        assert record.field['data_column'].type == expected_type
        assert null_record.field['data_column'].type == expected_type

        assert record.field['data_column']._data['value'] == expected_value
        assert null_record.field['data_column'] == None
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            connection.execute(f"DROP TABLE {table_name}")


# Rules: https://docs.oracle.com/cd/B19306_01/server.102/b14200/sql_elements008.htm
# Max Lenght: https://stackoverflow.com/questions/756558/what-is-the-maximum-length-of-a-table-name-in-oracle
OBJECT_NAMES_ORACLE = [
    ('keywords', 'table', 'column'),
    ('lowercase', get_random_string(string.ascii_lowercase, 20), get_random_string(string.ascii_lowercase, 20)),
    ('uppercase', get_random_string(string.ascii_uppercase, 20), get_random_string(string.ascii_uppercase, 20)),
    ('mixedcase', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 20)),
    ('max_table_name', get_random_string(string.ascii_letters, 30), get_random_string(string.ascii_letters, 20)),
    ('max_column_name', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 30)),
    ('numbers', get_random_string(string.ascii_letters, 5) + "0123456789", get_random_string(string.ascii_letters, 5) + "0123456789"),
    ('special', get_random_string(string.ascii_letters, 5) + "!@#$%^&*()_+=-?<>", get_random_string(string.ascii_letters, 5) + "!@#$%^&*()_+=-?<>"),
]
@database('oracle')
@pytest.mark.parametrize('test_name,table_name,offset_name', OBJECT_NAMES_ORACLE, ids=[i[0] for i in OBJECT_NAMES_ORACLE])
def test_object_names_oracle(sdc_builder, sdc_executor, database, test_name, table_name, offset_name):
    _test_object_names(sdc_builder, sdc_executor, database, table_name, offset_name)


# Rules: https://www.postgresql.org/docs/9.1/sql-syntax-lexical.html
OBJECT_NAMES_POSTGRESQL = [
    ('keywords', 'table', 'column'),
    ('lowercase', get_random_string(string.ascii_lowercase, 20), get_random_string(string.ascii_lowercase, 20)),
    ('uppercase', get_random_string(string.ascii_uppercase, 20), get_random_string(string.ascii_uppercase, 20)),
    ('mixedcase', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 20)),
    ('max_table_name', get_random_string(string.ascii_letters, 63), get_random_string(string.ascii_letters, 20)),
    ('max_column_name', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 63)),
    ('numbers', get_random_string(string.ascii_letters, 5) + "0123456789", get_random_string(string.ascii_letters, 5) + "0123456789"),
    ('special', get_random_string(string.ascii_letters, 5) + "$_", get_random_string(string.ascii_letters, 5) + "$_"),
]
@database('postgresql')
@pytest.mark.parametrize('test_name,table_name,offset_name', OBJECT_NAMES_POSTGRESQL, ids=[i[0] for i in OBJECT_NAMES_POSTGRESQL])
def test_object_names_postgresql(sdc_builder, sdc_executor, database, test_name, table_name, offset_name):
    _test_object_names(sdc_builder, sdc_executor, database, table_name, offset_name)


# Rules: https://dev.mysql.com/doc/refman/8.0/en/identifier-length.html
# Rules: https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
OBJECT_NAMES_MYSQL = [
    ('keywords', 'table', 'column'),
    ('lowercase', get_random_string(string.ascii_lowercase, 20), get_random_string(string.ascii_lowercase, 20)),
    ('uppercase', get_random_string(string.ascii_uppercase, 20), get_random_string(string.ascii_uppercase, 20)),
    ('mixedcase', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 20)),
    ('max_table_name', get_random_string(string.ascii_letters, 64), get_random_string(string.ascii_letters, 20)),
    ('max_column_name', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 64)),
    ('numbers', get_random_string(string.ascii_letters, 5) + "0123456789", get_random_string(string.ascii_letters, 5) + "0123456789"),
    ('special', get_random_string(string.ascii_letters, 5) + "$_", get_random_string(string.ascii_letters, 5) + "$_"),
]
@database('mysql')
@pytest.mark.parametrize('test_name,table_name,offset_name', OBJECT_NAMES_MYSQL, ids=[i[0] for i in OBJECT_NAMES_MYSQL])
def test_object_names_mysql(sdc_builder, sdc_executor, database, test_name, table_name, offset_name):
    if isinstance(database, MemSqlDatabase):
        pytest.skip("Standard Tests are currently only written for MySQL and not for MemSQL (sadly STF threads both DBs the same way)")
    _test_object_names(sdc_builder, sdc_executor, database, table_name, offset_name)

# Rules: https://stackoverflow.com/questions/5808332/sql-server-maximum-character-length-of-object-names
# Rules:
OBJECT_NAMES_SQLSERVER = [
    ('keywords', 'table', 'column'),
    ('lowercase', get_random_string(string.ascii_lowercase, 20), get_random_string(string.ascii_lowercase, 20)),
    ('uppercase', get_random_string(string.ascii_uppercase, 20), get_random_string(string.ascii_uppercase, 20)),
    ('mixedcase', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 20)),
    ('max_table_name', get_random_string(string.ascii_letters, 128), get_random_string(string.ascii_letters, 20)),
    ('max_column_name', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 128)),
    ('numbers', get_random_string(string.ascii_letters, 5) + "0123456789", get_random_string(string.ascii_letters, 5) + "0123456789"),
    ('special', get_random_string(string.ascii_letters, 5) + "!@#$%^&*()_+=-?<>", get_random_string(string.ascii_letters, 5) + "!@#$%^&*()_+=-?<>"),
]
@database('sqlserver')
@pytest.mark.parametrize('test_name,table_name,offset_name', OBJECT_NAMES_SQLSERVER, ids=[i[0] for i in OBJECT_NAMES_SQLSERVER])
def test_object_names_sqlserver(sdc_builder, sdc_executor, database, test_name, table_name, offset_name):
    _test_object_names(sdc_builder, sdc_executor, database, table_name, offset_name)


def _test_object_names(sdc_builder, sdc_executor, database, table_name, offset_name):
    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('JDBC Multitable Consumer')
    origin.table_configs=[{"tablePattern": f'%{table_name}%'}]
    origin.max_batch_size_in_records = 10

    trash = builder.add_stage('Trash')

    origin >> trash

    pipeline = builder.build().configure_for_environment(database)
    # Work-arounding STF behavior of upper-casing table name configuration
    origin.table_configs[0]["tablePattern"] = f'%{table_name}%'

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column(offset_name, sqlalchemy.Integer, primary_key=True, quote=True),
        quote=True
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        logger.info('Adding three rows into %s database ...', database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), [{offset_name: 1}])

        sdc_executor.add_pipeline(pipeline)
        snapshot = sdc_executor.capture_snapshot(pipeline=pipeline, start_pipeline=True).snapshot
        # We want to run for a few seconds to see if any errors show up (like that did in previous versions)
        time.sleep(10)
        sdc_executor.stop_pipeline(pipeline)

        # There should be no errors reported
        history = sdc_executor.get_pipeline_history(pipeline)
        assert history.latest.metrics.counter('stage.JDBCMultitableConsumer_01.errorRecords.counter').count == 0
        assert history.latest.metrics.counter('stage.JDBCMultitableConsumer_01.stageErrors.counter').count == 0

        # And verify that we properly read that one record
        assert len(snapshot[origin].output) == 1
        # SDC Will escape field names with certain characters, but not always...
        if "$" in offset_name:
            assert snapshot[origin].output[0].field[f'"{offset_name}"'] == 1
        else:
            assert snapshot[origin].output[0].field[offset_name] == 1
    finally:
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@database
@pytest.mark.parametrize('number_of_threads', [1, 10])
@pytest.mark.parametrize('processing_mode', ['DISABLED', 'BEST_EFFORT', 'REQUIRED'])
def test_multiple_batches(sdc_builder, sdc_executor, database, number_of_threads, processing_mode, keep_data):
    if isinstance(database, MemSqlDatabase):
        pytest.skip("Standard Tests are currently only written for MySQL and not for MemSQL (sadly STF threads both DBs the same way)")
    max_batch_size = 1000
    batches = 50
    table_name = get_random_string(string.ascii_lowercase, 20)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True),
        quote=True
    )

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('JDBC Multitable Consumer')
    origin.table_configs=[{
        "tablePattern": f'%{table_name}%',
        'partitioningMode': processing_mode,
        'partitionSize': str(2 * max_batch_size)
    }]
    origin.max_batch_size_in_records = max_batch_size
    origin.number_of_threads = number_of_threads
    origin.maximum_pool_size = number_of_threads

    wiretap = builder.add_wiretap()
    origin >> wiretap.destination

    finisher = builder.add_stage("Pipeline Finisher Executor")
    finisher.stage_record_preconditions = ["${record:eventType() == 'no-more-data'}"]
    origin >= finisher

    pipeline = builder.build().configure_for_environment(database)
    # Work-arounding STF behavior of upper-casing table name configuration
    origin.table_configs[0]["tablePattern"] = f'%{table_name}%'
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating table %s', table_name)
        table.create(database.engine)

        logger.info('Inserting data into %s', table_name)
        connection = database.engine.connect()
        connection.execute(table.insert(), [{'id' : n} for n in range(1, max_batch_size * batches + 1)])

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        records = wiretap.output_records
        assert len(records) == max_batch_size * batches

        # Verify each record
        def sortFunc(entry):
            return entry.field['id'].value

        records.sort(key=sortFunc)

        expected_number = 1
        for record in records:
            assert record.field['id'] == expected_number
            expected_number = expected_number + 1
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)


@database
def test_dataflow_events(sdc_builder, sdc_executor, database, keep_data):
    if isinstance(database, MemSqlDatabase):
        pytest.skip("Standard Tests are currently only written for MySQL and not for MemSQL (sadly STF threads both DBs the same way)")
    table_prefix = get_random_string(string.ascii_lowercase, 20)
    table_a = '{}_a'.format(table_prefix)
    table_b = '{}_b'.format(table_prefix)

    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('JDBC Multitable Consumer')
    source.transaction_isolation = 'TRANSACTION_READ_COMMITTED'
    source.table_configs = [{
        'tablePattern': f'{table_prefix}%',
        "enableNonIncremental": True,
    }]

    trash = builder.add_stage('Trash')

    source >> trash

    wiretap = builder.add_wiretap()
    source >= wiretap.destination

    pipeline = builder.build().configure_for_environment(database)
    # Work-arounding STF behavior of upper-casing table name configuration
    source.table_configs[0]["tablePattern"] = f'{table_prefix}%'
    sdc_executor.add_pipeline(pipeline)

    #  We need three tables for this test
    metadata = sqlalchemy.MetaData()
    a = sqlalchemy.Table(
        table_a,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True),
        quote=True
    )
    b = sqlalchemy.Table(
        table_b,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=False, quote=True),
        quote=True
    )

    try:
        logger.info('Creating tables %s and %s in %s database ...', table_a, table_b, database.type)
        a.create(database.engine)
        b.create(database.engine)

        logger.info('Inserting rows into %s and %s', table_a, table_b)
        connection = database.engine.connect()
        connection.execute(a.insert(), {'id': 1})
        connection.execute(b.insert(), {'id': 1})

        # Start the pipeline
        status = sdc_executor.start_pipeline(pipeline)

        # Read two records, generate 4 events, 6 records
        status.wait_for_pipeline_output_records_count(6)

        # Force lexicographically reverse order (table-finished, schema-finished, no-more-data)
        records = sorted(wiretap.output_records, key=lambda row: row.header.values['sdc.event.type'], reverse=True)
        assert len(records) == 4

        # First two events should be table-finished (for any order of the tables though)
        assert records[0].header.values['sdc.event.type'] == 'table-finished'
        assert records[1].header.values['sdc.event.type'] == 'table-finished'
        table_set = set()
        table_set.add(records[0].field['table'])
        table_set.add(records[1].field['table'])
        assert table_a in table_set
        assert table_b in table_set

        # Then we should have schema done with all the tables
        assert records[2].header.values['sdc.event.type'] == 'schema-finished'
        assert table_a in records[2].field['tables']
        assert table_b in records[2].field['tables']

        # Final event should be no more data
        assert records[3].header.values['sdc.event.type'] == 'no-more-data'

        wiretap.reset()

        # Second iteration - insert one new row
        logger.info('Inserting rows into %s', table_a)
        connection = database.engine.connect()
        connection.execute(a.insert(), {'id': 2})

        # 1 record, 3 events more
        status.wait_for_pipeline_output_records_count(10)

        # Force lexicographically reverse order (table-finished, schema-finished, no-more-data)
        records = sorted(wiretap.output_records, key=lambda row: row.header.values['sdc.event.type'], reverse=True)
        assert len(records) == 3

        assert records[0].header.values['sdc.event.type'] == 'table-finished'
        assert records[0].field['table'] == table_a

        assert records[1].header.values['sdc.event.type'] == 'schema-finished'
        assert table_a in records[1].field['tables']
        assert table_b in records[1].field['tables']

        assert records[2].header.values['sdc.event.type'] == 'no-more-data'

        # Now let's stop the pipeline and start it again
        # SDC-10022: Multitable JDBC Origin with non-incremental table does not properly trigger 'no-more-data' event
        sdc_executor.stop_pipeline(pipeline)

        # Portable truncate
        wiretap.reset()

        # Start the pipeline and wait for it to read three records (3 events)
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(3)

        # Force lexicographically reverse order (table-finished, schema-finished, no-more-data)
        records = sorted(wiretap.output_records, key=lambda row: row.header.values['sdc.event.type'], reverse=True)
        assert len(records) == 3

        assert records[0].header.values['sdc.event.type'] == 'table-finished'
        assert records[0].field['table'] == table_a

        assert records[1].header.values['sdc.event.type'] == 'schema-finished'
        assert table_a in records[1].field['tables']
        assert table_b in records[1].field['tables']

        assert records[2].header.values['sdc.event.type'] == 'no-more-data'
    finally:
        sdc_executor.stop_pipeline(pipeline)
        if not keep_data:
            logger.info('Dropping tables %s and %s in %s database...', table_a, table_b, database.type)
            a.drop(database.engine)
            b.drop(database.engine)


@database
def test_resume_offset(sdc_builder, sdc_executor, database, keep_data):
    if isinstance(database, MemSqlDatabase):
        pytest.skip("Standard Tests are currently only written for MySQL and not for MemSQL (sadly STF threads both DBs the same way)")
    iterations = 3
    records_per_iteration = 10
    table_name = get_random_string(string.ascii_lowercase, 20)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True),
        quote=True
    )

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('JDBC Multitable Consumer')
    origin.transaction_isolation = 'TRANSACTION_READ_COMMITTED'
    origin.table_configs = [{'tablePattern': f'{table_name}%'}]

    wiretap = builder.add_wiretap()

    origin >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)
    origin.table_configs[0]["tablePattern"] = f'%{table_name}%'
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating table %s', table_name)
        table.create(database.engine)

        for iteration in range(0, iterations):
            logger.info(f"Iteration: {iteration}")
            wiretap.reset()

            logger.info('Inserting data into %s', table_name)
            connection = database.engine.connect()
            connection.execute(table.insert(), [{'id': n} for n in range(iteration * records_per_iteration + 1, iteration * records_per_iteration + 1 + records_per_iteration)])

            sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(records_per_iteration)
            sdc_executor.stop_pipeline(pipeline)

            records = wiretap.output_records

            # We should get the right number of records
            assert len(records) == records_per_iteration

            expected_number = iteration * records_per_iteration + 1
            for record in records:
                assert record.field['id'].value == expected_number

                expected_number = expected_number + 1
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)
