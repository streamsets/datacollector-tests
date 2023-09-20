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
from streamsets.sdk.utils import Version
from streamsets.testframework.environments.databases import OracleDatabase, SQLServerDatabase, MySqlDatabase, \
    MariaDBDatabase, MemSqlDatabase
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

ROWS_IN_DATABASE = [
    {'id': 1, 'name': 'Dima'},
    {'id': 2, 'name': 'Jarcec'},
    {'id': 3, 'name': 'Arvind'}
]
ROWS_TO_UPDATE = [
    {'id': 2, 'name': 'Eddie'},
    {'id': 4, 'name': 'Jarcec'}
]
LOOKUP_RAW_DATA = ['id'] + [str(row['id']) for row in ROWS_IN_DATABASE]
RAW_DATA = ['name'] + [row['name'] for row in ROWS_IN_DATABASE]

LOOKUP_TABLE_DATA = [
    {'id': 1, 'dept': 'mt', 'name': 'Arvind'},
    {'id': 2, 'dept': 'mt', 'name': 'Girish'},
    {'id': 3, 'dept': 'extra', 'name': 'Dima'}
]

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
                "id" number primary key,
                "value" {sql_type} NULL
            )
        """)

        # And insert a row with actual value
        connection.execute(f"INSERT INTO {table_name} VALUES(1, {insert_fragment})")
        # And a null
        connection.execute(f"INSERT INTO {table_name} VALUES(2, NULL)")

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('Dev Raw Data Source')
        origin.data_format = 'JSON'
        origin.raw_data = '{"id": 1}\n{"id": 2}'
        origin.stop_after_first_batch = True

        lookup = builder.add_stage('JDBC Lookup')
        lookup.sql_query = 'SELECT "value" FROM {0} WHERE '.format(table_name) + '"id" = ${record:value("/id")}'
        lookup.column_mappings = [dict(dataType='USE_COLUMN_TYPE', columnName='value', field='/value')]

        wiretap = builder.add_wiretap()

        origin >> lookup >> wiretap.destination

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        records = wiretap.output_records

        assert len(records) == 2
        record = records[0]
        null_record = records[1]

        # Since we are controlling types, we want to check explicit values inside the record rather the the python
        # wrappers.
        # TLKT-177: Add ability for field to return raw value

        assert record.field['value'].type == expected_type
        assert null_record.field['value'].type == expected_type

        assert null_record.field['value'] == None
        if sql_type == 'XMLType':
            assert record.field['value']._data['value'].strip() == expected_value
        else:
            assert record.field['value']._data['value'] == expected_value
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            connection.execute(f"DROP TABLE {table_name}")


DATA_TYPES_MARIADB = [
    ('BIGINT', '-9223372036854775807', 'LONG', '-9223372036854775807'),
    ('BIGINT UNSIGNED', '18446744073709551615', 'DECIMAL', '18446744073709551615'),
    ('BINARY(5)', "'Hello'", 'BYTE_ARRAY', 'SGVsbG8='),
#    ('BIT(8)',"b'01010101'", 'BYTE_ARRAY', 'VQ=='), # Not supported like on MySQL
    ('BLOB', "'Hello'", 'BYTE_ARRAY', 'SGVsbG8='),
    ('BOOL', '1', 'BOOLEAN', True),
    ('BOOLEAN', '1', 'BOOLEAN', True),
    ('CHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('CHAR(5) BYTE', "'Hello'", 'BYTE_ARRAY', 'SGVsbG8='),
    ('CHAR VARYING(5)', "'Hello'", 'STRING', 'Hello'),
    ('CHARACTER(5)', "'Hello'", 'STRING', 'Hello'),
    ('CHARACTER VARYING(5)', "'Hello'", 'STRING', 'Hello'),
#    ('CLOB', "'Hello'", 'STRING', 'Hello'), # Only available in sql_mode='oracle' as synonym to LONGTEXT
    ('DATE', "'2019-01-01'", 'DATE', 1546300800000),
    ('DATETIME', "'2019-01-01 5:00:00'", 'DATETIME', 1546318800000),
    ('DEC(5, 2)', '5.20', 'DECIMAL', '5.20'),
    ('DECIMAL(5, 2)', '5.20', 'DECIMAL', '5.20'),
    ('DOUBLE', '5.2', 'DOUBLE', '5.2'),
    ('DOUBLE PRECISION', '5.2', 'DOUBLE', '5.2'),
    ("ENUM('a', 'b')", "'a'", 'STRING', 'a'),
    ('FIXED(5, 2)', '5.20', 'DECIMAL', '5.20'),
    ('FLOAT', '5.2', 'FLOAT', '5.2'),
    ('FLOAT4', '5.2', 'FLOAT', '5.2'),
    ('FLOAT8', '5.2', 'DOUBLE', '5.2'),
    ("GEOMETRY", "POINT(1, 1)", 'BYTE_ARRAY', 'AAAAAAEBAAAAAAAAAAAA8D8AAAAAAADwPw=='),
    ("GEOMETRYCOLLECTION", "ST_GeomCollFromText('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 1, 0 2),POINT(1 0))')", 'BYTE_ARRAY', 'AAAAAAEHAAAAAwAAAAEBAAAAAAAAAAAAAAAAAAAAAAAAAAECAAAAAgAAAAAAAAAAAAAAAAAAAAAA8D8AAAAAAAAAAAAAAAAAAABAAQEAAAAAAAAAAADwPwAAAAAAAAAA'),
    ('INET6', '"::192.0.2.42"', 'STRING', '::192.0.2.42'),
    ('INT', '-2147483648', 'INTEGER', '-2147483648'),
    ('INT UNSIGNED', '4294967295', 'DECIMAL', '4294967295'),
    ('INT1', '-128', 'SHORT', -128),
    ('INT1 UNSIGNED', '255', 'SHORT', 255),
    ('INT2', '-32768', 'SHORT', -32768),
    ('INT2 UNSIGNED', '65535', 'LONG', '65535'),
    ('INT4', '-2147483648', 'INTEGER', '-2147483648'),
    ('INT4 UNSIGNED', '4294967295', 'DECIMAL', '4294967295'),
    ('INT8', '-9223372036854775807', 'LONG', '-9223372036854775807'),
    ('INT8 UNSIGNED', '18446744073709551615', 'DECIMAL', '18446744073709551615'),
    ('INTEGER', '-2147483648', 'INTEGER', '-2147483648'),
    ('INTEGER UNSIGNED', '4294967295', 'DECIMAL', '4294967295'),
    ("JSON", "'{\"a\":\"b\"}'", 'STRING', '{\"a\":\"b\"}'),
    ("LINESTRING", "LineString(Point(0,0), Point(10,10), Point(20,25), Point(50,60))", 'BYTE_ARRAY', 'AAAAAAECAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAkQAAAAAAAACRAAAAAAAAANEAAAAAAAAA5QAAAAAAAAElAAAAAAAAATkA='),
    ('LONG', "'Hello'", 'STRING', 'Hello'),
    ('LONG CHAR VARYING', "'Hello'", 'STRING', 'Hello'),
    ('LONG CHARACTER VARYING', "'Hello'", 'STRING', 'Hello'),
    ('LONG VARBINARY', "'Hello'", 'BYTE_ARRAY', 'SGVsbG8='),
    ('LONG VARCHAR', "'Hello'", 'STRING', 'Hello'),
    ('LONG VARCHARACTER', "'Hello'", 'STRING', 'Hello'),
    ('LONGBLOB', "'Hello'", 'BYTE_ARRAY', 'SGVsbG8='),
    ('LONGTEXT', "'Hello'", 'STRING', 'Hello'),
    ('MEDIUMBLOB', "'Hello'", 'BYTE_ARRAY', 'SGVsbG8='),
    ('MEDIUMINT', '-8388608', 'INTEGER', '-8388608'),
    ('MEDIUMINT UNSIGNED', '16777215', 'LONG', '16777215'),
    ('MEDIUMTEXT', "'Hello'", 'STRING', 'Hello'),
    ('MIDDLEINT', '-8388608', 'INTEGER', '-8388608'),
    ('MIDDLEINT UNSIGNED', '16777215', 'LONG', '16777215'),
    ('MULTILINESTRING', "ST_MultiLineStringFromText('MULTILINESTRING((0 40, 0 20, 6 30, 12 20, 12 40),(15 40, 15 20, 25 20, 30 25, 30 35, 25 40, 15 40))')", 'BYTE_ARRAY', 'AAAAAAEFAAAAAgAAAAECAAAABQAAAAAAAAAAAAAAAAAAAAAAREAAAAAAAAAAAAAAAAAAADRAAAAAAAAAGEAAAAAAAAA+QAAAAAAAAChAAAAAAAAANEAAAAAAAAAoQAAAAAAAAERAAQIAAAAHAAAAAAAAAAAALkAAAAAAAABEQAAAAAAAAC5AAAAAAAAANEAAAAAAAAA5QAAAAAAAADRAAAAAAAAAPkAAAAAAAAA5QAAAAAAAAD5AAAAAAACAQUAAAAAAAAA5QAAAAAAAAERAAAAAAAAALkAAAAAAAABEQA=='),
    ('MULTIPOINT', "ST_MultiPointFromText('MULTIPOINT(0 0, 1 0, 1 1, 0 1)')", 'BYTE_ARRAY', 'AAAAAAEEAAAABAAAAAEBAAAAAAAAAAAAAAAAAAAAAAAAAAEBAAAAAAAAAAAA8D8AAAAAAAAAAAEBAAAAAAAAAAAA8D8AAAAAAADwPwEBAAAAAAAAAAAAAAAAAAAAAADwPw=='),
    ('MULTIPOLYGON', "ST_MultiPolygonFromText('MULTIPOLYGON(((0 40, 0 20, 6 30, 12 20, 12 40, 0 40),(15 40, 15 20, 25 20, 30 25, 30 35, 25 40, 15 40)))')", 'BYTE_ARRAY', 'AAAAAAEGAAAAAQAAAAEDAAAAAgAAAAYAAAAAAAAAAAAAAAAAAAAAAERAAAAAAAAAAAAAAAAAAAA0QAAAAAAAABhAAAAAAAAAPkAAAAAAAAAoQAAAAAAAADRAAAAAAAAAKEAAAAAAAABEQAAAAAAAAAAAAAAAAAAAREAHAAAAAAAAAAAALkAAAAAAAABEQAAAAAAAAC5AAAAAAAAANEAAAAAAAAA5QAAAAAAAADRAAAAAAAAAPkAAAAAAAAA5QAAAAAAAAD5AAAAAAACAQUAAAAAAAAA5QAAAAAAAAERAAAAAAAAALkAAAAAAAABEQA=='),
    ('NATIONAL CHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('NATIONAL CHAR VARYING(5)', "'Hello'", 'STRING', 'Hello'),
    ('NATIONAL CHARACTER(5)', "'Hello'", 'STRING', 'Hello'),
    ('NATIONAL CHARACTER VARYING(5)', "'Hello'", 'STRING', 'Hello'),
    ('NCHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('NCHAR VARCHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ('NCHAR VARCHARACTER(32)', "'Hello'", 'STRING', 'Hello'),
    ('NCHAR VARYING(32)', "'Hello'", 'STRING', 'Hello'),
#    ('NUMBER', '5', 'DECIMAL', '5'), # Only available in sql_mode='oracle' as synonym to DECIMAL
    ('NUMERIC(5, 2)', '5.20', 'DECIMAL', '5.20'),
    ('NVARCHAR(5)', "'Hello'", 'STRING', 'Hello'),
    ("POINT", "POINT(1, 1)", 'BYTE_ARRAY', 'AAAAAAEBAAAAAAAAAAAA8D8AAAAAAADwPw=='),
    ("POLYGON", "Polygon(LineString(Point(0,0),Point(10,0),Point(10,10),Point(0,10),Point(0,0)),LineString(Point(5,5),Point(7,5),Point(7,7),Point(5,7),Point(5,5)))", 'BYTE_ARRAY', 'AAAAAAEDAAAAAgAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAJEAAAAAAAAAAAAAAAAAAACRAAAAAAAAAJEAAAAAAAAAAAAAAAAAAACRAAAAAAAAAAAAAAAAAAAAAAAUAAAAAAAAAAAAUQAAAAAAAABRAAAAAAAAAHEAAAAAAAAAUQAAAAAAAABxAAAAAAAAAHEAAAAAAAAAUQAAAAAAAABxAAAAAAAAAFEAAAAAAAAAUQA=='),
#    ('RAW(5)', "'Hello'", 'BYTE_ARRAY', 'SGVsbG8='), # Only available in sql_mode='oracle' as synonym to VARBINARY
    ('REAL', '5.2', 'DOUBLE', '5.2'),
# Serial?
    ("set('a', 'b')", "'a,b'", 'STRING', 'a,b'),
    ('SMALLINT', '-32768', 'SHORT', -32768),
    ('SMALLINT UNSIGNED', '65535', 'LONG', '65535'),
    ('SQL_TSI_YEAR', "'2019'", 'DATE', 1546300800000),
    ('TEXT', "'Hello'", 'STRING', 'Hello'),
    ('TIME', "'5:00:00'", 'TIME', 18000000),
    ('TIMESTAMP', "'2019-01-01 5:00:00'", 'DATETIME', 1546318800000),
    ('TINYBLOB', "'Hello'", 'BYTE_ARRAY', 'SGVsbG8='),
    ('TINYINT', '-128', 'SHORT', -128),
    ('TINYINT UNSIGNED', '255', 'SHORT', 255),
    ('TINYTEXT', "'Hello'", 'STRING', 'Hello'),
    ('VARBINARY(5)', "'Hello'", 'BYTE_ARRAY', 'SGVsbG8='),
    ('VARCHAR(5)', "'Hello'", 'STRING', 'Hello'),
#   ('VARCHAR2(5)', "'Hello'", 'STRING', 'Hello'), # Only available in sql_mode='oracle' as synonym to VARCHAR
    ('VARCHARACTER(5)', "'Hello'", 'STRING', 'Hello'),
    ('YEAR', "'2019'", 'DATE', 1546300800000),
]
@sdc_min_version('5.2.0')
@database('mariadb')
@pytest.mark.parametrize('sql_type,insert_fragment,expected_type,expected_value', DATA_TYPES_MARIADB, ids=[i[0] for i in DATA_TYPES_MARIADB])
def test_data_types_mariadb(sdc_builder, sdc_executor, database, sql_type, insert_fragment, expected_type, expected_value, keep_data):
    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()
    try:
        # Create table
        connection.execute(f"""
            CREATE TABLE {table_name}(
                id int primary key,
                value {sql_type} NULL
            )
        """)

        # And insert a row with actual value
        connection.execute(f"INSERT INTO {table_name} VALUES(1, {insert_fragment})")
        # And a null
        connection.execute(f"INSERT INTO {table_name} VALUES(2, NULL)")

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('Dev Raw Data Source')
        origin.data_format = 'JSON'
        origin.raw_data = '{"id": 1}\n{"id": 2}'
        origin.stop_after_first_batch = True

        lookup = builder.add_stage('JDBC Lookup')
        lookup.sql_query = 'SELECT value FROM {0} WHERE '.format(table_name) + 'id = ${record:value("/id")}'
        lookup.column_mappings = [dict(dataType='USE_COLUMN_TYPE', columnName='value', field='/value')]

        wiretap = builder.add_wiretap()

        origin >> lookup >> wiretap.destination

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        records = wiretap.output_records

        assert len(records) == 2
        record = records[0]
        null_record = records[1]

        # Since we are controlling types, we want to check explicit values inside the record rather the the python
        # wrappers.
        # TLKT-177: Add ability for field to return raw value

        assert record.field['value'].type == expected_type
        assert null_record.field['value'].type == expected_type

        assert record.field['value']._data['value'] == expected_value
        assert null_record.field['value'] == None
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
    ("LINESTRING", "LineString(Point(0,0), Point(10,10), Point(20,25), Point(50,60))", 'BYTE_ARRAY', 'AAAAAAECAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAkQAAAAAAAACRAAAAAAAAANEAAAAAAAAA5QAAAAAAAAElAAAAAAAAATkA='),
    ("POLYGON", "Polygon(LineString(Point(0,0),Point(10,0),Point(10,10),Point(0,10),Point(0,0)),LineString(Point(5,5),Point(7,5),Point(7,7),Point(5,7),Point(5,5)))", 'BYTE_ARRAY', 'AAAAAAEDAAAAAgAAAAUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAJEAAAAAAAAAAAAAAAAAAACRAAAAAAAAAJEAAAAAAAAAAAAAAAAAAACRAAAAAAAAAAAAAAAAAAAAAAAUAAAAAAAAAAAAUQAAAAAAAABRAAAAAAAAAHEAAAAAAAAAUQAAAAAAAABxAAAAAAAAAHEAAAAAAAAAUQAAAAAAAABxAAAAAAAAAFEAAAAAAAAAUQA=='),
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
                value {sql_type} NULL
            )
        """)

        # And insert a row with actual value
        connection.execute(f"INSERT INTO {table_name} VALUES(1, {insert_fragment})")
        # And a null
        connection.execute(f"INSERT INTO {table_name} VALUES(2, NULL)")

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('Dev Raw Data Source')
        origin.data_format = 'JSON'
        origin.raw_data = '{"id": 1}\n{"id": 2}'
        origin.stop_after_first_batch = True

        lookup = builder.add_stage('JDBC Lookup')
        lookup.sql_query = 'SELECT value FROM {0} WHERE '.format(table_name) + 'id = ${record:value("/id")}'
        lookup.column_mappings = [dict(dataType='USE_COLUMN_TYPE', columnName='value', field='/value')]

        wiretap = builder.add_wiretap()

        origin >> lookup >> wiretap.destination

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        records = wiretap.output_records

        assert len(records) == 2
        record = records[0]
        null_record = records[1]

        # Since we are controlling types, we want to check explicit values inside the record rather the the python
        # wrappers.
        # TLKT-177: Add ability for field to return raw value

        assert record.field['value'].type == expected_type
        assert null_record.field['value'].type == expected_type

        assert record.field['value']._data['value'] == expected_value
        assert null_record.field['value'] == None
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
@sdc_min_version('3.21.0')
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
                value {sql_type} NULL
            )
        """)

        # And insert a row with actual value
        connection.execute(f"INSERT INTO {table_name} VALUES(1, {insert_fragment})")
        # And a null
        connection.execute(f"INSERT INTO {table_name} VALUES(2, NULL)")

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('Dev Raw Data Source')
        origin.data_format = 'JSON'
        origin.raw_data = '{"id": 1}\n{"id": 2}'
        origin.stop_after_first_batch = True

        lookup = builder.add_stage('JDBC Lookup')
        lookup.sql_query = 'SELECT value FROM {0} WHERE '.format(table_name) + 'id = ${record:value("/id")}'
        lookup.column_mappings = [dict(dataType='USE_COLUMN_TYPE', columnName='value', field='/value')]
        lookup.on_unknown_type = 'CONVERT_TO_STRING'

        wiretap = builder.add_wiretap()

        origin >> lookup >> wiretap.destination

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        records = wiretap.output_records

        assert len(records) == 2
        record = records[0]
        null_record = records[1]

        # Since we are controlling types, we want to check explicit values inside the record rather the the python
        # wrappers.
        # TLKT-177: Add ability for field to return raw value

        assert record.field['value'].type == expected_type
        assert null_record.field['value'].type == expected_type

        assert record.field['value']._data['value'] == expected_value
        assert null_record.field['value'] == None
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
                value {sql_type} NULL
            )
        """)

        # And insert a row with actual value
        connection.execute(f"INSERT INTO {table_name} VALUES(1, {insert_fragment})")
        # And a null
        connection.execute(f"INSERT INTO {table_name} VALUES(2, NULL)")

        builder = sdc_builder.get_pipeline_builder()

        origin = builder.add_stage('Dev Raw Data Source')
        origin.data_format = 'JSON'
        origin.raw_data = '{"id": 1}\n{"id": 2}'
        origin.stop_after_first_batch = True

        lookup = builder.add_stage('JDBC Lookup')
        lookup.sql_query = 'SELECT value FROM {0} WHERE '.format(table_name) + 'id = ${record:value("/id")}'
        lookup.column_mappings = [dict(dataType='USE_COLUMN_TYPE', columnName='value', field='/value')]

        wiretap = builder.add_wiretap()

        # As a part of SDC-10125, DATETIMEOFFSET is natively supported in SDC, and is converted into ZONED_DATETIME
        if sql_type == 'DATETIMEOFFSET':
            if Version(sdc_executor.version) >= Version('3.14.0'):
                expected_type = 'ZONED_DATETIME'
                expected_value = '2004-05-23T14:25:10.3456-08:00'
            else:
                expected_type = 'STRING'
                expected_value = '2004-05-23 14:25:10.3456 -08:00'
                # This unknown_type_action setting is required, otherwise DATETIMEOFFSET tests for SDC < 3.14 will fail.
                lookup.on_unknown_type = 'CONVERT_TO_STRING'

        origin >> lookup >> wiretap.destination

        pipeline = builder.build().configure_for_environment(database)
        sdc_executor.add_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        records = wiretap.output_records

        assert len(records) == 2
        record = records[0]
        null_record = records[1]

        # Since we are controlling types, we want to check explicit values inside the record rather the the python
        # wrappers.
        # TLKT-177: Add ability for field to return raw value

        assert record.field['value'].type == expected_type
        assert null_record.field['value'].type == expected_type

        assert record.field['value']._data['value'] == expected_value
        assert null_record.field['value'] == None
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            connection.execute(f"DROP TABLE {table_name}")


@database
def test_object_names(sdc_builder, sdc_executor, database):
    pytest.skip("The JDBC Lookup Processor doesn't generate queries - it only takes user input, thus user is responsible to"
                "properly escape or enclose names and thefore there is not much for us to test here.")


@database
def test_multiple_batches(sdc_builder, sdc_executor, database, keep_data):
    if isinstance(database, MemSqlDatabase):
        pytest.skip("Standard Tests are currently only written for MySQL and not for MemSQL (sadly STF threads both DBs the same way)")

    # TODO: UPDATE ME TO LARGE VALUES AGAIN
    batch_size = 1000
    batches = 10
    table_name = get_random_string(string.ascii_lowercase, 20)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True),
        sqlalchemy.Column('value', sqlalchemy.Integer, primary_key=False, quote=True),
        quote = True
    )

    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Data Generator')
    origin.batch_size = batch_size
    origin.fields_to_generate = [{
        "type": "LONG_SEQUENCE",
        "field": "seq"
    }]

    expression = builder.add_stage('Expression Evaluator')
    expression.field_expressions = [ {
        'fieldToSet': '/lookup',
        'expression': '${record:value("/seq") % 3 + 1}'
      } ]

    lookup = builder.add_stage('JDBC Lookup')
    if isinstance(database, OracleDatabase):
        lookup.sql_query = 'SELECT "value" FROM "{0}" WHERE '.format(table_name) + '"id" = ${record:value("/lookup")}'
    else:
        lookup.sql_query = 'SELECT value FROM {0} WHERE '.format(table_name) + 'id = ${record:value("/lookup")}'
    lookup.column_mappings = [dict(dataType='USE_COLUMN_TYPE', columnName='value', field='/value')]

    wiretap = builder.add_wiretap()
    origin >> expression >> lookup >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating table %s', table_name)
        table.create(database.engine)

        logger.info('Inserting data into %s', table_name)
        connection = database.engine.connect()
        connection.execute(table.insert(), [
            {'id': 1, 'value': 10},
            {'id': 2, 'value': 20},
            {'id': 3, 'value': 30}
        ])

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(batches * batch_size)
        sdc_executor.stop_pipeline(pipeline)

        # Now the pipeline will write some amount of records that will be larger, so we get precise count from metrics
        history = sdc_executor.get_pipeline_history(pipeline)
        record_count = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        logger.info(f"Detected {record_count} output records")
        # Sanity check
        assert record_count >= batch_size * batches

        records = wiretap.output_records
        assert len(records) == record_count

        # Verify each record
        def sortFunc(entry):
            return entry.field['seq'].value

        records.sort(key=sortFunc)

        expected_number = 0
        for record in records:
            assert record.field['seq'] == expected_number
            assert record.field['lookup'] == expected_number % 3 + 1
            assert record.field['value'] == (expected_number % 3 + 1) * 10

            expected_number = expected_number + 1
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)


@database
def test_data_format(sdc_builder, sdc_executor, database, keep_data):
    pytest.skip("JDBC Lookup Processor doesn't deal with data formats")


@database
def test_dataflow_events(sdc_builder, sdc_executor, database, keep_data):
    pytest.skip("JDBC Lookup processor does not support events today")


@database
@pytest.mark.parametrize('raw_data', [LOOKUP_RAW_DATA, ['id', str(ROWS_IN_DATABASE[0]['id'])]])
def test_validate_single_and_multiple_records(sdc_builder, sdc_executor, database, credential_store, raw_data):
    """
    The pipeline looks like:
        dev_raw_data_source >> jdbc_lookup >> trash
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_and_populate_lookup_table(table_name, database)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(raw_data),
                                       stop_after_first_batch=True)

    jdbc_lookup = pipeline_builder.add_stage('JDBC Lookup')
    if type(database) in [MySqlDatabase, MariaDBDatabase]:
        query_str = f'SELECT `name` FROM {table_name} WHERE `id` = ${{record:value("/id")}}'
    else:
        query_str = f'SELECT "name" FROM {table_name} WHERE "id" = ${{record:value("/id")}}'
    jdbc_lookup.set_attributes(sql_query=query_str,
                               column_mappings=[])

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> jdbc_lookup >> wiretap.destination
    pipeline = pipeline_builder.build(title='JDBC Lookup Records').configure_for_environment(database, credential_store)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_records = wiretap.output_records
        assert len(output_records) == len(raw_data) - 1, 'Wrong numbers of records'
        for out_record in output_records:
            id = int(out_record.field["id"].value)
            name = [record['name'] for record in LOOKUP_TABLE_DATA if record['id'] == id]
            assert len(name) == 1, 'Wrong record!'
            assert out_record.field["name"] == name[0], 'Wrong record!'
    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@database
def test_default_value(sdc_builder, sdc_executor, database, credential_store):
    """
    The pipeline looks like:
        dev_raw_data_source >> jdbc_lookup >> trash
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_and_populate_lookup_table(table_name, database)
    raw_data = LOOKUP_RAW_DATA

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(raw_data),
                                       stop_after_first_batch=True)

    jdbc_lookup = pipeline_builder.add_stage('JDBC Lookup')
    if type(database) in [MySqlDatabase, MariaDBDatabase]:
        query_str = f'SELECT `name` FROM {table_name} WHERE `id` = -1'
    else:
        query_str = f'SELECT "name" FROM {table_name} WHERE "id" = -1'
    column_mappings = [dict(defaultValue='Albert',
                            dataType='STRING',
                            columnName="name",
                            field='/FirstName')]
    jdbc_lookup.set_attributes(sql_query=query_str,
                               column_mappings=column_mappings)

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> jdbc_lookup >> wiretap.destination
    pipeline = pipeline_builder.build(title='JDBC Lookup Default Value').configure_for_environment(database, credential_store)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_records = wiretap.output_records
        assert len(output_records) == len(raw_data) - 1, 'Wrong numbers of records'
        for out_record in output_records:
            assert out_record.field["FirstName"] == 'Albert', 'Wrong record. Invalid default value!'
    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


@database
def test_retry_on_cache_miss(sdc_builder, sdc_executor, database, credential_store):
    """
    The pipeline looks like:
        dev_raw_data_source >> jdbc_lookup >> trash
    """
    table_name = get_random_string(string.ascii_lowercase, 20)
    table = _create_table(table_name, database)
    raw_data = LOOKUP_RAW_DATA

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(raw_data),
                                       stop_after_first_batch=True)

    jdbc_lookup = pipeline_builder.add_stage('JDBC Lookup')
    if type(database) in [MySqlDatabase, MariaDBDatabase]:
        query_str = f'SELECT `name` FROM {table_name} WHERE `id` = ${{record:value("/id")}}'
    else:
        query_str = f'SELECT "name" FROM {table_name} WHERE "id" = ${{record:value("/id")}}'
    column_mappings = [dict(defaultValue='Albert',
                            dataType='STRING',
                            field='/name',
                            columnName="name")]

    jdbc_lookup.set_attributes(sql_query=query_str,
                               enable_local_caching=True,
                               retry_on_missing_value=True,
                               eviction_policy_type='EXPIRE_AFTER_ACCESS',
                               column_mappings=column_mappings)

    wiretap = pipeline_builder.add_wiretap()
    dev_raw_data_source >> jdbc_lookup >> wiretap.destination
    pipeline = pipeline_builder.build(title='JDBC Lookup Cache').configure_for_environment(database, credential_store)
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_records = wiretap.output_records
        assert len(output_records) == len(raw_data) - 1, 'Wrong numbers of records'
        for out_record in output_records:
            assert out_record.field["name"] == 'Albert', 'Wrong record. Invalid default value!'

        logger.info('Adding %s rows into %s database ...', len(LOOKUP_TABLE_DATA), database.type)
        connection = database.engine.connect()
        connection.execute(table.insert(), LOOKUP_TABLE_DATA)
        connection.close()
        wiretap.reset()

        sdc_executor.start_pipeline(pipeline).wait_for_finished()
        output_records = wiretap.output_records
        assert len(output_records) == len(raw_data) - 1, 'Wrong numbers of records'
        for out_record in output_records:
            id = int(out_record.field["id"].value)
            name = [record['name'] for record in LOOKUP_TABLE_DATA if record['id'] == id]
            assert len(name) == 1, 'Wrong record!'
            assert out_record.field["name"] == name[0], 'Wrong record!'

    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        logger.info('Dropping table %s in %s database...', table_name, database.type)
        table.drop(database.engine)


def _create_and_populate_lookup_table(name, database):
    """Create common lookup table and fill it with data."""
    table = sqlalchemy.Table(name,
                             sqlalchemy.MetaData(),
                             sqlalchemy.Column('id', sqlalchemy.Integer, quote=True),
                             sqlalchemy.Column('dept', sqlalchemy.String(32), quote=True),
                             sqlalchemy.Column('name', sqlalchemy.String(32), quote=True),
                             quote=True
                             )
    logger.info('Creating table %s in %s database ...', name, database.type)
    table.create(database.engine)

    logger.info('Adding %s rows into %s database ...', len(LOOKUP_TABLE_DATA), database.type)
    connection = database.engine.connect()
    connection.execute(table.insert(), LOOKUP_TABLE_DATA)
    connection.close()

    return table


def _create_table(table_name, database, schema_name=None, name_type=sqlalchemy.String(32)):
    """Helper function to create a table with two columns: id (int, PK) and name.

    Args:
        table_name: (:obj:`str`) the name for the new table.
        database: a :obj:`streamsets.testframework.environment.Database` object.
        name_type: sqlalchemy type
        schema_name: (:obj:`str`, optional) when provided, create the new table in a specific schema; otherwise,
            the default schema for the engine’s database connection is used.

    Return:
        The new table as a sqlalchemy.Table object.

    """
    metadata = sqlalchemy.MetaData()
    # quote=True makes the names case sensitive
    if type(database) == SQLServerDatabase:
        table = sqlalchemy.Table(table_name,
                                 metadata,
                                 sqlalchemy.Column('name', name_type, quote=True),
                                 sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True,
                                                   autoincrement=False, quote=True),
                                 schema=schema_name,
                                 quote=True)
    else:
        table = sqlalchemy.Table(table_name,
                                 metadata,
                                 sqlalchemy.Column('name', name_type, quote=True),
                                 sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True, quote=True),
                                 schema=schema_name,
                                 quote=True)

    logger.info('Creating table %s in %s database ...', table_name, database.type)
    table.create(database.engine)
    return table
