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

import datetime
import logging
import string
import json

import pytest
import sqlalchemy
from ..utils.utils_postgresql import compare_database_server_version
from streamsets.testframework.environments.databases import MySqlDatabase, MariaDBDatabase, OracleDatabase, MemSqlDatabase
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string, Version

logger = logging.getLogger(__name__)


DATA_TYPES_ORACLE = [
    # Oracle actually doesn't have boolean as a type, it does accept boolen insert into char(1) fields though
    ('true', 'BOOLEAN', 'char(1)', '1'),
    ('true', 'BOOLEAN', 'varchar(1)', '1'),
    ('true', 'BOOLEAN', 'nchar(1)', '1'),
    ('true', 'BOOLEAN', 'nvarchar2(1)', '1'),
    # Byte
    ('65', 'BYTE', 'char(2)', '65'),
    # Char
    ('a', 'CHAR', 'char(1)', 'a'),
    ('a', 'CHAR', 'varchar(1)', 'a'),
    ('a', 'CHAR', 'nchar(1)', 'a'),
    ('a', 'CHAR', 'nvarchar2(1)', 'a'),
    # Short
    (120, 'SHORT', 'number', 120),
    (120, 'SHORT', 'char(5)', '120  '),
    (120, 'SHORT', 'varchar(5)', '120'),
    (120, 'SHORT', 'nchar(5)', '120  '),
    (120, 'SHORT', 'nvarchar2(5)', '120'),
    (120, 'SHORT', 'long', '120'),
    # Integer
    (120, 'INTEGER', 'number', 120),
    (120, 'INTEGER', 'char(5)', '120  '),
    (120, 'INTEGER', 'varchar(5)', '120'),
    (120, 'INTEGER', 'nchar(5)', '120  '),
    (120, 'INTEGER', 'nvarchar2(5)', '120'),
    (120, 'INTEGER', 'long', '120'),
    # Long
    (120, 'LONG', 'number', 120),
    (120, 'LONG', 'char(5)', '120  '),
    (120, 'LONG', 'varchar(5)', '120'),
    (120, 'LONG', 'nchar(5)', '120  '),
    (120, 'LONG', 'nvarchar2(5)', '120'),
    (120, 'LONG', 'long', '120'),
    # Float
    (120.0, 'FLOAT', 'number', 120.0),
    (120.0, 'FLOAT', 'char(5)', '120.0'),
    (120.0, 'FLOAT', 'varchar(5)', '120.0'),
    (120.0, 'FLOAT', 'nchar(5)', '120.0'),
    (120.0, 'FLOAT', 'nvarchar2(5)', '120.0'),
    (120.0, 'FLOAT', 'long', '120.0'),
    # Double
    (120.0, 'DOUBLE', 'number', 120.0),
    (120.0, 'DOUBLE', 'char(5)', '120.0'),
    (120.0, 'DOUBLE', 'varchar(5)', '120.0'),
    (120.0, 'DOUBLE', 'nchar(5)', '120.0'),
    (120.0, 'DOUBLE', 'nvarchar2(5)', '120.0'),
    (120.0, 'DOUBLE', 'long', '120.0'),
    # Decimal
    (120.0, 'DECIMAL', 'number', 120.0),
    (120.0, 'DECIMAL', 'char(6)', '120.00'),
    (120.0, 'DECIMAL', 'varchar(6)', '120.00'),
    (120.0, 'DECIMAL', 'nchar(6)', '120.00'),
    (120.0, 'DECIMAL', 'nvarchar2(6)', '120.00'),
    (120.0, 'DECIMAL', 'long', '120.00'),
    # Date
    ('2020-01-01 10:00:00', 'DATE', 'char(50)', 'Wed Jan 01 10:00:00 GMT 2020                      '),
    ('2020-01-01 10:00:00', 'DATE', 'varchar(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATE', 'varchar2(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATE', 'nchar(50)', 'Wed Jan 01 10:00:00 GMT 2020                      '),
    ('2020-01-01 10:00:00', 'DATE', 'nvarchar2(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATE', 'long', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATE', 'date', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATE', 'timestamp', datetime.datetime(2020, 1, 1, 10, 0)),
    # Time
    ('2020-01-01 10:00:00', 'TIME', 'char(50)', 'Wed Jan 01 10:00:00 GMT 2020                      '),
    ('2020-01-01 10:00:00', 'TIME', 'varchar(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'TIME', 'varchar2(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'TIME', 'nchar(50)', 'Wed Jan 01 10:00:00 GMT 2020                      '),
    ('2020-01-01 10:00:00', 'TIME', 'nvarchar2(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'TIME', 'long', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'TIME', 'date', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'TIME', 'timestamp', datetime.datetime(2020, 1, 1, 10, 0)),
    # DateTime
    ('2020-01-01 10:00:00', 'DATETIME', 'char(50)', 'Wed Jan 01 10:00:00 GMT 2020                      '),
    ('2020-01-01 10:00:00', 'DATETIME', 'varchar(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATETIME', 'varchar2(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATETIME', 'nchar(50)', 'Wed Jan 01 10:00:00 GMT 2020                      '),
    ('2020-01-01 10:00:00', 'DATETIME', 'nvarchar2(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATETIME', 'long', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATETIME', 'date', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATETIME', 'timestamp', datetime.datetime(2020, 1, 1, 10, 0)),
    # Zoned DateTime
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'char(50)', '2020-01-01 10:00:00.000+0000                      '),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'varchar(50)', '2020-01-01 10:00:00.000+0000'),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'varchar2(50)', '2020-01-01 10:00:00.000+0000'),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'nchar(50)', '2020-01-01 10:00:00.000+0000                      '),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'nvarchar2(50)', '2020-01-01 10:00:00.000+0000'),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'long', '2020-01-01 10:00:00.000+0000'),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'date', datetime.datetime(2020, 1, 1, 10, 0)),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'timestamp', datetime.datetime(2020, 1, 1, 10, 0)),
#  This test is disabled because it's timezone dependent (you run on local host it will return different value then
#  on Jenkins, ...). Which is actually correct - the type is after all "local timezone" and is always converted by
#  the Oracle database. We can however insert the data to this type.
#    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'timestamp with local time zone', datetime.datetime(2020, 1, 1, 3, 0)),
    # String
    ('string', 'STRING', 'char(15)', 'string         '),
    ('string', 'STRING', 'varchar(15)', 'string'),
    ('string', 'STRING', 'nchar(15)', 'string         '),
    ('string', 'STRING', 'nvarchar2(15)', 'string'),
    ('55', 'STRING', 'number', 55),
    ('1998-1-1 6:22:33', 'STRING', 'date', datetime.datetime(1998, 1, 1, 6, 22, 33)),
    ('1998-1-1 6:22:33', 'STRING', 'timestamp', datetime.datetime(1998, 1, 1, 6, 22, 33)),
    ('55', 'STRING', 'long', '55'),
    ('55', 'STRING', 'blob', b'U'),
    ('55', 'STRING', 'clob', '55'),
    ('55', 'STRING', 'nclob', '55'),
    ('<a></a>', 'STRING', 'xmltype', '<a/>\n'),
    # Byte array
    ('string', 'BYTE_ARRAY', 'char(15)', 'string         '),
    ('string', 'BYTE_ARRAY', 'varchar(15)', 'string'),
    ('string', 'BYTE_ARRAY', 'varchar2(15)', 'string'),
    ('string', 'BYTE_ARRAY', 'nchar(15)', 'string         '),
    ('string', 'BYTE_ARRAY', 'nvarchar2(15)', 'string'),
    ('string', 'BYTE_ARRAY', 'long', 'string'),
    ('string', 'BYTE_ARRAY', 'blob', b'string'),
]
@database('oracle')
@pytest.mark.parametrize('input,converter_type,database_type,expected', DATA_TYPES_ORACLE, ids=[f"{i[1]}-{i[2]}" for i in DATA_TYPES_ORACLE])
def test_data_types_oracle(sdc_builder, sdc_executor, input, converter_type, database_type, expected, database, keep_data):
    _test_data_types(sdc_builder, sdc_executor, input, converter_type, database_type, expected, database, keep_data)


DATA_TYPES_MARIADB = [
    # Boolean
    ('true', 'BOOLEAN', 'char(1)', '1'),
    ('true', 'BOOLEAN', 'char(5)', '1'),
    ('true', 'BOOLEAN', 'int', 1),
    # Byte
    ('65', 'BYTE', 'char(2)', '65'),
    # Char
    ('a', 'CHAR', 'char(1)', 'a'),
    ('a', 'CHAR', 'varchar(1)', 'a'),
    ('a', 'CHAR', 'text', 'a'),
    # Short
    (120, 'SHORT', 'tinyint', 120),
    (120, 'SHORT', 'tinyint unsigned', 120),
    (120, 'SHORT', 'smallint', 120),
    (120, 'SHORT', 'smallint unsigned', 120),
    (120, 'SHORT', 'mediumint', 120),
    (120, 'SHORT', 'mediumint unsigned', 120),
    (120, 'SHORT', 'int', 120),
    (120, 'SHORT', 'int unsigned', 120),
    (120, 'SHORT', 'bigint', 120),
    (120, 'SHORT', 'bigint unsigned', 120),
    (120, 'SHORT', 'decimal(5,2)', 120.0),
    (120, 'SHORT', 'numeric(5,2)', 120.0),
    (120, 'SHORT', 'char(5)', '120'),
    (120, 'SHORT', 'varchar(5)', '120'),
    (120, 'SHORT', 'binary(5)', b'120\x00\x00'),
    (120, 'SHORT', 'varbinary(5)', b'120'),
    (120, 'SHORT', 'text', '120'),
    (120, 'SHORT', 'blob', b'120'),
    # Integer
    (120, 'INTEGER', 'tinyint', 120),
    (120, 'INTEGER', 'tinyint unsigned', 120),
    (120, 'INTEGER', 'smallint', 120),
    (120, 'INTEGER', 'smallint unsigned', 120),
    (120, 'INTEGER', 'mediumint', 120),
    (120, 'INTEGER', 'mediumint unsigned', 120),
    (120, 'INTEGER', 'int', 120),
    (120, 'INTEGER', 'int unsigned', 120),
    (120, 'INTEGER', 'bigint', 120),
    (120, 'INTEGER', 'bigint unsigned', 120),
    (120, 'INTEGER', 'decimal(5,2)', 120.0),
    (120, 'INTEGER', 'numeric(5,2)', 120.0),
    (120, 'INTEGER', 'char(5)', '120'),
    (120, 'INTEGER', 'varchar(5)', '120'),
    (120, 'INTEGER', 'binary(5)', b'120\x00\x00'),
    (120, 'INTEGER', 'varbinary(5)', b'120'),
    (120, 'INTEGER', 'text', '120'),
    (120, 'INTEGER', 'blob', b'120'),
    # Long
    (120, 'LONG', 'tinyint', 120),
    (120, 'LONG', 'tinyint unsigned', 120),
    (120, 'LONG', 'smallint', 120),
    (120, 'LONG', 'smallint unsigned', 120),
    (120, 'LONG', 'mediumint', 120),
    (120, 'LONG', 'mediumint unsigned', 120),
    (120, 'LONG', 'int', 120),
    (120, 'LONG', 'int unsigned', 120),
    (120, 'LONG', 'bigint', 120),
    (120, 'LONG', 'bigint unsigned', 120),
    (120, 'LONG', 'decimal(5,2)', 120.0),
    (120, 'LONG', 'numeric(5,2)', 120.0),
    (120, 'LONG', 'char(5)', '120'),
    (120, 'LONG', 'varchar(5)', '120'),
    (120, 'LONG', 'binary(5)', b'120\x00\x00'),
    (120, 'LONG', 'varbinary(5)', b'120'),
    (120, 'LONG', 'text', '120'),
    (120, 'LONG', 'blob', b'120'),
    # Float
    (120.0, 'FLOAT', 'numeric(5,2)', 120.0),
    (120.0, 'FLOAT', 'decimal(5,2)', 120.0),
    (120.0, 'FLOAT', 'float', 120.0),
    (120.0, 'FLOAT', 'double', 120.0),
    (120.0, 'FLOAT', 'char(5)', '120.0'),
    (120.0, 'FLOAT', 'varchar(5)', '120.0'),
    (120.0, 'FLOAT', 'binary(5)', b'120.0'),
    (120.0, 'FLOAT', 'varbinary(5)', b'120.0'),
    (120.0, 'FLOAT', 'text', '120.0'),
    (120.0, 'FLOAT', 'blob', b'120.0'),
    # Double
    (120.0, 'DOUBLE', 'numeric(5,2)', 120.0),
    (120.0, 'DOUBLE', 'decimal(5,2)', 120.0),
    (120.0, 'DOUBLE', 'float', 120.0),
    (120.0, 'DOUBLE', 'double', 120.0),
    (120.0, 'DOUBLE', 'char(5)', '120.0'),
    (120.0, 'DOUBLE', 'varchar(5)', '120.0'),
    (120.0, 'DOUBLE', 'binary(5)', b'120.0'),
    (120.0, 'DOUBLE', 'varbinary(5)', b'120.0'),
    (120.0, 'DOUBLE', 'text', '120.0'),
    (120.0, 'DOUBLE', 'blob', b'120.0'),
    # Decimal
    (120.0, 'DECIMAL', 'numeric(5,2)', 120.00),
    (120.0, 'DECIMAL', 'decimal(5,2)', 120.00),
    (120.0, 'DECIMAL', 'float', 120.0),
    (120.0, 'DECIMAL', 'double', 120.0),
    (120.0, 'DECIMAL', 'char(5)', '120.0'),
    (120.0, 'DECIMAL', 'varchar(5)', '120.0'),
    (120.0, 'DECIMAL', 'binary(5)', b'120.0'),
    (120.0, 'DECIMAL', 'varbinary(5)', b'120.0'),
    (120.0, 'DECIMAL', 'text', '120.00'),
    (120.0, 'DECIMAL', 'blob', b'120.00'),
    # Date
    ('2020-01-01 10:00:00', 'DATE', 'date', datetime.date(2020, 1, 1)),
    ('2020-01-01 10:00:00', 'DATE', 'datetime', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATE', 'timestamp', datetime.datetime(2020, 1, 1, 10, 0)),
    # Time
    ('2020-01-01 10:00:00', 'TIME', 'date', datetime.date(2020, 1, 1)),
    ('2020-01-01 10:00:00', 'TIME', 'datetime', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'TIME', 'timestamp', datetime.datetime(2020, 1, 1, 10, 0)),
    # DateTime
    ('2020-01-01 10:00:00', 'DATETIME', 'date', datetime.date(2020, 1, 1)),
    ('2020-01-01 10:00:00', 'DATETIME', 'datetime', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATETIME', 'timestamp', datetime.datetime(2020, 1, 1, 10, 0)),
    # Zoned DateTime
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'date', datetime.date(2020, 1, 1)),
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'datetime', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'timestamp', datetime.datetime(2020, 1, 1, 10, 0)),
    # String
    ('120', 'STRING', 'tinyint', 120),
    ('120', 'STRING', 'tinyint unsigned', 120),
    ('120', 'STRING', 'smallint', 120),
    ('120', 'STRING', 'smallint unsigned', 120),
    ('120', 'STRING', 'mediumint', 120),
    ('120', 'STRING', 'mediumint unsigned', 120),
    ('120', 'STRING', 'int', 120),
    ('120', 'STRING', 'int unsigned', 120),
    ('120', 'STRING', 'bigint', 120),
    ('120', 'STRING', 'bigint unsigned', 120),
    ('120.0', 'STRING', 'decimal(5,2)', 120.0),
    ('120.0', 'STRING', 'numeric(5,2)', 120.0),
    ('120.0', 'STRING', 'float', 120.0),
    ('120.0', 'STRING', 'double', 120.0),
    ('1998-01-01', 'STRING', 'date', datetime.date(1998, 1, 1)),
    ('1998-01-01 06:11:22', 'STRING', 'datetime', datetime.datetime(1998, 1, 1, 6, 11, 22)),
    ('1998-01-01 06:11:22', 'STRING', 'timestamp', datetime.datetime(1998, 1, 1, 6, 11, 22)),
    ('06:11:22', 'STRING', 'time', datetime.timedelta(0, 22282)),
    ('string', 'STRING', 'char(6)', 'string'),
    ('string', 'STRING', 'varchar(6)', 'string'),
    ('string', 'STRING', 'binary(6)', b'string'),
    ('string', 'STRING', 'varbinary(6)', b'string'),
    ('string', 'STRING', 'text', 'string'),
    ('string', 'STRING', 'blob', b'string'),
    ('a', 'STRING', "enum('a', 'b')", 'a'),
    ('a', 'STRING', "set('a', 'b')", 'a'),
    # Byte array
    ('string', 'BYTE_ARRAY', 'blob', b'string'),
]
@database('mariadb')
@pytest.mark.parametrize('input,converter_type,database_type,expected', DATA_TYPES_MARIADB, ids=[f"{i[1]}-{i[2]}" for i in DATA_TYPES_MARIADB])
def test_data_types_mariadb(sdc_builder, sdc_executor, input, converter_type, database_type, expected, database, keep_data):
    _test_data_types(sdc_builder, sdc_executor, input, converter_type, database_type, expected, database, keep_data)


DATA_TYPES_MYSQL = [
    # Boolean
    ('true', 'BOOLEAN', 'char(1)', 't'),
    ('true', 'BOOLEAN', 'char(5)', 'true'),
    ('true', 'BOOLEAN', 'int', 1),
    # Byte
    ('65', 'BYTE', 'char(2)', '65'),
    # Char
    ('a', 'CHAR', 'char(1)', 'a'),
    ('a', 'CHAR', 'varchar(1)', 'a'),
    ('a', 'CHAR', 'binary(1)', b'a'),
    ('a', 'CHAR', 'varbinary(1)', b'a'),
    ('a', 'CHAR', 'text', 'a'),
    ('a', 'CHAR', 'blob', b'a'),
    # Short
    (120, 'SHORT', 'tinyint', 120),
    (120, 'SHORT', 'tinyint unsigned', 120),
    (120, 'SHORT', 'smallint', 120),
    (120, 'SHORT', 'smallint unsigned', 120),
    (120, 'SHORT', 'mediumint', 120),
    (120, 'SHORT', 'mediumint unsigned', 120),
    (120, 'SHORT', 'int', 120),
    (120, 'SHORT', 'int unsigned', 120),
    (120, 'SHORT', 'bigint', 120),
    (120, 'SHORT', 'bigint unsigned', 120),
    (120, 'SHORT', 'decimal(5,2)', 120.0),
    (120, 'SHORT', 'numeric(5,2)', 120.0),
    (120, 'SHORT', 'char(5)', '120'),
    (120, 'SHORT', 'varchar(5)', '120'),
    (120, 'SHORT', 'binary(5)', b'120\x00\x00'),
    (120, 'SHORT', 'varbinary(5)', b'120'),
    (120, 'SHORT', 'text', '120'),
    (120, 'SHORT', 'blob', b'120'),
    # Integer
    (120, 'INTEGER', 'tinyint', 120),
    (120, 'INTEGER', 'tinyint unsigned', 120),
    (120, 'INTEGER', 'smallint', 120),
    (120, 'INTEGER', 'smallint unsigned', 120),
    (120, 'INTEGER', 'mediumint', 120),
    (120, 'INTEGER', 'mediumint unsigned', 120),
    (120, 'INTEGER', 'int', 120),
    (120, 'INTEGER', 'int unsigned', 120),
    (120, 'INTEGER', 'bigint', 120),
    (120, 'INTEGER', 'bigint unsigned', 120),
    (120, 'INTEGER', 'decimal(5,2)', 120.0),
    (120, 'INTEGER', 'numeric(5,2)', 120.0),
    (120, 'INTEGER', 'char(5)', '120'),
    (120, 'INTEGER', 'varchar(5)', '120'),
    (120, 'INTEGER', 'binary(5)', b'120\x00\x00'),
    (120, 'INTEGER', 'varbinary(5)', b'120'),
    (120, 'INTEGER', 'text', '120'),
    (120, 'INTEGER', 'blob', b'120'),
    # Long
    (120, 'LONG', 'tinyint', 120),
    (120, 'LONG', 'tinyint unsigned', 120),
    (120, 'LONG', 'smallint', 120),
    (120, 'LONG', 'smallint unsigned', 120),
    (120, 'LONG', 'mediumint', 120),
    (120, 'LONG', 'mediumint unsigned', 120),
    (120, 'LONG', 'int', 120),
    (120, 'LONG', 'int unsigned', 120),
    (120, 'LONG', 'bigint', 120),
    (120, 'LONG', 'bigint unsigned', 120),
    (120, 'LONG', 'decimal(5,2)', 120.0),
    (120, 'LONG', 'numeric(5,2)', 120.0),
    (120, 'LONG', 'char(5)', '120'),
    (120, 'LONG', 'varchar(5)', '120'),
    (120, 'LONG', 'binary(5)', b'120\x00\x00'),
    (120, 'LONG', 'varbinary(5)', b'120'),
    (120, 'LONG', 'text', '120'),
    (120, 'LONG', 'blob', b'120'),
    # Float
    (120.0, 'FLOAT', 'numeric(5,2)', 120.0),
    (120.0, 'FLOAT', 'decimal(5,2)', 120.0),
    (120.0, 'FLOAT', 'float', 120.0),
    (120.0, 'FLOAT', 'double', 120.0),
    (120.0, 'FLOAT', 'char(5)', '120.0'),
    (120.0, 'FLOAT', 'varchar(5)', '120.0'),
    (120.0, 'FLOAT', 'binary(5)', b'120.0'),
    (120.0, 'FLOAT', 'varbinary(5)', b'120.0'),
    (120.0, 'FLOAT', 'text', '120.0'),
    (120.0, 'FLOAT', 'blob', b'120.0'),
    # Double
    (120.0, 'DOUBLE', 'numeric(5,2)', 120.0),
    (120.0, 'DOUBLE', 'decimal(5,2)', 120.0),
    (120.0, 'DOUBLE', 'float', 120.0),
    (120.0, 'DOUBLE', 'double', 120.0),
    (120.0, 'DOUBLE', 'char(5)', '120.0'),
    (120.0, 'DOUBLE', 'varchar(5)', '120.0'),
    (120.0, 'DOUBLE', 'binary(5)', b'120.0'),
    (120.0, 'DOUBLE', 'varbinary(5)', b'120.0'),
    (120.0, 'DOUBLE', 'text', '120.0'),
    (120.0, 'DOUBLE', 'blob', b'120.0'),
    # Decimal
    (120.0, 'DECIMAL', 'numeric(5,2)', 120.00),
    (120.0, 'DECIMAL', 'decimal(5,2)', 120.00),
    (120.0, 'DECIMAL', 'float', 120.0),
    (120.0, 'DECIMAL', 'double', 120.0),
    (120.0, 'DECIMAL', 'char(5)', '120.0'),
    (120.0, 'DECIMAL', 'varchar(5)', '120.0'),
    (120.0, 'DECIMAL', 'binary(5)', b'120.0'),
    (120.0, 'DECIMAL', 'varbinary(5)', b'120.0'),
    (120.0, 'DECIMAL', 'text', '120.00'),
    (120.0, 'DECIMAL', 'blob', b'120.00'),
    # Date
    ('2020-01-01 10:00:00', 'DATE', 'date', datetime.date(2020, 1, 1)),
    ('2020-01-01 10:00:00', 'DATE', 'datetime', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATE', 'timestamp', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATE', 'char(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATE', 'varchar(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATE', 'binary(50)', b'Wed Jan 01 10:00:00 GMT 2020\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'),
    ('2020-01-01 10:00:00', 'DATE', 'varbinary(50)', b'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATE', 'text', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATE', 'blob', b'Wed Jan 01 10:00:00 GMT 2020'),
    # Time
    ('2020-01-01 10:00:00', 'TIME', 'date', datetime.date(2020, 1, 1)),
    ('2020-01-01 10:00:00', 'TIME', 'datetime', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'TIME', 'timestamp', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'TIME', 'char(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'TIME', 'varchar(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'TIME', 'binary(50)', b'Wed Jan 01 10:00:00 GMT 2020\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'),
    ('2020-01-01 10:00:00', 'TIME', 'varbinary(50)', b'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'TIME', 'text', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'TIME', 'blob', b'Wed Jan 01 10:00:00 GMT 2020'),
    # DateTime
    ('2020-01-01 10:00:00', 'DATETIME', 'date', datetime.date(2020, 1, 1)),
    ('2020-01-01 10:00:00', 'DATETIME', 'datetime', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATETIME', 'timestamp', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATETIME', 'char(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATETIME', 'varchar(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATETIME', 'binary(50)', b'Wed Jan 01 10:00:00 GMT 2020\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'),
    ('2020-01-01 10:00:00', 'DATETIME', 'varbinary(50)', b'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATETIME', 'text', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATETIME', 'blob', b'Wed Jan 01 10:00:00 GMT 2020'),
    # String
    ('120', 'STRING', 'tinyint', 120),
    ('120', 'STRING', 'tinyint unsigned', 120),
    ('120', 'STRING', 'smallint', 120),
    ('120', 'STRING', 'smallint unsigned', 120),
    ('120', 'STRING', 'mediumint', 120),
    ('120', 'STRING', 'mediumint unsigned', 120),
    ('120', 'STRING', 'int', 120),
    ('120', 'STRING', 'int unsigned', 120),
    ('120', 'STRING', 'bigint', 120),
    ('120', 'STRING', 'bigint unsigned', 120),
    ('120.0', 'STRING', 'decimal(5,2)', 120.0),
    ('120.0', 'STRING', 'numeric(5,2)', 120.0),
    ('120.0', 'STRING', 'float', 120.0),
    ('120.0', 'STRING', 'double', 120.0),
    ('1998-01-01', 'STRING', 'date', datetime.date(1998, 1, 1)),
    ('1998-01-01 06:11:22', 'STRING', 'datetime', datetime.datetime(1998, 1, 1, 6, 11, 22)),
    ('1998-01-01 06:11:22', 'STRING', 'timestamp', datetime.datetime(1998, 1, 1, 6, 11, 22)),
    ('06:11:22', 'STRING', 'time', datetime.timedelta(0, 22282)),
    ('string', 'STRING', 'char(6)', 'string'),
    ('string', 'STRING', 'varchar(6)', 'string'),
    ('string', 'STRING', 'binary(6)', b'string'),
    ('string', 'STRING', 'varbinary(6)', b'string'),
    ('string', 'STRING', 'text', 'string'),
    ('string', 'STRING', 'blob', b'string'),
    ('a', 'STRING', "enum('a', 'b')", 'a'),
    ('a', 'STRING', "set('a', 'b')", 'a'),
    # Byte array
    ('string', 'BYTE_ARRAY', 'blob', b'string'),
]
@database('mysql')
@pytest.mark.parametrize('input,converter_type,database_type,expected', DATA_TYPES_MYSQL, ids=[f"{i[1]}-{i[2]}" for i in DATA_TYPES_MYSQL])
def test_data_types_mysql(sdc_builder, sdc_executor, input, converter_type, database_type, expected, database, keep_data):
    if isinstance(database, MemSqlDatabase):
        pytest.skip("Standard Tests are currently only written for MySQL and not for MemSQL (sadly STF threads both "
                    "DBs the same way)")
    _test_data_types(sdc_builder, sdc_executor, input, converter_type, database_type, expected, database, keep_data)


DATA_TYPES_ZONED_DATETIME_MYSQL = [
    #Zoned Datetime
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'char(50)', '2020-01-01 10:00:00Z', '2020-01-01T10:00Z'),
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'varchar(50)', '2020-01-01 10:00:00Z', '2020-01-01T10:00Z'),
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'binary(20)', b'2020-01-01T10:00Z\x00\x00\x00',
     b'2020-01-01T10:00Z\x00\x00\x00'),
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'varbinary(50)', b'2020-01-01T10:00Z', b'2020-01-01T10:00Z'),
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'text', '2020-01-01 10:00:00Z', '2020-01-01T10:00Z'),
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'blob', b'2020-01-01T10:00Z', b'2020-01-01T10:00Z'),
]
@database('mysql')
@pytest.mark.parametrize('input,converter_type,database_type,expected_new_database_version,'
                         'expected_old_database_version', DATA_TYPES_ZONED_DATETIME_MYSQL,
                         ids=[f"{i[1]}-{i[2]}" for i in DATA_TYPES_ZONED_DATETIME_MYSQL])
def test_data_types_zoned_data_time_mysql(sdc_builder, sdc_executor, input, converter_type, database_type,
                                          expected_new_database_version, expected_old_database_version, database,
                                          keep_data):

    if isinstance(database, MemSqlDatabase):
        pytest.skip("Standard Tests are currently only written for MySQL and not for MemSQL (sadly STF threads both DBs"
                    " the same way)")

    # In MySQL 8.0 the value of zoned data time is different than in mySQL 5.7 we need to return the correct
    # value for the database version.
    expected = expected_old_database_version if Version(database.version) < Version('8.0.0') \
        else expected_new_database_version

    _test_data_types(sdc_builder, sdc_executor, input, converter_type, database_type, expected, database, keep_data)


DATA_TYPES_POSTGRESQL = [
    # Boolean
    ('true', 'BOOLEAN', 'char(4)', 'true'),
    ('true', 'BOOLEAN', 'int', 1),
    ('true', 'BOOLEAN', 'boolean', True),
    # Byte
    ('65', 'BYTE', 'char(2)', '65'),
    # Char
    ('a', 'CHAR', 'char(1)', 'a'),
    ('a', 'CHAR', 'varchar(1)', 'a'),
    ('a', 'CHAR', 'text', 'a'),
    # Short
    (120, 'SHORT', 'smallint', 120),
    (120, 'SHORT', 'integer', 120),
    (120, 'SHORT', 'bigint', 120),
    (120, 'SHORT', 'decimal(5,2)', 120),
    (120, 'SHORT', 'numeric(5,2)', 120),
    (120, 'SHORT', 'real', 120),
    (120, 'SHORT', 'double precision', 120),
    (120, 'SHORT', 'char(3)', '120'),
    (120, 'SHORT', 'varchar(3)', '120'),
    (120, 'SHORT', 'text', '120'),
    # Integer
    (120, 'INTEGER', 'smallint', 120),
    (120, 'INTEGER', 'integer', 120),
    (120, 'INTEGER', 'bigint', 120),
    (120, 'INTEGER', 'decimal(5,2)', 120),
    (120, 'INTEGER', 'numeric(5,2)', 120),
    (120, 'INTEGER', 'real', 120),
    (120, 'INTEGER', 'double precision', 120),
    (120, 'INTEGER', 'char(3)', '120'),
    (120, 'INTEGER', 'varchar(3)', '120'),
    (120, 'INTEGER', 'text', '120'),
    # Long
    (120, 'LONG', 'smallint', 120),
    (120, 'LONG', 'integer', 120),
    (120, 'LONG', 'bigint', 120),
    (120, 'LONG', 'decimal(5,2)', 120),
    (120, 'LONG', 'numeric(5,2)', 120),
    (120, 'LONG', 'real', 120),
    (120, 'LONG', 'double precision', 120),
    (120, 'LONG', 'char(3)', '120'),
    (120, 'LONG', 'varchar(3)', '120'),
    (120, 'LONG', 'text', '120'),
    # Float
    (120.0, 'FLOAT', 'decimal(5,2)', 120.0),
    (120.0, 'FLOAT', 'numeric(5,2)', 120.0),
    (120.0, 'FLOAT', 'real', 120.0),
    (120.0, 'FLOAT', 'double precision', 120.0),
    (120.0, 'FLOAT', 'char(5)', '120.0'),
    (120.0, 'FLOAT', 'varchar(5)', '120.0'),
    (120.0, 'FLOAT', 'text', '120.0'),
    # Double
    (120.0, 'DOUBLE', 'decimal(5,2)', 120.0),
    (120.0, 'DOUBLE', 'numeric(5,2)', 120.0),
    (120.0, 'DOUBLE', 'real', 120.0),
    (120.0, 'DOUBLE', 'double precision', 120.0),
    (120.0, 'DOUBLE', 'char(5)', '120.0'),
    (120.0, 'DOUBLE', 'varchar(5)', '120.0'),
    (120.0, 'DOUBLE', 'text', '120.0'),
    # Decimal
    (120, 'DECIMAL', 'smallint', 120),
    (120, 'DECIMAL', 'integer', 120),
    (120, 'DECIMAL', 'bigint', 120),
    (120, 'DECIMAL', 'decimal(5,2)', 120.0),
    (120, 'DECIMAL', 'numeric(5,2)', 120.0),
    (120, 'DECIMAL', 'real', 120.0),
    (120, 'DECIMAL', 'double precision', 120.0),
    (120, 'DECIMAL', 'char(6)', '120.00'),
    (120, 'DECIMAL', 'varchar(6)', '120.00'),
    (120, 'DECIMAL', 'text', '120.00'),
    # Date
    ('2020-01-01 10:00:00', 'DATE', 'date', datetime.date(2020, 1, 1)),
    ('2020-01-01 10:00:00', 'DATE', 'timestamp', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATE', 'timestamp with time zone', datetime.datetime(2020, 1, 1, 10, 0, tzinfo=datetime.timezone.utc)),
    ('2020-01-01 10:00:00', 'DATE', 'char(30)', 'Wed Jan 01 10:00:00 GMT 2020  '),
    ('2020-01-01 10:00:00', 'DATE', 'varchar(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATE', 'text', 'Wed Jan 01 10:00:00 GMT 2020'),
    # Time
    ('2020-01-01 10:00:00', 'TIME', 'time', datetime.time(10, 0)),
    ('2020-01-01 10:00:00', 'TIME', 'char(30)', 'Wed Jan 01 10:00:00 GMT 2020  '),
    ('2020-01-01 10:00:00', 'TIME', 'varchar(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'TIME', 'text', 'Wed Jan 01 10:00:00 GMT 2020'),
    # DateTime
    ('2020-01-01 10:00:00', 'DATETIME', 'date', datetime.date(2020, 1, 1)),
    ('2020-01-01 10:00:00', 'DATETIME', 'timestamp', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATETIME', 'timestamp with time zone', datetime.datetime(2020, 1, 1, 10, 0, tzinfo=datetime.timezone.utc)),
    ('2020-01-01 10:00:00', 'DATETIME', 'char(30)', 'Wed Jan 01 10:00:00 GMT 2020  '),
    ('2020-01-01 10:00:00', 'DATETIME', 'varchar(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATETIME', 'text', 'Wed Jan 01 10:00:00 GMT 2020'),
    # Zoned DateTime
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'char(25)', '2020-01-01 10:00:00+00   '),
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'varchar(25)', '2020-01-01 10:00:00+00'),
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'text', '2020-01-01 10:00:00+00'),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'timestamp with time zone', datetime.datetime(2020, 1, 1, 10, 0, tzinfo=datetime.timezone.utc)),
    # String
    ('120', 'STRING', 'smallint', 120),
    ('120', 'STRING', 'integer', 120),
    ('120', 'STRING', 'bigint', 120),
    ('120', 'STRING', 'decimal(5,2)', 120.0),
    ('120', 'STRING', 'numeric(5,2)', 120.0),
    ('120', 'STRING', 'real', 120.0),
    ('120', 'STRING', 'double precision', 120.0),
    ('120', 'STRING', 'char(5)', '120  '),
    ('120', 'STRING', 'varchar(5)', '120'),
    ('120', 'STRING', 'text', '120'),
    ("1 years 0 mons 0 days 0 hours 0 mins 0.0 secs", 'STRING', 'interval', datetime.timedelta(days=365)),
    ('1 years 2 mons 10 days 10 hours 0 mins 0.0 secs', 'STRING', 'interval', datetime.timedelta(days=435, hours=10)),
    ('0 years -6 mons 0 days 0 hours 0 mins 0.0 secs', 'STRING', 'interval', datetime.timedelta(days=-180)),
    # It treats 1 year - 1 month as 11 months & then 11months * 30 days = 330 days. (Not like 365days - 30 days)
    ('1 years -1 mons 0 days 6 hours 0 mins 0.0 secs', 'STRING', 'interval', datetime.timedelta(days=330, seconds=21600)),
    ('2003-04-12 04:05:06', 'STRING', 'timestamp', datetime.datetime(2003, 4, 12, 4, 5, 6)),
    ('2020-01-01', 'STRING', 'date', datetime.date(2020, 1, 1)),
    ('10:00:00', 'STRING', 'time', datetime.time(10, 0)),
    ('true', 'STRING', 'boolean', True),
    # Byte array
    ('string', 'BYTE_ARRAY', 'bytea', b'string'),
    # Inet
    ('127.0.0.1/16', 'STRING', 'inet', '127.0.0.1/16'),
    ('127.0/16', 'STRING', 'inet', '127.0.0.0/16'),
    ('127.0.0.0/32', 'STRING', 'inet', '127.0.0.0'),
    ('127.0.0.0', 'STRING', 'inet', '127.0.0.0'),
    ('10.1.2/8', 'STRING', 'inet', '10.1.2.0/8'),
    ('2001:0db8:85a3:0000:0000:8a2e:0370:7334/128', 'STRING', 'inet', '2001:db8:85a3::8a2e:370:7334'),
    ('2001:0db8:85a3:0000:0000:8a2e:0370:7334', 'STRING', 'inet', '2001:db8:85a3::8a2e:370:7334'),
    # Cidr
    ('127.0.0.0/16', 'STRING', 'cidr', '127.0.0.0/16'),
    ('127.0/16', 'STRING', 'cidr', '127.0.0.0/16'),
    ('127.0.0', 'STRING', 'cidr', '127.0.0.0/24'),
    ('127.0', 'STRING', 'cidr', '127.0.0.0/16'),
    ('127', 'STRING', 'cidr', '127.0.0.0/8'),
    ('128', 'STRING', 'cidr', '128.0.0.0/16'),
    ('192.168/24', 'STRING', 'cidr', '192.168.0.0/24'),
    ('192.168.1', 'STRING', 'cidr', '192.168.1.0/24'),
    ('10.1.2', 'STRING', 'cidr', '10.1.2.0/24'),
    ('10', 'STRING', 'cidr', '10.0.0.0/8'),
    ('10.1.2.3/32', 'STRING', 'cidr', '10.1.2.3/32'),
    ('::ffff:1.2.3.0/128', 'STRING', 'cidr', '::ffff:1.2.3.0/128'),
    # Macaddr
    ('08:00:2b:01:02:03', 'STRING', 'macaddr', '08:00:2b:01:02:03'),
    ('08-00-2b-01-02-03', 'STRING', 'macaddr', '08:00:2b:01:02:03'),
    ('08002b:010203', 'STRING', 'macaddr', '08:00:2b:01:02:03'),
    ('08002b-010203', 'STRING', 'macaddr', '08:00:2b:01:02:03'),
    ('0800.2b01.0203', 'STRING', 'macaddr', '08:00:2b:01:02:03'),
    ('0800-2b01-0203', 'STRING', 'macaddr', '08:00:2b:01:02:03'),
    ('08002b010203', 'STRING', 'macaddr', '08:00:2b:01:02:03'),
    # Macaddr8
    ('08:00:2b:01:02:03', 'STRING', 'macaddr8', '08:00:2b:ff:fe:01:02:03'),
    ('08-00-2b-01-02-03', 'STRING', 'macaddr8', '08:00:2b:ff:fe:01:02:03'),
    ('08002b:010203', 'STRING', 'macaddr8', '08:00:2b:ff:fe:01:02:03'),
    ('08002b-010203', 'STRING', 'macaddr8', '08:00:2b:ff:fe:01:02:03'),
    ('0800.2b01.0203', 'STRING', 'macaddr8', '08:00:2b:ff:fe:01:02:03'),
    ('0800-2b01-0203', 'STRING', 'macaddr8', '08:00:2b:ff:fe:01:02:03'),
    ('08:00:2b:01:02:03', 'STRING', 'macaddr8', '08:00:2b:ff:fe:01:02:03'),
    ('08002b010203', 'STRING', 'macaddr8', '08:00:2b:ff:fe:01:02:03'),
    ('08002bff:fe010203', 'STRING', 'macaddr8', '08:00:2b:ff:fe:01:02:03'),
    # JSON
    ("{\"a\":\"b\"}", 'STRING', 'json', {"a": "b"}),
    ('{"a":"b"}', 'STRING', 'json', {"a": "b"}),
    ("{\"jobId1\":{\"id\":\"id1\",\"jobId\":\"jobId1\"},\"jobId2\":{\"id\": \"id2\",\"jobId\":\"jobId2\"}}", 'STRING', 'json', {"jobId1":{"id":"id1","jobId":"jobId1"},"jobId2": {"id":"id2","jobId":"jobId2"}}),
    ('{"jobId1":{"id":"id1","jobId":"jobId1"},"jobId2":{"id": "id2","jobId":"jobId2"}}', 'STRING', 'json', {"jobId1":{"id":"id1","jobId":"jobId1"},"jobId2": {"id":"id2","jobId":"jobId2"}}),
    # JSONB
    ("{\"a\":\"b\"}", 'STRING', 'jsonb', {"a": "b"}),
    ('{"a":"b"}', 'STRING', 'jsonb', {"a": "b"}),
    ("{\"jobId1\":{\"id\":\"id1\",\"jobId\":\"jobId1\"},\"jobId2\":{\"id\": \"id2\",\"jobId\":\"jobId2\"}}", 'STRING', 'jsonb', {"jobId1":{"id":"id1","jobId":"jobId1"},"jobId2": {"id":"id2","jobId":"jobId2"}}),
    ('{"jobId1":{"id":"id1","jobId":"jobId1"},"jobId2":{"id": "id2","jobId":"jobId2"}}', 'STRING', 'jsonb', {"jobId1":{"id":"id1","jobId":"jobId1"},"jobId2": {"id":"id2","jobId":"jobId2"}})
]


@database('postgresql')
@pytest.mark.parametrize('input,converter_type,database_type,expected', DATA_TYPES_POSTGRESQL, ids=[f"{i[1]}-{i[2]}" for i in DATA_TYPES_POSTGRESQL])
def test_data_types_postgresql(sdc_builder, sdc_executor, input, converter_type, database_type, expected, database, keep_data):
    _test_data_types(sdc_builder, sdc_executor, input, converter_type, database_type, expected, database, keep_data)


DATA_TYPES_SQLSERVER = [
    # Boolean
    ('true', 'BOOLEAN', 'char(4)', '1   '),
    ('true', 'BOOLEAN', 'int', 1),
    # Byte
    ('65', 'BYTE', 'char(2)', '65'),
    # Char
    ('a', 'CHAR', 'char(1)', 'a'),
    ('a', 'CHAR', 'varchar(1)', 'a'),
    ('a', 'CHAR', 'nchar(1)', 'a'),
    ('a', 'CHAR', 'nvarchar(1)', 'a'),
    ('a', 'CHAR', 'text', 'a'),
    ('a', 'CHAR', 'ntext', 'a'),
    # Short
    (120, 'SHORT', 'tinyint', 120),
    (120, 'SHORT', 'smallint', 120),
    (120, 'SHORT', 'int', 120),
    (120, 'SHORT', 'bigint', 120),
    (120, 'SHORT', 'decimal(5,2)', 120),
    (120, 'SHORT', 'numeric(5,2)', 120),
    (120, 'SHORT', 'real', 120),
    (120, 'SHORT', 'float', 120),
    (120, 'SHORT', 'money', 120),
    (120, 'SHORT', 'smallmoney', 120),
    (120, 'SHORT', 'char(3)', '120'),
    (120, 'SHORT', 'varchar(3)', '120'),
    (120, 'SHORT', 'nchar(3)', '120'),
    (120, 'SHORT', 'nvarchar(3)', '120'),
    (120, 'SHORT', 'text', '120'),
    (120, 'SHORT', 'ntext', '120'),
    # Integer
    (120, 'INTEGER', 'tinyint', 120),
    (120, 'INTEGER', 'smallint', 120),
    (120, 'INTEGER', 'int', 120),
    (120, 'INTEGER', 'bigint', 120),
    (120, 'INTEGER', 'decimal(5,2)', 120),
    (120, 'INTEGER', 'numeric(5,2)', 120),
    (120, 'INTEGER', 'real', 120),
    (120, 'INTEGER', 'float', 120),
    (120, 'INTEGER', 'money', 120),
    (120, 'INTEGER', 'smallmoney', 120),
    (120, 'INTEGER', 'char(3)', '120'),
    (120, 'INTEGER', 'varchar(3)', '120'),
    (120, 'INTEGER', 'nchar(3)', '120'),
    (120, 'INTEGER', 'nvarchar(3)', '120'),
    (120, 'INTEGER', 'text', '120'),
    (120, 'INTEGER', 'ntext', '120'),
    # Long
    (120, 'LONG', 'tinyint', 120),
    (120, 'LONG', 'smallint', 120),
    (120, 'LONG', 'int', 120),
    (120, 'LONG', 'bigint', 120),
    (120, 'LONG', 'decimal(5,2)', 120),
    (120, 'LONG', 'numeric(5,2)', 120),
    (120, 'LONG', 'real', 120),
    (120, 'LONG', 'float', 120),
    (120, 'LONG', 'money', 120),
    (120, 'LONG', 'smallmoney', 120),
    (120, 'LONG', 'char(3)', '120'),
    (120, 'LONG', 'varchar(3)', '120'),
    (120, 'LONG', 'nchar(3)', '120'),
    (120, 'LONG', 'nvarchar(3)', '120'),
    (120, 'LONG', 'text', '120'),
    (120, 'LONG', 'ntext', '120'),
    # Float
    (120.0, 'FLOAT', 'decimal(5,2)', 120.0),
    (120.0, 'FLOAT', 'numeric(5,2)', 120.0),
    (120.0, 'FLOAT', 'real', 120.0),
    (120.0, 'FLOAT', 'float', 120.0),
    (120.0, 'FLOAT', 'money', 120),
    (120.0, 'FLOAT', 'smallmoney', 120),
    (120.0, 'FLOAT', 'char(5)', '120  '),
    (120.0, 'FLOAT', 'varchar(5)', '120'),
    (120.0, 'FLOAT', 'nchar(5)', '120  '),
    (120.0, 'FLOAT', 'nvarchar(5)', '120'),
    (120.0, 'FLOAT', 'text', '120'),
    (120.0, 'FLOAT', 'ntext', '120'),
    # Double
    (120.0, 'DOUBLE', 'decimal(5,2)', 120.0),
    (120.0, 'DOUBLE', 'numeric(5,2)', 120.0),
    (120.0, 'DOUBLE', 'real', 120.0),
    (120.0, 'DOUBLE', 'float', 120.0),
    (120.0, 'DOUBLE', 'money', 120),
    (120.0, 'DOUBLE', 'smallmoney', 120),
    (120.0, 'DOUBLE', 'char(5)', '120  '),
    (120.0, 'DOUBLE', 'varchar(5)', '120'),
    (120.0, 'DOUBLE', 'nchar(5)', '120  '),
    (120.0, 'DOUBLE', 'nvarchar(5)', '120'),
    (120.0, 'DOUBLE', 'text', '120'),
    (120.0, 'DOUBLE', 'ntext', '120'),
    # Decimal
    (120.0, 'DECIMAL', 'decimal(5,2)', 120.0),
    (120.0, 'DECIMAL', 'numeric(5,2)', 120.0),
    (120.0, 'DECIMAL', 'real', 120.0),
    (120.0, 'DECIMAL', 'float', 120.0),
    (120.0, 'DECIMAL', 'money', 120),
    (120.0, 'DECIMAL', 'smallmoney', 120),
    (120.0, 'DECIMAL', 'char(6)', '120.00'),
    (120.0, 'DECIMAL', 'varchar(6)', '120.00'),
    (120.0, 'DECIMAL', 'nchar(6)', '120.00'),
    (120.0, 'DECIMAL', 'nvarchar(6)', '120.00'),
    (120.0, 'DECIMAL', 'text', '120.00'),
    (120.0, 'DECIMAL', 'ntext', '120.00'),
    # Date
    ('2020-01-01 10:00:00', 'DATE', 'date', datetime.date(2020, 1, 1)),
    ('2020-01-01 10:00:00', 'DATE', 'datetime', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATE', 'datetime2', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATE', 'smalldatetime', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATE', 'char(30)', 'Wed Jan 01 10:00:00 GMT 2020  '),
    ('2020-01-01 10:00:00', 'DATE', 'varchar(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATE', 'nchar(30)', 'Wed Jan 01 10:00:00 GMT 2020  '),
    ('2020-01-01 10:00:00', 'DATE', 'nvarchar(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATE', 'text', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATE', 'ntext', 'Wed Jan 01 10:00:00 GMT 2020'),
    # Time
    ('2020-01-01 10:00:00', 'TIME', 'time', datetime.time(10, 0)),
    ('2020-01-01 10:00:00', 'TIME', 'char(30)', 'Wed Jan 01 10:00:00 GMT 2020  '),
    ('2020-01-01 10:00:00', 'TIME', 'varchar(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'TIME', 'nchar(30)', 'Wed Jan 01 10:00:00 GMT 2020  '),
    ('2020-01-01 10:00:00', 'TIME', 'nvarchar(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'TIME', 'text', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'TIME', 'ntext', 'Wed Jan 01 10:00:00 GMT 2020'),
    # DateTime
    ('2020-01-01 10:00:00', 'DATETIME', 'date', datetime.date(2020, 1, 1)),
    ('2020-01-01 10:00:00', 'DATETIME', 'datetime', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATETIME', 'datetime2', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATETIME', 'smalldatetime', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATETIME', 'char(30)', 'Wed Jan 01 10:00:00 GMT 2020  '),
    ('2020-01-01 10:00:00', 'DATETIME', 'varchar(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATETIME', 'nchar(30)', 'Wed Jan 01 10:00:00 GMT 2020  '),
    ('2020-01-01 10:00:00', 'DATETIME', 'nvarchar(50)', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATETIME', 'text', 'Wed Jan 01 10:00:00 GMT 2020'),
    ('2020-01-01 10:00:00', 'DATETIME', 'ntext', 'Wed Jan 01 10:00:00 GMT 2020'),
    # Zoned DateTime
# Currently not supported at all
    # String
    ('120', 'STRING', 'tinyint', 120),
    ('120', 'STRING', 'smallint', 120),
    ('120', 'STRING', 'integer', 120),
    ('120', 'STRING', 'bigint', 120),
    ('120', 'STRING', 'decimal(5,2)', 120.0),
    ('120', 'STRING', 'numeric(5,2)', 120.0),
    ('120', 'STRING', 'real', 120.0),
    ('120', 'STRING', 'float', 120.0),
    ('120', 'STRING', 'money', 120.0),
    ('120', 'STRING', 'smallmoney', 120.0),
    ('120', 'STRING', 'char(5)', '120  '),
    ('120', 'STRING', 'varchar(5)', '120'),
    ('120', 'STRING', 'nchar(5)', '120  '),
    ('120', 'STRING', 'nvarchar(5)', '120'),
    ('120', 'STRING', 'text', '120'),
    ('120', 'STRING', 'ntext', '120'),
    ('2003-04-12 04:05:06', 'STRING', 'datetime', datetime.datetime(2003, 4, 12, 4, 5, 6)),
    ('2003-04-12 04:05:06', 'STRING', 'datetime2', datetime.datetime(2003, 4, 12, 4, 5, 6)),
    ('2003-04-12 04:05:06', 'STRING', 'smalldatetime', datetime.datetime(2003, 4, 12, 4, 5)),
    ('2020-01-01', 'STRING', 'date', datetime.date(2020, 1, 1)),
    ('10:00:00', 'STRING', 'time', datetime.time(10, 0)),
    # Byte array
    ('string', 'BYTE_ARRAY', 'binary(6)', b'string'),
    ('string', 'BYTE_ARRAY', 'varbinary(6)', b'string'),
    ('string', 'BYTE_ARRAY', 'char(6)', 'string'),
    ('string', 'BYTE_ARRAY', 'varchar(6)', 'string'),
    ('string', 'BYTE_ARRAY', 'text', 'string'),
]
@database('sqlserver')
@pytest.mark.parametrize('input,converter_type,database_type,expected', DATA_TYPES_SQLSERVER, ids=[f"{i[1]}-{i[2]}" for i in DATA_TYPES_SQLSERVER])
def test_data_types_sqlserver(sdc_builder, sdc_executor, input, converter_type, database_type, expected, database, keep_data):
    _test_data_types(sdc_builder, sdc_executor, input, converter_type, database_type, expected, database, keep_data)

def _test_data_types(sdc_builder, sdc_executor, input, converter_type, database_type, expected, database, keep_data):

    if database_type == 'macaddr8' and compare_database_server_version(database.database_server_version, version_2_major=10,
                                                                       version_2_minor=0, version_2_patch=0) < 0:
        _version = database.database_server_version
        pytest.skip(f"PostgreSQL Database Version ({_version.major}.{_version.minor}.{_version.patch}) "
                    f"doesn't support macaddr8 datatype.")

    table_name = get_random_string(string.ascii_lowercase, 20)
    connection = database.engine.connect()
    if isinstance(database, MySqlDatabase) or isinstance(database, MariaDBDatabase):
        connection.execute("SET sql_mode=ANSI_QUOTES")

    # Build pipeline
    builder = sdc_builder.get_pipeline_builder()
    origin = builder.add_stage('Dev Raw Data Source')
    origin.data_format = 'JSON'
    origin.stop_after_first_batch = True
    origin.raw_data = json.dumps({"value": input })

    converter = builder.add_stage('Field Type Converter')
    converter.conversion_method = 'BY_FIELD'
    converter.field_type_converter_configs = [{
        'fields': ['/value'],
        'targetType': converter_type,
        'dataLocale': 'en,US',
        'dateFormat': 'YYYY_MM_DD_HH_MM_SS',
        'zonedDateTimeFormat': 'ISO_OFFSET_DATE_TIME',
        'scale': 2
    }]

    target = builder.add_stage('JDBC Producer')
    target.table_name = table_name
    target.enclose_object_names = True
    target.field_to_column_mapping = []
    target.on_record_error = 'STOP_PIPELINE'

    origin >> converter >> target

    pipeline = builder.build().configure_for_environment(database)
    # Workarounds for STE,STF specific stuff
    target.table_name = table_name
    if isinstance(database, MySqlDatabase) or isinstance(database, MariaDBDatabase):
        target.init_query = "SET sql_mode=ANSI_QUOTES"

    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table
        connection.execute(f"""
            CREATE TABLE "{table_name}"(
                "value" {database_type} NULL
            )
        """)

        # Run pipeline and read from Elasticsearch to assert
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        if database_type == 'xmltype':
            rs = connection.execute(f'select XMLTYPE.GETCLOBVAL("value") from "{table_name}"')
        else:
            rs = connection.execute(f'select "value" from "{table_name}"')

        rows = [row for row in rs]
        assert len(rows) == 1
        actual = rows[0][0]

        # Coercions that can't be directly asserted
        if type(actual) == memoryview:
            actual = actual.tobytes()

        assert actual == expected
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            connection.execute(f'DROP TABLE "{table_name}"')


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
@pytest.mark.parametrize('test_name,table_name,column_name', OBJECT_NAMES_ORACLE, ids=[i[0] for i in OBJECT_NAMES_ORACLE])
def test_object_names_oracle(sdc_builder, sdc_executor, database, test_name, table_name, column_name, keep_data):
    _test_object_names(sdc_builder, sdc_executor, database, table_name, column_name, False, keep_data)


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
@pytest.mark.parametrize('use_multi_row_operation', [True, False])
@pytest.mark.parametrize('test_name,table_name,column_name', OBJECT_NAMES_POSTGRESQL, ids=[i[0] for i in OBJECT_NAMES_POSTGRESQL])
def test_object_names_postgresql(sdc_builder, sdc_executor, database, test_name, table_name, column_name, use_multi_row_operation, keep_data):
    _test_object_names(sdc_builder, sdc_executor, database, table_name, column_name, use_multi_row_operation, keep_data)


# Rules: https://mariadb.com/kb/en/identifier-names/
OBJECT_NAMES_MARIADB = [
    ('keywords', 'table', 'column'),
    ('lowercase', get_random_string(string.ascii_lowercase, 20), get_random_string(string.ascii_lowercase, 20)),
    ('uppercase', get_random_string(string.ascii_uppercase, 20), get_random_string(string.ascii_uppercase, 20)),
    ('mixedcase', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 20)),
    ('max_table_name', get_random_string(string.ascii_letters, 64), get_random_string(string.ascii_letters, 20)),
    ('max_column_name', get_random_string(string.ascii_letters, 20), get_random_string(string.ascii_letters, 64)),
    ('numbers', get_random_string(string.ascii_letters, 5) + "0123456789", get_random_string(string.ascii_letters, 5) + "0123456789"),
    ('special', get_random_string(string.ascii_letters, 5) + "$_", get_random_string(string.ascii_letters, 5) + "$_"),
]
@database('mariadb')
@pytest.mark.parametrize('use_multi_row_operation', [True, False])
@pytest.mark.parametrize('test_name,table_name,column_name', OBJECT_NAMES_MARIADB, ids=[i[0] for i in OBJECT_NAMES_MARIADB])
def test_object_names_mariadb(sdc_builder, sdc_executor, database, test_name, table_name, column_name, use_multi_row_operation, keep_data):
    _test_object_names(sdc_builder, sdc_executor, database, table_name, column_name, use_multi_row_operation, keep_data)


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
@pytest.mark.parametrize('use_multi_row_operation', [True, False])
@pytest.mark.parametrize('test_name,table_name,column_name', OBJECT_NAMES_MYSQL, ids=[i[0] for i in OBJECT_NAMES_MYSQL])
def test_object_names_mysql(sdc_builder, sdc_executor, database, test_name, table_name, column_name, use_multi_row_operation, keep_data):
    if isinstance(database, MemSqlDatabase):
        pytest.skip("Standard Tests are currently only written for MySQL and not for MemSQL (sadly STF threads both DBs the same way)")
    _test_object_names(sdc_builder, sdc_executor, database, table_name, column_name, use_multi_row_operation, keep_data)


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
@pytest.mark.parametrize('use_multi_row_operation', [True, False])
@pytest.mark.parametrize('test_name,table_name,column_name', OBJECT_NAMES_SQLSERVER, ids=[i[0] for i in OBJECT_NAMES_SQLSERVER])
def test_object_names_sqlserver(sdc_builder, sdc_executor, database, test_name, table_name, column_name, use_multi_row_operation, keep_data):
    _test_object_names(sdc_builder, sdc_executor, database, table_name, column_name, use_multi_row_operation, keep_data)


def _test_object_names(sdc_builder, sdc_executor, database, table_name, column_name, use_multi_row_operation, keep_data):
    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.data_format = 'JSON'
    source.raw_data = f'{{ "{column_name}" : 1 }}'
    source.stop_after_first_batch = True

    target = builder.add_stage('JDBC Producer')
    target.table_name = table_name
    target.enclose_object_names = True
    target.field_to_column_mapping = []
    target.use_multi_row_operation = use_multi_row_operation
    target.on_record_error = 'STOP_PIPELINE'

    source >> target
    pipeline = builder.build().configure_for_environment(database)
    # Work-arounding STF behavior of upper-casing table name configuration
    target.table_name = table_name
    # Our environment is running default MySQL instance that doesn't set SQL_ANSI_MODE that we're expecting
    if isinstance(database, MySqlDatabase) or isinstance(database, MariaDBDatabase):
        target.init_query = "SET sql_mode=ANSI_QUOTES"

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column(column_name, sqlalchemy.Integer, primary_key=False, quote=True),
        quote=True
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify that the data were indeed inserted
        result = database.engine.execute(table.select())
        rows = result.fetchall()
        result.close()

        assert len(rows) == 1
        assert rows[0][0] == 1
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)


@database
@pytest.mark.parametrize('use_multi_row_operation', [True, False])
def test_multiple_batches(sdc_builder, sdc_executor, use_multi_row_operation, database, keep_data):
    if isinstance(database, MemSqlDatabase):
        pytest.skip("Standard Tests are currently only written for MySQL and not for MemSQL (sadly STF threads both DBs the same way)")
    if isinstance(database, OracleDatabase) and use_multi_row_operation:
        pytest.skip("Oracle doesn't support multi-row statements.")

    table_name = get_random_string(string.ascii_lowercase, 20)
    batch_size = 100
    batches = 10

    builder = sdc_builder.get_pipeline_builder()

    origin = builder.add_stage('Dev Data Generator')
    origin.batch_size = batch_size
    origin.fields_to_generate = [{
        "type": "LONG_SEQUENCE",
        "field": "seq"
    }]

    target = builder.add_stage('JDBC Producer')
    target.table_name = table_name
    target.enclose_object_names = True
    target.field_to_column_mapping = []
    target.use_multi_row_operation = use_multi_row_operation
    target.on_record_error = 'STOP_PIPELINE'

    origin >> target
    pipeline = builder.build().configure_for_environment(database)

    # Work-arounding STF behavior of upper-casing table name configuration
    target.table_name = table_name
    # Our environment is running default MySQL instance that doesn't set SQL_ANSI_MODE that we're expecting
    if isinstance(database, MySqlDatabase) or isinstance(database, MariaDBDatabase):
        target.init_query = "SET sql_mode=ANSI_QUOTES"

    sdc_executor.add_pipeline(pipeline)

    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column('seq', sqlalchemy.Integer, primary_key=False, quote=True),
        quote=True
    )
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        table.create(database.engine)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(batches * batch_size)
        sdc_executor.stop_pipeline(pipeline)

        # Now the pipeline will write some amount of records that will be larger, so we get precise count from metrics
        history = sdc_executor.get_pipeline_history(pipeline)
        records = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        logger.info(f"Detected {records} output records")
        # Sanity check
        assert records >= batch_size * batches

        result = database.engine.execute(table.select())
        data = sorted([row[0] for row in result.fetchall()])
        result.close()

        assert data == [i for i in range(0, records)]
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            table.drop(database.engine)


@database
def test_dataflow_events(sdc_builder, sdc_executor, database):
    pytest.skip("No events supported in JDBC Producer destination at this time.")


@database
def test_data_format(sdc_builder, sdc_executor, database, keep_data):
    pytest.skip("JDBC Producer doesn't deal with data formats")


@database
def test_push_pull(sdc_builder, sdc_executor, database):
    pytest.skip("We haven't re-implemented this test since Dev Data Generator (push) is art of test_multiple_batches and Dev Raw Data Source (pull) is part of test_data_types.")
