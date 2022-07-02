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
from streamsets.testframework.environments.databases import MySqlDatabase, MariaDBDatabase, MemSqlDatabase
from streamsets.testframework.markers import database, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


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
    # Zoned DateTime
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'char(50)', '2020-01-01T10:00Z'),
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'varchar(50)', '2020-01-01T10:00Z'),
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'binary(20)', b'2020-01-01T10:00Z\x00\x00\x00'),
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'varbinary(50)', b'2020-01-01T10:00Z'),
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'text', '2020-01-01T10:00Z'),
    ('2020-01-01T10:00:00+00:00', 'ZONED_DATETIME', 'blob', b'2020-01-01T10:00Z'),
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
        pytest.skip("Standard Tests are currently only written for MySQL and not for MemSQL (sadly STF threads both DBs the same way)")

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
    ('2003-04-12 04:05:06', 'STRING', 'timestamp', datetime.datetime(2003, 4, 12, 4, 5, 6)),
    ('2020-01-01', 'STRING', 'date', datetime.date(2020, 1, 1)),
    ('10:00:00', 'STRING', 'time', datetime.time(10, 0)),
    ('true', 'STRING', 'boolean', True),
    ('{"a": "b"}', 'STRING', 'json', {'a': 'b'}),
    ('{"a": "b"}', 'STRING', 'jsonb', {'a': 'b'}),
    # Byte array
    ('string', 'BYTE_ARRAY', 'bytea', b'string'),
]
@database('postgresql')
@pytest.mark.parametrize('input,converter_type,database_type,expected', DATA_TYPES_POSTGRESQL, ids=[f"{i[1]}-{i[2]}" for i in DATA_TYPES_POSTGRESQL])
def test_data_types_postgresql(sdc_builder, sdc_executor, input, converter_type, database_type, expected, database, keep_data):
    _test_data_types(sdc_builder, sdc_executor, input, converter_type, database_type, expected, database, keep_data)


def _test_data_types(sdc_builder, sdc_executor, input, converter_type, database_type, expected, database, keep_data):
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

    tee = builder.add_stage('JDBC Tee')
    tee.table_name = table_name
    tee.default_operation = 'INSERT'
    tee.field_to_column_mapping = []
    tee.on_record_error = 'STOP_PIPELINE'
    tee.generated_column_mappings = [{
        'dataType': 'USE_COLUMN_TYPE',
        'columnName': 'id',
        'field': '/id'
      }]

    wiretap = builder.add_wiretap()

    origin >> converter >> tee >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)
    # Workarounds for STE,STF specific stuff
    if isinstance(database, MySqlDatabase) or isinstance(database, MariaDBDatabase):
        tee.init_query = "SET sql_mode=ANSI_QUOTES"

    sdc_executor.add_pipeline(pipeline)

    try:
        # Create table
        if isinstance(database, MySqlDatabase) or isinstance(database, MariaDBDatabase):
            connection.execute(f"""
                CREATE TABLE "{table_name}"(
                   "id" int primary key auto_increment, 
                    "value" {database_type} NULL
                )
            """)
        else: # Assuming PostgreSQL (no other DB is supported)
            connection.execute(f"""
                CREATE TABLE "{table_name}"(
                   "id" serial, 
                    "value" {database_type} NULL
                )
            """)

        # Run pipeline and read from Elasticsearch to assert
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify returned records
        records = wiretap.output_records
        assert len(records) == 1
        assert records[0].field['id'] == 1

        rs = connection.execute(f'select "id", "value" from "{table_name}"')
        rows = [row for row in rs]
        assert len(rows) == 1

        # Generated key is "1"
        assert rows[0][0] == 1

        # And assert actual value - few corrections for "problematical" types
        actual = rows[0][1]
        if type(actual) == memoryview:
            actual = actual.tobytes()
        assert actual == expected
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database ...', table_name, database.type)
            connection.execute(f'DROP TABLE "{table_name}"')


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


def _test_object_names(sdc_builder, sdc_executor, database, table_name, column_name, use_multi_row_operation, keep_data):
    connection = database.engine.connect()
    if isinstance(database, MySqlDatabase) or isinstance(database, MariaDBDatabase):
        connection.execute("SET sql_mode=ANSI_QUOTES")

    builder = sdc_builder.get_pipeline_builder()

    source = builder.add_stage('Dev Raw Data Source')
    source.data_format = 'JSON'
    source.raw_data = f'{{ "{column_name}" : 666 }}'
    source.stop_after_first_batch = True

    tee = builder.add_stage('JDBC Tee')
    tee.table_name = table_name
    tee.default_operation = 'INSERT'
    tee.enclose_table_name = True
    tee.field_to_column_mapping = []
    tee.on_record_error = 'STOP_PIPELINE'
    tee.use_multi_row_operation = use_multi_row_operation
    tee.generated_column_mappings = [{
        'dataType': 'USE_COLUMN_TYPE',
        'columnName': 'id',
        'field': '/id'
      }]

    wiretap = builder.add_wiretap()

    source >> tee >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)
    # Work-arounding STF behavior of upper-casing table name configuration
    tee.table_name = table_name

    # Our environment is running default MySQL instance that doesn't set SQL_ANSI_MODE that we're expecting
    if isinstance(database, MySqlDatabase) or isinstance(database, MariaDBDatabase):
        tee.init_query = "SET sql_mode=ANSI_QUOTES"

    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        if isinstance(database, MySqlDatabase) or isinstance(database, MariaDBDatabase):
            connection.execute(f"""
                CREATE TABLE "{table_name}"(
                   "id" int primary key auto_increment, 
                    "{column_name}" int NULL
                )
            """)
        else: # Assuming PostgreSQL (no other DB is supported
            connection.execute(f"""
                CREATE TABLE "{table_name}"(
                   "id" serial, 
                    "{column_name}" int NULL
                )
            """)

        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Verify that the data were indeed inserted
        result = connection.execute(f'select "id", "{column_name}" from "{table_name}"')
        rows = result.fetchall()
        result.close()

        assert len(rows) == 1
        assert rows[0][0] == 1
        assert rows[0][1] == 666

        # Verify returned records
        records = wiretap.output_records
        assert len(records) == 1
        assert records[0].field['id'] == 1
        assert records[0].field[column_name] == 666
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            connection.execute(f'DROP TABLE "{table_name}"')


@database('mysql', 'postgresql')
@pytest.mark.parametrize('use_multi_row_operation', [True, False])
def test_multiple_batches(sdc_builder, sdc_executor, use_multi_row_operation, database, keep_data):
    if isinstance(database, MemSqlDatabase):
        pytest.skip("Standard Tests are currently only written for MySQL and not for MemSQL (sadly STF threads both DBs the same way)")
    connection = database.engine.connect()
    if isinstance(database, MySqlDatabase) or isinstance(database, MariaDBDatabase):
        connection.execute("SET sql_mode=ANSI_QUOTES")

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

    tee = builder.add_stage('JDBC Tee')
    tee.table_name = table_name
    tee.default_operation = 'INSERT'
    tee.field_to_column_mapping = []
    tee.on_record_error = 'STOP_PIPELINE'
    tee.use_multi_row_operation = use_multi_row_operation
    tee.generated_column_mappings = [{
        'dataType': 'USE_COLUMN_TYPE',
        'columnName': 'id',
        'field': '/id'
      }]

    wiretap = builder.add_wiretap()

    origin >> tee >> wiretap.destination

    pipeline = builder.build().configure_for_environment(database)

    # Work-arounding STF behavior of upper-casing table name configuration
    tee.table_name = table_name
    # Our environment is running default MySQL instance that doesn't set SQL_ANSI_MODE that we're expecting
    if isinstance(database, MySqlDatabase) or isinstance(database, MariaDBDatabase):
        tee.init_query = "SET sql_mode=ANSI_QUOTES"

    sdc_executor.add_pipeline(pipeline)
    try:
        logger.info('Creating table %s in %s database ...', table_name, database.type)
        if isinstance(database, MySqlDatabase) or isinstance(database, MariaDBDatabase):
            connection.execute(f"""
                CREATE TABLE "{table_name}"(
                   "id" int primary key auto_increment, 
                    "seq" int NULL
                )
            """)
        else: # Assuming PostgreSQL (no other DB is supported
            connection.execute(f"""
                CREATE TABLE "{table_name}"(
                   "id" serial, 
                    "seq" int NULL
                )
            """)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(batches * batch_size)
        sdc_executor.stop_pipeline(pipeline)

        # Now the pipeline will write some amount of records that will be larger, so we get precise count from metrics
        history = sdc_executor.get_pipeline_history(pipeline)
        record_count = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        logger.info(f"Detected {record_count} output records")
        # Sanity check
        assert record_count >= batch_size * batches

        # Assert database side
        result = connection.execute(f'select "id", "seq" from "{table_name}"')
        data = sorted([(row[0], row[1]) for row in result.fetchall()])
        result.close()
        assert data == [(i+1, i) for i in range(0, record_count)]

        records = wiretap.output_records
        assert len(records) == record_count
        # Verify each record
        def sortFunc(r):
            return r.field['id'].value
        records.sort(key=sortFunc)

        assert [(r.field['id'], r.field['seq']) for r in records] == [(i+1, i) for i in range(0, record_count)]
    finally:
        if not keep_data:
            logger.info('Dropping table %s in %s database...', table_name, database.type)
            connection.execute(f'DROP TABLE "{table_name}"')


@database
def test_dataflow_events(sdc_builder, sdc_executor, database):
    pytest.skip("No events supported in JDBC Tee processor this time.")


@database
def test_data_format(sdc_builder, sdc_executor, database, keep_data):
    pytest.skip("JDBC Tee Processor doesn't deal with data formats")


@database
def test_push_pull(sdc_builder, sdc_executor, database):
    pytest.skip("We haven't re-implemented this test since Dev Data Generator (push) is art of test_multiple_batches and Dev Raw Data Source (pull) is part of test_data_types.")
