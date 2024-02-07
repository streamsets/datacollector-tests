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
import string
import datetime

import pytest
from abc import ABC, abstractmethod

from typing import Callable

from sqlalchemy.sql.expression import asc
from sqlalchemy import (
    Table,
    MetaData,
    Column,
    String,
    Integer
)
from streamsets.testframework.environments.teradata import TeradataInstance
from streamsets.testframework.markers import teradata, sdc_min_version
from streamsets.testframework.utils import get_random_string

from stage.utils.data_loading_stages import DataType

logger = logging.getLogger(__name__)

pytestmark = [teradata, sdc_min_version('5.9.0')]

STAGE_NAME = 'com_streamsets_pipeline_stage_teradata_destination_TeradataDTarget'
EXTERNAL_STAGING_LOCATIONS = ['AWS_S3', 'ADLS_GEN2', 'BLOB_STORAGE', 'GCS']
DEFAULT_EXTERNAL_STAGING_LOCATION = ['ADLS_GEN2']
LOCAL_STAGING_LOCATION = ['LOCAL']
ALL_STAGING_LOCATIONS = LOCAL_STAGING_LOCATION + EXTERNAL_STAGING_LOCATIONS
ALL_STAGING_FILE_FORMATS = ['CSV', 'PARQUET']

CREATE_TABLE_DDL_TEMPLATE = 'CREATE MULTISET TABLE %s.%s ( %s )'

CSV_EXTERNAL_SUPPORTED_DATA_TYPES = [
    'BYTE', 'VARBYTE(25500)', 'BLOB(16776192)',
    'CHARACTER', 'VARCHAR(255)', 'CLOB(16776192)',
    'DECIMAL(5)', 'BIGINT', 'FLOAT', 'INTEGER', 'NUMBER', 'BYTEINT', 'SMALLINT',
    'TIME', 'DATE', 'TIMESTAMP', 'TIMESTAMP WITH TIME ZONE', 'TIME WITH TIME ZONE',
    'INTERVAL SECOND', 'INTERVAL MINUTE TO SECOND', 'INTERVAL MINUTE', 'INTERVAL HOUR',
    'INTERVAL HOUR TO MINUTE', 'INTERVAL HOUR TO SECOND', 'INTERVAL DAY', 'INTERVAL DAY TO HOUR',
    'INTERVAL DAY TO MINUTE', 'INTERVAL DAY TO SECOND', 'INTERVAL MONTH', 'INTERVAL YEAR TO MONTH', 'INTERVAL YEAR',
    'BOOLEAN', 'DOUBLE'
]

PARQUET_EXTERNAL_SUPPORTED_DATA_TYPES = [
    'VARBYTE(25500)', 'BLOB(16776192)',
    'VARCHAR(255)', 'CLOB(16776192)',
    'DECIMAL(5)', 'BIGINT',
    # 'FLOAT', while FLOAT works, it creates imprecision in the value, and we should not support that, better use DOUBLE
    'INTEGER',
    'TIME', 'DATE', 'TIMESTAMP', 'TIME WITH TIME ZONE',
    'DOUBLE'
]

FASTLOAD_SUPPORTED_DATA_TYPES = [
    'VARCHAR(255)', 'CHARACTER',
    'DECIMAL(5)', 'BIGINT', 'FLOAT', 'INTEGER', 'NUMBER', 'BYTEINT', 'SMALLINT',
    'TIME', 'DATE', 'TIMESTAMP', 'TIMESTAMP WITH TIME ZONE', 'TIME WITH TIME ZONE',
    'INTERVAL SECOND', 'INTERVAL MINUTE TO SECOND', 'INTERVAL MINUTE', 'INTERVAL HOUR',
    'INTERVAL HOUR TO MINUTE', 'INTERVAL HOUR TO SECOND', 'INTERVAL DAY', 'INTERVAL DAY TO HOUR',
    'INTERVAL DAY TO MINUTE', 'INTERVAL DAY TO SECOND', 'INTERVAL MONTH', 'INTERVAL YEAR TO MONTH', 'INTERVAL YEAR',
    'BOOLEAN', 'DOUBLE'
]

AUTO_CREATION_SUPPORTED_DATA_TYPES = [
    # It's not like other SDC types do not support auto creation, but we are never creating them,
    # and they would just be converted to one of these
    'BYTE', 'VARBYTE(25500)',
    'VARCHAR(255)',
    'DECIMAL(5)', 'BIGINT', 'FLOAT', 'INTEGER', 'SMALLINT',
    'TIME', 'DATE', 'TIMESTAMP', 'TIMESTAMP WITH TIME ZONE',
    'BOOLEAN', 'DOUBLE'
]

CSV_DATA_TYPES = {
    # I didn't find why the binary data behaves like this, so just copied the output
    'BYTE': DataType('BYTE', 'BYTE', '1', b'\x10'),
    'VARBYTE(25500)': DataType('BYTE_ARRAY', 'VARBYTE(25500)', 'byte_data', b'\x00\x00\x00\x90\x00\x00'),
    'BLOB(16776192)': DataType('BYTE_ARRAY', 'BLOB(16776192)', 'byte_data', b'\x00\x00\x00\x90\x00\x00'),

    'CHARACTER': DataType('CHAR', 'CHARACTER', 'J', 'J '),
    'VARCHAR(255)': DataType('STRING', 'VARCHAR(255)', 'Joaquin', 'Joaquin'),
    'CLOB(16776192)': DataType('STRING', 'CLOB(16776192)', 'Joaquin', 'Joaquin'),

    'DECIMAL(5)': DataType('DECIMAL', 'DECIMAL(5)', 42069, 42069),
    'BIGINT': DataType('LONG', 'BIGINT', 42069, 42069),
    'FLOAT': DataType('FLOAT', 'FLOAT', 420.69, 420.69),
    'INTEGER': DataType('INTEGER', 'INTEGER', 42069, 42069),
    'NUMBER': DataType('INTEGER', 'NUMBER', 42069, 42069),
    'BYTEINT': DataType('INTEGER', 'BYTEINT', 5, 5),
    'SMALLINT': DataType('SHORT', 'SMALLINT', 7, 7),

    'TIME': DataType('TIME', 'TIME', '04:20:59', datetime.time(4, 20, 59)),
    'DATE': DataType('DATE', 'DATE', '2024-02-20', datetime.date(2024, 2, 20)),
    'TIMESTAMP': DataType('DATETIME', 'TIMESTAMP', '2024-02-20 04:20:59', datetime.datetime(2024, 2, 20, 4, 20, 59)),
    'TIMESTAMP WITH TIME ZONE': DataType('ZONED_DATETIME', 'TIMESTAMP WITH TIME ZONE', '2024-02-20T04:20:59+00:00',
                                         datetime.datetime(2024, 2, 20, 4, 20, 59,
                                                           tzinfo=datetime.timezone(
                                                               datetime.timedelta(days=-1, seconds=68400)))),
    'TIME WITH TIME ZONE': DataType('TIME', 'TIME WITH TIME ZONE', '04:20:59',
                                    datetime.time(4, 20, 59, tzinfo=datetime.timezone(
                                        datetime.timedelta(days=-1, seconds=68400)))),

    'INTERVAL SECOND': DataType('STRING', 'INTERVAL SECOND', '4', '  4.000000'),
    'INTERVAL MINUTE TO SECOND': DataType('STRING', 'INTERVAL MINUTE TO SECOND', '5:40', '  5:40.000000'),
    'INTERVAL MINUTE': DataType('STRING', 'INTERVAL MINUTE', '6', '  6'),
    'INTERVAL HOUR': DataType('STRING', 'INTERVAL HOUR', '2', '  2'),
    'INTERVAL HOUR TO MINUTE': DataType('STRING', 'INTERVAL HOUR TO MINUTE', '2:34', '  2:34'),
    'INTERVAL HOUR TO SECOND': DataType('STRING', 'INTERVAL HOUR TO SECOND', '2:34:45', '  2:34:45.000000'),
    'INTERVAL DAY': DataType('STRING', 'INTERVAL DAY', '9', '  9'),
    'INTERVAL DAY TO HOUR': DataType('STRING', 'INTERVAL DAY TO HOUR', '2 34', '  3 10'),
    'INTERVAL DAY TO MINUTE': DataType('STRING', 'INTERVAL DAY TO MINUTE', '-2 15:45', ' -2 15:45'),
    'INTERVAL DAY TO SECOND': DataType('STRING', 'INTERVAL DAY TO SECOND', '2 13:46:11', '  2 13:46:11.000000'),
    'INTERVAL MONTH': DataType('STRING', 'INTERVAL MONTH', '9', '  9'),
    'INTERVAL YEAR TO MONTH': DataType('STRING', 'INTERVAL YEAR TO MONTH', '9-11', '  9-11'),
    'INTERVAL YEAR': DataType('STRING', 'INTERVAL YEAR', '8', '  8'),

    'BOOLEAN': DataType('BOOLEAN', 'VARCHAR(255)', True, 'true'),
    'DOUBLE': DataType('DOUBLE', 'FLOAT', 420.69, 420.69),
    # 'VARCHAR(255)': DataType('VARCHAR(255)', 'VARCHAR(255)', 'FILE_REF', 'file_ref', 'file_ref'), # not sure how to create FILE_REF
}

PARQUET_DATA_TYPES = {
    'VARBYTE(25500)': DataType('BYTE_ARRAY', 'VARBYTE(25500)', 'byte_data', b'byte_data'),
    'BLOB(16776192)': DataType('BYTE_ARRAY', 'BLOB(16776192)', 'byte_data', b'byte_data'),

    'VARCHAR(255)': DataType('STRING', 'VARCHAR(255)', 'Joaquin', 'Joaquin'),
    'CLOB(16776192)': DataType('STRING', 'CLOB(16776192)', 'Joaquin', 'Joaquin'),

    'DECIMAL(5)': DataType('DECIMAL', 'DECIMAL(5)', 42069, 42069),
    'BIGINT': DataType('LONG', 'BIGINT', 42069, 42069),
    # using non-decimal in float to avoid imprecision that can happen by doing so
    'FLOAT': DataType('FLOAT', 'FLOAT', 42069, 42069),
    'INTEGER': DataType('INTEGER', 'INTEGER', 42069, 42069),

    # offset in parquet times due to timezone, note that TIME WITH TIME ZONE won't change
    'TIME': DataType('TIME', 'TIME', '04:20:59', datetime.time(23, 20, 59)),
    'DATE': DataType('DATE', 'DATE', '2024-02-20', datetime.date(2024, 2, 20)),
    'TIMESTAMP': DataType('DATETIME', 'TIMESTAMP', '2024-02-20 04:20:59', datetime.datetime(2024, 2, 19, 23, 20, 59)),
    'TIME WITH TIME ZONE': DataType('TIME', 'TIME WITH TIME ZONE', '04:20:59',
                                    datetime.time(4, 20, 59, tzinfo=datetime.timezone.utc)),

    'DOUBLE': DataType('DOUBLE', 'FLOAT', 420.69, 420.69),
}

CSV_EXTERNAL_DATA_TYPES = [CSV_DATA_TYPES[key] for key in CSV_EXTERNAL_SUPPORTED_DATA_TYPES if key in CSV_DATA_TYPES]
FASTLOAD_DATA_TYPES = [CSV_DATA_TYPES[key] for key in FASTLOAD_SUPPORTED_DATA_TYPES if key in CSV_DATA_TYPES]
AUTO_CREATED_DATA_TYPES = [CSV_DATA_TYPES[key] for key in AUTO_CREATION_SUPPORTED_DATA_TYPES if key in CSV_DATA_TYPES]
PARQUET_EXTERNAL_DATA_TYPES = [PARQUET_DATA_TYPES[key] for key in PARQUET_EXTERNAL_SUPPORTED_DATA_TYPES]

TROUBLESOME_TABLE_NAMES = [
    # https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/International-Character-Set-Support/Managing-International-Language-Support/Object-Names/Rules-for-Object-Naming
    ('random', f'STF_{get_random_string(string.ascii_uppercase, 10)}'),
    ('table', 'TABLE'),
    ('select', 'SELECT'),
    ('from', 'FROM'),
    ('table*', 'TABLE*'),
    ('ta$ble', 'TA$BLE'),
    ('ta+-/ble', 'TA+-/BLE'),
    ('ta ble', 'TA BLE'),
    ('max_size', get_random_string(string.ascii_uppercase, 128)),
    ('plus', get_random_string(string.ascii_uppercase, 5) + '+' + get_random_string(string.ascii_uppercase, 5)),
    ('underscore', get_random_string(string.ascii_uppercase, 5) + '_' + get_random_string(string.ascii_uppercase, 5)),
    ('comma', get_random_string(string.ascii_uppercase, 5) + ',' + get_random_string(string.ascii_uppercase, 5)),
    ('short', 'A'),
]

TROUBLESOME_COLUMN_NAMES = [
    ('random', f'STF_{get_random_string(string.ascii_uppercase, 10)}'),
    ('table', 'TABLE'),
    ('select', 'SELECT'),
    ('from', 'FROM'),
    ('column*', 'COLUMN*'),
    ('col$umn', 'COL$UMN'),
    ('col+-/umn', 'COL+-/UMN'),
    ('col umn', 'COL UMN'),
    ('max_size', get_random_string(string.ascii_uppercase, 30)),
    ('plus', get_random_string(string.ascii_uppercase, 5) + '+' + get_random_string(string.ascii_uppercase, 5)),
    ('underscore', get_random_string(string.ascii_uppercase, 5) + '_' + get_random_string(string.ascii_uppercase, 5)),
    ('short', 'A'),
]


@pytest.fixture
def stage_name() -> str:
    return STAGE_NAME


class TeradataAuthorization(ABC):
    def __init__(self, name: str, teradata: TeradataInstance, cleanup: Callable):
        self.name = name
        self.teradata = teradata
        cleanup(self.drop)
        self.create()

    @abstractmethod
    def create(self):
        pass

    def drop(self):
        logger.info(f'Dropping Teradata Authorization {self.teradata.database}.{self.name}')
        self.teradata.engine.execute(f'DROP AUTHORIZATION {self.teradata.database}.{self.name}')


class LocalTeradataAuthorization(TeradataAuthorization):
    def create(self):
        # local staging does not require to create auth
        self.name = ""

    def drop(self):
        # local staging does not require to drop auth
        pass


class AwsTeradataAuthorization(TeradataAuthorization):
    def create(self):
        logger.info(f'Creating Teradata Authorization for AWS {self.teradata.database}.{self.name}')
        self.teradata.engine.execute(
             f"CREATE AUTHORIZATION {self.teradata.database}.{self.name} "
             f"USER '{self.teradata.aws_access_key_id}' "
             f"PASSWORD '{self.teradata.aws_secret_access_key}'")


class BlobStorageTeradataAuthorization(TeradataAuthorization):
    def create(self):
        logger.info(f'Creating Teradata Authorization for Azure {self.teradata.database}.{self.name}')
        self.teradata.engine.execute(
            f"CREATE AUTHORIZATION {self.teradata.database}.{self.name} "
            f"USER '{self.teradata.azure_storage_account_name}' "
            f"PASSWORD '{self.teradata.azure_storage_account_key}'")


class AzureTeradataAuthorization(TeradataAuthorization):
    def create(self):
        logger.info(f'Creating Teradata Authorization for Azure {self.teradata.database}.{self.name}')
        self.teradata.engine.execute(
            f"CREATE AUTHORIZATION {self.teradata.database}.{self.name} "
            f"USER '{self.teradata.adls_gen2_account}' "
            f"PASSWORD '{self.teradata.storage_gen2_account_key}'")


class GCSTeradataAuthorization(TeradataAuthorization):
    def create(self):
        logger.info(f'Creating Teradata Authorization for GCS {self.teradata.database}.{self.name}')
        self.teradata.engine.execute(
            f"CREATE AUTHORIZATION {self.teradata.database}.{self.name} "
            f"USER '{self.teradata.gcp_email}' "
            f"PASSWORD '{self.teradata.gcp_credentials_key}'")


class TeradataConnectionManager:
    def __init__(self, teradata: TeradataInstance, staging_location: str, cleanup: Callable):
        self.teradata = teradata
        self.staging_location = staging_location
        self.cleanup = cleanup

    def execute_query(self, query: str):
        return self.teradata.engine.execute(query)

    def create_table(self, table: Table = None) -> Table:
        """Creates a Teradata table and returns it. If there is no Table passed, a default table is created."""
        if table is None:
            table = self.describe_table()
        logger.info(f'Creating Teradata Table {table.name}')
        table.create(self.teradata.engine)
        return table

    def describe_table(self, table_name: str = None, columns: tuple = None) -> Table:
        """Describes a Teradata table definition and returns it."""
        if table_name is None:
            table_name = f'STF_TABLE_{get_random_string(string.ascii_uppercase, 10)}'
        if columns is None:
            columns = ({Column('id', Integer, primary_key=True), Column('name', String(32))})
        metadata = MetaData()
        table = Table(
            table_name,
            metadata,
            *columns,
            schema=self.teradata.database,
        )
        logger.info(f'Describing Teradata Table {table.name}')
        self.cleanup(table.drop, self.teradata.engine)
        return table

    def populate_table(self, table: Table) -> Table:
        """
            It will get the table definition from the database, in case it has changed.
        """
        metadata = MetaData()
        return Table(table.name, metadata, autoload=True, autoload_with=self.teradata.engine, schema=self.teradata.database)

    def column_names(self, table: Table) -> list:
        """
            Gets the list of column names
        """
        return [column.name for column in table.columns]

    def column_definitions(self, table: Table) -> list:
        """
            Gets the list of column types
        """
        return [column.type for column in table.columns]

    def primary_key_definition(self, table: Table) -> list:
        """
            Gets the list of primary key column names
        """
        return [column.name for column in table.primary_key]

    def create_authorization(self, authorization_name: str = None) -> TeradataAuthorization:
        """Creates a Teradata authorization and returns it"""
        if authorization_name is None:
            authorization_name = f'STF_AUTH_{get_random_string(string.ascii_uppercase, 10)}'
        if self.staging_location == "LOCAL":
            return LocalTeradataAuthorization(authorization_name, self.teradata, self.cleanup)
        elif self.staging_location == "AWS_S3":
            return AwsTeradataAuthorization(authorization_name, self.teradata, self.cleanup)
        elif self.staging_location == "ADLS_GEN2":
            return AzureTeradataAuthorization(authorization_name, self.teradata, self.cleanup)
        elif self.staging_location == "BLOB_STORAGE":
            return BlobStorageTeradataAuthorization(authorization_name, self.teradata, self.cleanup)
        elif self.staging_location == "GCS":
            return GCSTeradataAuthorization(authorization_name, self.teradata, self.cleanup)

    def select_from_table(self, table: Table, order_by_column: str = None) -> list:
        """
            Selects all data from a Teradata table and returns it with columns ordered by
            column name and values ordered by column value passed, or by first column values.
        """
        table_column_names = sorted(table.columns.keys())
        if order_by_column is None:
            order_by_column = table_column_names[0] or ""
        result = self.teradata.engine.execute(table.select().order_by(asc(order_by_column)))
        database_rows = result.fetchall()
        result.close()
        logger.info(f'Selected {len(database_rows)} rows from Teradata Table {table.name}')
        return self.generate_database_result(database_rows, table_column_names)

    def generate_database_result(self, rows: list, column_names: list) -> list:
        """
            Generates a [{column_name_1: column_value_1, ...}, ...] structure using the
            rows and column_names of a query result.
        """
        json_rows = []
        for row in rows:
            row_dict = {}
            for column_name in column_names:
                row_dict[column_name] = getattr(row, column_name)
            json_rows.append(row_dict)
        return json_rows


@pytest.fixture
def teradata_manager(teradata: TeradataInstance, staging_location: str, cleanup: Callable) -> TeradataConnectionManager:
    return TeradataConnectionManager(teradata, staging_location, cleanup)
