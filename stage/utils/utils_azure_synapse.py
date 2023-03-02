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

"""
A module providing utils for working with Azure Synapse
"""

import datetime
import json
import logging

from streamsets.testframework.utils import Version

logger = logging.getLogger(__name__)

STAGE_NAME = 'com_streamsets_pipeline_stage_destination_datawarehouse_AzureDataWarehouseDTarget'

DEFAULT_DIRECTORY_TEMPLATE = '/tmp/out/${YYYY()}-${MM()}-${DD()}-${hh()}'

SDC_COLUMN_TYPES = [
    {'name': 'SDC_STRING', 'type': 'varchar'},
    {'name': 'SDC_INTEGER', 'type': 'int'},
    {'name': 'SDC_LONG', 'type': 'bigint'},
    {'name': 'SDC_FLOAT', 'type': 'real'},
    {'name': 'SDC_DOUBLE', 'type': 'float'},
    {'name': 'SDC_DATE', 'type': 'date'},
    {'name': 'SDC_DATETIME', 'type': 'datetime'},
    {'name': 'SDC_ZONED_DATETIME', 'type': 'varchar'},
    {'name': 'SDC_TIME', 'type': 'time'},
    {'name': 'SDC_BOOLEAN', 'type': 'bit'},
    {'name': 'SDC_DECIMAL', 'type': 'numeric'},
    {'name': 'SDC_BYTE_ARRAY', 'type': 'binary'}
]

SDC_DATA_TYPES = [
    {'field': 'SDC_STRING', 'type': 'STRING'},
    {'field': 'SDC_INTEGER', 'type': 'INTEGER'},
    {'field': 'SDC_LONG', 'type': 'LONG'},
    {'field': 'SDC_FLOAT', 'type': 'FLOAT'},
    {'field': 'SDC_DOUBLE', 'type': 'DOUBLE'},
    {'field': 'SDC_DATE', 'type': 'DATE'},
    {'field': 'SDC_DATETIME', 'type': 'DATETIME'},
    {'field': 'SDC_ZONED_DATETIME', 'type': 'ZONED_DATETIME'},
    {'field': 'SDC_TIME', 'type': 'TIME'},
    {'field': 'SDC_BOOLEAN', 'type': 'BOOLEAN'},
    {'field': 'SDC_DECIMAL', 'precision': 10, 'scale': 2, 'type': 'DECIMAL'},
    {'field': 'SDC_BYTE_ARRAY', 'type': 'BYTE_ARRAY'},
]

DATA_TYPES = [
    # Boolean
    ('true', 'BOOLEAN', 'char(4)', '1   '),
    ('true', 'BOOLEAN', 'nchar(4)', '1   '),
    ('true', 'BOOLEAN', 'varchar(4)', '1'),
    ('true', 'BOOLEAN', 'nvarchar(4)', '1'),
    ('true', 'BOOLEAN', 'bigint', True),
    ('true', 'BOOLEAN', 'bit', True),
    ('true', 'BOOLEAN', 'decimal', True),
    ('true', 'BOOLEAN', 'float', True),
    ('true', 'BOOLEAN', 'int', True),
    ('true', 'BOOLEAN', 'numeric', True),
    ('true', 'BOOLEAN', 'real', True),
    ('true', 'BOOLEAN', 'smallint', True),
    ('true', 'BOOLEAN', 'tinyint', True),

    # Byte
    ('65', 'BYTE', 'char(2)', '65'),
    ('65', 'BYTE', 'varchar(2)', '65'),
    ('65', 'BYTE', 'nchar(2)', '65'),
    ('65', 'BYTE', 'nvarchar(2)', '65'),
    ('65', 'BYTE', 'bigint', 65),
    ('65', 'BYTE', 'decimal', 65),
    ('65', 'BYTE', 'float', 65),
    ('65', 'BYTE', 'int', 65),
    ('65', 'BYTE', 'numeric', 65),
    ('65', 'BYTE', 'real', 65),
    ('65', 'BYTE', 'smallint', 65),
    ('65', 'BYTE', 'tinyint', 65),
    ('65', 'BYTE', 'money', 65.0000),
    ('65', 'BYTE', 'smallmoney', 65.0000),

    # Short
    (120, 'SHORT', 'char(5)', '120  '),
    (120, 'SHORT', 'varchar(5)', '120'),
    (120, 'SHORT', 'nchar(5)', '120  '),
    (120, 'SHORT', 'nvarchar(5)', '120'),
    (120, 'SHORT', 'bigint', 120),
    (120, 'SHORT', 'decimal', 120),
    (120, 'SHORT', 'float', 120),
    (120, 'SHORT', 'int', 120),
    (120, 'SHORT', 'numeric', 120),
    (120, 'SHORT', 'real', 120),
    (120, 'SHORT', 'smallint', 120),
    (120, 'SHORT', 'tinyint', 120),
    (120, 'SHORT', 'money', 120.0000),
    (120, 'SHORT', 'smallmoney', 120.0000),

    # Integer
    (120, 'INTEGER', 'char(5)', '120  '),
    (120, 'INTEGER', 'varchar(5)', '120'),
    (120, 'INTEGER', 'nchar(5)', '120  '),
    (120, 'INTEGER', 'nvarchar(5)', '120'),
    (120, 'INTEGER', 'bigint', 120),
    (120, 'INTEGER', 'decimal', 120),
    (120, 'INTEGER', 'float', 120),
    (120, 'INTEGER', 'int', 120),
    (120, 'INTEGER', 'numeric', 120),
    (120, 'INTEGER', 'real', 120),
    (120, 'INTEGER', 'money', 120.0000),
    (120, 'INTEGER', 'smallmoney', 120.0000),

    # Long
    (120, 'LONG', 'char(5)', '120  '),
    (120, 'LONG', 'varchar(5)', '120'),
    (120, 'LONG', 'nchar(5)', '120  '),
    (120, 'LONG', 'nvarchar(5)', '120'),
    (120, 'LONG', 'bigint', 120),
    (120, 'LONG', 'decimal', 120),
    (120, 'LONG', 'float', 120),
    (120, 'LONG', 'numeric', 120),
    (120, 'LONG', 'money', 120.0000),
    (120, 'LONG', 'smallmoney', 120.0000),

    # Float
    (120.0, 'FLOAT', 'char(5)', '120.0'),
    (120.0, 'FLOAT', 'varchar(5)', '120.0'),
    (120.0, 'FLOAT', 'nchar(5)', '120.0'),
    (120.0, 'FLOAT', 'nvarchar(5)', '120.0'),
    (120.0, 'FLOAT', 'decimal', 120),
    (120.0, 'FLOAT', 'float', 120),
    (120.0, 'FLOAT', 'numeric', 120),
    (120.0, 'FLOAT', 'real', 120),
    (120.0, 'FLOAT', 'money', 120.0000),
    (120.0, 'FLOAT', 'smallmoney', 120.0000),

    # Double
    (120.0, 'DOUBLE', 'char(5)', '120.0'),
    (120.0, 'DOUBLE', 'varchar(5)', '120.0'),
    (120.0, 'DOUBLE', 'nchar(5)', '120.0'),
    (120.0, 'DOUBLE', 'nvarchar(5)', '120.0'),
    (120.0, 'DOUBLE', 'decimal', 120),
    (120.0, 'DOUBLE', 'float', 120),
    (120.0, 'DOUBLE', 'numeric', 120),
    (120.0, 'DOUBLE', 'money', 120.0000),
    (120.0, 'DOUBLE', 'smallmoney', 120.0000),

    # Decimal
    (120.0, 'DECIMAL', 'char(15)', '120.00         '),
    (120.0, 'DECIMAL', 'varchar(15)', '120.00'),
    (120.0, 'DECIMAL', 'nchar(15)', '120.00         '),
    (120.0, 'DECIMAL', 'nvarchar(15)', '120.00'),
    (120.0, 'DECIMAL', 'decimal', 120.00),
    (120.0, 'DECIMAL', 'numeric', 120.00),
    (120.0, 'DECIMAL', 'money', 120.0000),
    (120.0, 'DECIMAL', 'smallmoney', 120.0000),

    # Date
    ('2020-01-01 10:00:00', 'DATE', 'char(50)', '2020-01-01                                        '),
    ('2020-01-01 10:00:00', 'DATE', 'varchar(50)', '2020-01-01'),
    ('2020-01-01 10:00:00', 'DATE', 'nchar(50)', '2020-01-01                                        '),
    ('2020-01-01 10:00:00', 'DATE', 'nvarchar(50)', '2020-01-01'),
    ('2020-01-01 10:00:00', 'DATE', 'date', datetime.date(2020, 1, 1)),

    # Time
    ('10:00:00', 'TIME', 'char(50)', '10:00:00.000                                      '),
    ('10:00:00', 'TIME', 'varchar(50)', '10:00:00.000'),
    ('10:00:00', 'TIME', 'nchar(50)', '10:00:00.000                                      '),
    ('10:00:00', 'TIME', 'nvarchar(50)', '10:00:00.000'),
    ('10:00:00', 'TIME', 'time', datetime.time(10, 0, 0)),

    # DateTime
    ('2020-01-01 10:00:00', 'DATETIME', 'char(50)', '2020-01-01 10:00:00.000                           '),
    ('2020-01-01 10:00:00', 'DATETIME', 'varchar(50)', '2020-01-01 10:00:00.000'),
    ('2020-01-01 10:00:00', 'DATETIME', 'nchar(50)', '2020-01-01 10:00:00.000                           '),
    ('2020-01-01 10:00:00', 'DATETIME', 'nvarchar(50)', '2020-01-01 10:00:00.000'),
    ('2020-01-01 10:00:00', 'DATETIME', 'date', datetime.date(2020, 1, 1)),
    ('2020-01-01 10:00:00', 'DATETIME', 'datetime', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATETIME', 'datetime2', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATETIME', 'smalldatetime', datetime.datetime(2020, 1, 1, 10, 0)),
    ('2020-01-01 10:00:00', 'DATETIME', 'time', datetime.time(10, 0)),

    # Zoned DateTime
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'char(50)', '2020-01-01 10:00:00.000 +0000                     '),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'varchar(50)', '2020-01-01 10:00:00.000 +0000'),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'nchar(50)', '2020-01-01 10:00:00.000 +0000                     '),
    ("2020-01-01T10:00:00+00:00", 'ZONED_DATETIME', 'nvarchar(50)', '2020-01-01 10:00:00.000 +0000'),

    # String
    ('string', 'STRING', 'char(15)', 'string         '),
    ('string', 'STRING', 'varchar(15)', 'string'),
    ('string', 'STRING', 'nchar(15)', 'string         '),
    ('string', 'STRING', 'nvarchar(15)', 'string'),
]

ROWS_IN_DATABASE = [
    {
        'id': 1,
        'name': 'Roger Federer'
    }, {
        'id': 2,
        'name': 'Rafael Nadal'
    }, {
        'id': 3,
        'name': 'Alexander Zverev'
    }
]

EXTENDED_ROWS_IN_DATABASE = [
    {
        'id': 4,
        'name': 'Ana Ivanovic',
        'country': 'Serbia',
        'birth': 1987
    }, {
        'id': 5,
        'name': 'Serena Williams',
        'country': 'United States',
        'birth': 1981
    }, {
        'id': 6,
        'name': 'Caroline Wozniacki',
        'country': 'Denmark',
        'birth': 1990
    }
]

EXPECTED_DRIFT_RESULT = [
    {
        'id': 1,
        'name': 'Roger Federer',
        'country': None,
        'birth': None
    }, {
        'id': 2,
        'name': 'Rafael Nadal',
        'country': None,
        'birth': None
    }, {
        'id': 3,
        'name': 'Alexander Zverev',
        'country': None,
        'birth': None
    }, {
        'id': 4,
        'name': 'Ana Ivanovic',
        'country': 'Serbia',
        'birth': 1987
    }, {
        'id': 5,
        'name': 'Serena Williams',
        'country': 'United States',
        'birth': 1981
    }, {
        'id': 6,
        'name': 'Caroline Wozniacki',
        'country': 'Denmark',
        'birth': 1990
    }
]

INVALID_TYPES_ROWS_IN_DATABASE = [
    {
        'id': 7,
        'name': 'Julia Goerges',
        'country': 'Germany',
        'birth': 'invalid-year'
    }, {
        'id': 8,
        'name': 'Andy Murray',
        'country': 1,
        'birth': 1987
    }
]

EXPECTED_DRIFT_RESULT_WITH_INVALID_TYPES_ROWS = [
    {
        'id': 4,
        'name': 'Ana Ivanovic',
        'country': 'Serbia',
        'birth': 1987
    }, {
        'id': 5,
        'name': 'Serena Williams',
        'country': 'United States',
        'birth': 1981
    }, {
        'id': 6,
        'name': 'Caroline Wozniacki',
        'country': 'Denmark',
        'birth': 1990
    }, {
        'id': 7,
        'name': 'Julia Goerges',
        'country': 'Germany',
        'birth': None
    }, {
        'id': 8,
        'name': 'Andy Murray',
        'country': '1',
        'birth': 1987
    }
]

ROWS_IN_DATABASE_WITH_PARTITION = [
    {
        'id': 1,
        'name': 'Roger Federer',
        'partition': 'a'
    }, {
        'id': 2,
        'name': 'Rafael Nadal',
        'partition': 'a'
    }, {
        'id': 3,
        'name': 'Alexander Zverev',
        'partition': 'b'
    }, {
        'id': 4,
        'name': 'Rogelio Federer',
        'partition': 'b'
    }, {
        'id': 5,
        'name': 'Rafa Nadal',
        'partition': 'c'
    }, {
        'id': 6,
        'name': 'Alex Zverev',
        'partition': 'c'
    }
]

FORMATTED_ROWS_IN_DATABASE_WITH_PARTITION = [
    (row['id'], row['name'], row['partition']) for row in ROWS_IN_DATABASE_WITH_PARTITION
]

# from CommonDatabaseHeader.java
PRIMARY_KEY_COLUMN_OLD_VALUE = 'jdbc.primaryKey.before'
PRIMARY_KEY_COLUMN_NEW_VALUE = 'jdbc.primaryKey.after'
PRIMARY_KEY_SPECIFICATION = 'jdbc.primaryKeySpecification'

CDC_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY = [
    {
        'TYPE': 'Hobbit',
        'ID': 1,
        'NAME': 'Bilbo',
        'SURNAME': 'Baggins',
        'ADDRESS': 'Bag End 0'
    }, {
        'TYPE': 'Fallohide',
        'ID': 1,
        'NAME': 'Bilbo',
        'SURNAME': 'Baggins',
        'ADDRESS': 'Bag End 1'
    }, {
        'TYPE': 'Fallohide',
        'ID': 2,
        'NAME': 'Bilbo',
        'SURNAME': 'Baggins',
        'ADDRESS': 'Bag End 2'
    }, {
        'TYPE': 'Hobbit - Fallohide',
        'ID': 3,
        'NAME': 'Bilbo',
        'SURNAME': 'Baggins',
        'ADDRESS': 'Bag End 3'
    }, {
        'TYPE': 'Hobbit, Fallohide',
        'ID': 3,
        'NAME': 'Bilbo',
        'SURNAME': 'Baggins',
        'ADDRESS': 'Bag End 4'
    }, {
        'TYPE': 'Hobbit, Fallohide',
        'ID': 4,
        'NAME': 'Bilbo',
        'SURNAME': 'Baggins',
        'ADDRESS': 'Bag End 5'
    }, {
        'TYPE': 'Hobbit, Fallohide',
        'ID': 4,
        'NAME': 'Bilbo',
        'SURNAME': 'Baggins',
        'ADDRESS': 'Bag End 6'
    }
]

CDC_PK_UPDATES_ROWS_IN_DATABASE_COMPOSITE_KEY_HEADER = [
    {
        'sdc.operation.type': 1,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 1,
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 1,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Hobbit',
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Hobbit'
    }, {
        'sdc.operation.type': 3,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 1,
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 1,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Hobbit',
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Fallohide'
    }, {
        'sdc.operation.type': 3,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 1,
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 2,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Fallohide',
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Fallohide'
    }, {
        'sdc.operation.type': 3,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 2,
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 3,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Fallohide',
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Hobbit - Fallohide'
    }, {
        'sdc.operation.type': 3,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 3,
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 3,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Hobbit - Fallohide',
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Hobbit, Fallohide'
    }, {
        'sdc.operation.type': 3,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 3,
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 4,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Hobbit, Fallohide',
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Hobbit, Fallohide'
    }, {
        'sdc.operation.type': 3,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.ID': 4,
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.ID': 4,
        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.TYPE': 'Hobbit, Fallohide',
        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.TYPE': 'Hobbit, Fallohide'
    }
]


def delete_table(engine, table_name, schema_name):
    table_name_with_schema = f'{schema_name}.{table_name}'
    logger.info('Deleting table with name = %s...', table_name_with_schema)
    query = f'DROP TABLE {table_name_with_schema};'
    engine.execute(query)
    engine.dispose()


def delete_schema(engine, schema_name):
    logger.info('Deleting schema with name = %s...', schema_name)
    query = f'DROP SCHEMA {schema_name};'
    engine.execute(query)
    engine.dispose()


def clean_up(dl_fs, directory_name):
    paths = dl_fs.ls(directory_name).response.json()['paths']
    dl_files = [item['name'] for item in paths] if paths else []
    # Note: Non-empty directory is not allowed to be removed, hence remove all files first.
    logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
    for dl_file in dl_files:
        dl_fs.rm(dl_file)
    dl_fs.rmdir(directory_name)


def stop_pipeline(sdc_executor, pipeline):
    if sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
        sdc_executor.stop_pipeline(pipeline)


def get_columns(engine, table_name):
    table_name_with_schema = f'{table_name}'
    logger.info('Getting table metadata with name = %s...', table_name_with_schema)
    query = f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS " \
            f"WHERE TABLE_NAME = '{table_name_with_schema}' " \
            f"ORDER BY ORDINAL_POSITION;"
    result = engine.execute(query)
    column_info = sorted(result.fetchall(), key=lambda row: row[0])
    result.close()
    return column_info


def get_synapse_pipeline(
        sdc_builder,
        azure,
        data,
        stage_file_prefix,
        destination_table_name,
        enable_data_drift=True,
        ignore_missing_fields=True,
        ignore_fields_with_invalid_types=True,
        purge_stage_file_after_loading=True,
        directory_template=DEFAULT_DIRECTORY_TEMPLATE
):
    raw_data = json.dumps(data)
    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(
        data_format='JSON',
        json_content='ARRAY_OBJECTS',
        raw_data=raw_data,
        stop_after_first_batch=True
    )

    azure_synapse_destination = builder.add_stage(name=STAGE_NAME)
    azure_synapse_destination.set_attributes(
        auto_create_table=True,
        table=destination_table_name,
        enable_data_drift=enable_data_drift,
        ignore_missing_fields=ignore_missing_fields,
        ignore_fields_with_invalid_types=ignore_fields_with_invalid_types,
        purge_stage_file_after_loading=purge_stage_file_after_loading,
        stage_file_prefix=stage_file_prefix,
        directory_template=directory_template
    )
    dev_raw_data_source >> azure_synapse_destination

    pipeline = builder.build().configure_for_environment(azure)
    return pipeline


def get_data_warehouse_types_pipeline(
        sdc_builder,
        azure,
        stage_file_prefix,
        destination_table_name,
        enable_data_drift=True,
        ignore_missing_fields=True,
        ignore_fields_with_invalid_types=True,
        is_cdc=False
):
    builder = sdc_builder.get_pipeline_builder()
    dev_data_generator = builder.add_stage('Dev Data Generator')
    dev_data_generator.set_attributes(
        batch_size=1,
        delay_between_batches=10
    )

    dev_data_generator.fields_to_generate = SDC_DATA_TYPES

    azure_synapse_destination = builder.add_stage(name=STAGE_NAME)
    azure_synapse_destination.set_attributes(
        auto_create_table=True,
        table=destination_table_name,
        enable_data_drift=enable_data_drift,
        ignore_missing_fields=ignore_missing_fields,
        ignore_fields_with_invalid_types=ignore_fields_with_invalid_types,
        purge_stage_file_after_loading=True,
        stage_file_prefix=stage_file_prefix,
        merge_cdc_data=is_cdc
    )
    if is_cdc:
        # Build Expression Evaluator
        expression_evaluator = builder.add_stage('Expression Evaluator')
        expression_evaluator.set_attributes(
            header_attribute_expressions=[{
               'attributeToSet': 'sdc.operation.type',
               'headerAttributeExpression': "1"
           }]
        )

        azure_synapse_destination.set_attributes(
            key_columns=[{
                "keyColumns": ["SDC_STRING"],
                "table": destination_table_name
            }]
        )

        # primary key location attribute added in stage version 5
        if Version(azure_synapse_destination.stage_version) > Version('4'):
            azure_synapse_destination.set_attributes(primary_key_location="TABLE")

        dev_data_generator >> expression_evaluator >> azure_synapse_destination
    else:
        dev_data_generator >> azure_synapse_destination

    datalake_dest_pipeline = builder.build().configure_for_environment(azure)
    return datalake_dest_pipeline
