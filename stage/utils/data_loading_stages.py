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

import json
import string

import pytest

from typing import Callable

from streamsets.sdk.sdc_models import History
from streamsets.sdk.utils import get_random_string
from streamsets.sdk.exceptions import RunError, RunningError
from streamsets.testframework.sdc import DataCollector
from streamsets.testframework.sdc_models import PipelineBuilder, Stage, Pipeline

# from CommonDatabaseHeader.java
PRIMARY_KEY_COLUMN_OLD_VALUE = 'jdbc.primaryKey.before'
PRIMARY_KEY_COLUMN_NEW_VALUE = 'jdbc.primaryKey.after'
PRIMARY_KEY_SPECIFICATION = 'jdbc.primaryKeySpecification'
SDC_OPERATION_TYPE = 'sdc.operation.type'

# from OperationType.java
OPERATION_TYPE = {
    "INSERT": 1,
    "DELETE": 2,
    "UPDATE": 3,
    "UPSERT": 4,
    "UNSUPPORTED": 5,
    "UNDELETE": 6,
    "REPLACE": 7,
    "MERGE": 8,
    "LOAD": 9
}

PRIMARY_KEY_COLUMNS = 'PRIMARY_KEY_COLUMNS'
SETUP_OPERATIONS = 'SETUP_OPERATIONS'
CDC_OPERATIONS = 'CDC_OPERATIONS'
EXPECTED_RESULT = 'EXPECTED_RESULT'

ROW = 'ROW'
TABLE = 'TABLE'
OPERATION = 'OPERATION'
PRIMARY_KEY_DEFINITION = 'PRIMARY_KEY_DEFINITION'
PRIMARY_KEY_VALUE_UPDATE = 'PRIMARY_KEY_VALUE_UPDATE'
PRIMARY_KEY_PREVIOUS_VALUE = 'PRIMARY_KEY_PREVIOUS_VALUE'
CDC_METADATA_RECORD_FIELDS = [TABLE, OPERATION, PRIMARY_KEY_DEFINITION, PRIMARY_KEY_VALUE_UPDATE]

SIMPLE_ROWS = [
    {'id': 1, 'name': 'Nicky Jam'},
    {'id': 2, 'name': 'Don Omar'},
    {'id': 3, 'name': 'Daddy Yankee'}
]

DUPLICATE_COLUMN_ROWS = [
    {'id': 1, 'name': 'Bad Bunny'},
    {'id': 2, 'name': 'Bad Bunny'},
    {'id': 3, 'name': 'Trueno'}
]

MISSING_FIELD_ROWS = [
    {'name': 'Nicky Jam'},
    {'id': 2, 'name': 'Don Omar'},
    {'id': 3, 'name': 'Daddy Yankee'}
]


def wildcard_field_name_simple_rows(field_name: str):
    return [
        {'id': 1, field_name: 'Nicky Jam'},
        {'id': 2, field_name: 'Don Omar'},
        {'id': 3, field_name: 'Daddy Yankee'}
    ]


def wildcard_field_simple_rows(field: dict):
    return [
        {**field, 'name': 'Nicky Jam'},
        {'id': 2, 'name': 'Don Omar'},
        {'id': 3, 'name': 'Daddy Yankee'}
    ]


DATA_TYPE_TEST_COLUMN = 'column'

CDC_DATABASE_COLUMNS = ['col_1', 'col_2']

CDC_TEST_CASES = [
    ({
        PRIMARY_KEY_COLUMNS: ['col_1'],
        SETUP_OPERATIONS: [],
        CDC_OPERATIONS: [
            {OPERATION: 'INSERT', ROW: {'col_1': 1, 'col_2': 1}},
            {OPERATION: 'UPDATE', ROW: {'col_1': 1, 'col_2': 2}}
        ],
        EXPECTED_RESULT: [
            {'col_1': 1, 'col_2': 2}
        ]
    }),
    ({
        PRIMARY_KEY_COLUMNS: ['col_1'],
        SETUP_OPERATIONS: [
            {OPERATION: 'INSERT', ROW: {'col_1': 1, 'col_2': 1}}
        ],
        CDC_OPERATIONS: [  # if the 3 operations are executed in the same batch, table doesn't even get created
            {OPERATION: 'UPDATE', ROW: {'col_1': 2, 'col_2': 2}, PRIMARY_KEY_PREVIOUS_VALUE: {'col_1': 1}},
            {OPERATION: 'DELETE', ROW: {'col_1': 2, 'col_2': 2}}
        ],
        EXPECTED_RESULT: []
    }),
    ({
        PRIMARY_KEY_COLUMNS: ['col_1'],
        SETUP_OPERATIONS: [
            {OPERATION: 'INSERT', ROW: {'col_1': 1, 'col_2': 1}}
        ],
        CDC_OPERATIONS: [
            {OPERATION: 'MERGE', ROW: {'col_1': 1, 'col_2': 2}}
        ],
        EXPECTED_RESULT: [
            {'col_1': 1, 'col_2': 1}  # we will just have the INSERT record in the db
        ]
    }),
    ({
        PRIMARY_KEY_COLUMNS: ['col_1'],
        SETUP_OPERATIONS: [],
        CDC_OPERATIONS: [
            {OPERATION: 'INSERT', ROW: {'col_1': 1, 'col_2': 1}},
            {OPERATION: 'UPDATE', ROW: {'col_1': 1, 'col_2': 2}},
            {OPERATION: 'REPLACE', ROW: {'col_1': 1, 'col_2': 3}},
            {OPERATION: 'UPSERT', ROW: {'col_1': 1, 'col_2': 4}}
        ],
        EXPECTED_RESULT: [
            {'col_1': 1, 'col_2': 4}
        ]
    }),
    ({
        PRIMARY_KEY_COLUMNS: ['col_1'],
        SETUP_OPERATIONS: [
            {OPERATION: 'INSERT', ROW: {'col_1': 1, 'col_2': 1}}
        ],
        CDC_OPERATIONS: [
            {OPERATION: 'UPDATE', ROW: {'col_1': 2, 'col_2': 1}, PRIMARY_KEY_PREVIOUS_VALUE: {'col_1': 1}},
            {OPERATION: 'DELETE', ROW: {'col_1': 2, 'col_2': 1}},
            {OPERATION: 'INSERT', ROW: {'col_1': 1, 'col_2': 3}},
            {OPERATION: 'DELETE', ROW: {'col_1': 1, 'col_2': 4}},
            {OPERATION: 'INSERT', ROW: {'col_1': 5, 'col_2': 5}}
        ],
        EXPECTED_RESULT: [
            {'col_1': 5, 'col_2': 5}
        ]
    }),
    ({
        PRIMARY_KEY_COLUMNS: ['col_1'],
        SETUP_OPERATIONS: [
            {OPERATION: 'INSERT', ROW: {'col_1': 1, 'col_2': 1}}
        ],
        CDC_OPERATIONS: [
            {OPERATION: 'UPDATE', ROW: {'col_1': 2, 'col_2': 1}, PRIMARY_KEY_PREVIOUS_VALUE: {'col_1': 1}},
            {OPERATION: 'DELETE', ROW: {'col_1': 2, 'col_2': 1}},
            {OPERATION: 'INSERT', ROW: {'col_1': 1, 'col_2': 3}},
            {OPERATION: 'UPDATE', ROW: {'col_1': 3, 'col_2': 4}, PRIMARY_KEY_PREVIOUS_VALUE: {'col_1': 1}},
            {OPERATION: 'DELETE', ROW: {'col_1': 3, 'col_2': 4}},
            {OPERATION: 'INSERT', ROW: {'col_1': 1, 'col_2': 5}}
        ],
        EXPECTED_RESULT: [
            {'col_1': 1, 'col_2': 5}
        ]
    }),
    ({
        PRIMARY_KEY_COLUMNS: ['col_1', 'col_2'],
        SETUP_OPERATIONS: [
            {OPERATION: 'INSERT', ROW: {'col_1': 1, 'col_2': 1}}
        ],
        CDC_OPERATIONS: [
            {OPERATION: 'UPDATE', ROW: {'col_1': 1, 'col_2': 2}, PRIMARY_KEY_PREVIOUS_VALUE: {'col_1': 1, 'col_2': 1}},
            {OPERATION: 'UPDATE', ROW: {'col_1': 2, 'col_2': 2}, PRIMARY_KEY_PREVIOUS_VALUE: {'col_1': 1, 'col_2': 2}},
            {OPERATION: 'UPDATE', ROW: {'col_1': 2, 'col_2': 3}, PRIMARY_KEY_PREVIOUS_VALUE: {'col_1': 2, 'col_2': 2}},
            {OPERATION: 'UPDATE', ROW: {'col_1': 3, 'col_2': 3}, PRIMARY_KEY_PREVIOUS_VALUE: {'col_1': 2, 'col_2': 3}},
            {OPERATION: 'UPDATE', ROW: {'col_1': 4, 'col_2': 4}, PRIMARY_KEY_PREVIOUS_VALUE: {'col_1': 3, 'col_2': 3}},
            {OPERATION: 'UPDATE', ROW: {'col_1': 4, 'col_2': 5}, PRIMARY_KEY_PREVIOUS_VALUE: {'col_1': 4, 'col_2': 4}},
            {OPERATION: 'UPDATE', ROW: {'col_1': 5, 'col_2': 5}, PRIMARY_KEY_PREVIOUS_VALUE: {'col_1': 4, 'col_2': 5}}
        ],
        EXPECTED_RESULT: [
            {'col_1': 5, 'col_2': 5}
        ]
    }),
    ({
        PRIMARY_KEY_COLUMNS: ['col_1', 'col_2'],
        SETUP_OPERATIONS: [
            {OPERATION: 'INSERT', ROW: {'col_1': 1, 'col_2': 1}}
        ],
        CDC_OPERATIONS: [
            {OPERATION: 'UPDATE', ROW: {'col_1': 1, 'col_2': 2}, PRIMARY_KEY_PREVIOUS_VALUE: {'col_1': 1, 'col_2': 1}},
            {OPERATION: 'DELETE', ROW: {'col_1': 1, 'col_2': 2}},
            {OPERATION: 'INSERT', ROW: {'col_1': 1, 'col_2': 2}},
            {OPERATION: 'UPDATE', ROW: {'col_1': 1, 'col_2': 1}, PRIMARY_KEY_PREVIOUS_VALUE: {'col_1': 1, 'col_2': 2}},
            {OPERATION: 'DELETE', ROW: {'col_1': 1, 'col_2': 1}},
            {OPERATION: 'INSERT', ROW: {'col_1': 1, 'col_2': 1}},
            {OPERATION: 'UPDATE', ROW: {'col_1': 1, 'col_2': 2}, PRIMARY_KEY_PREVIOUS_VALUE: {'col_1': 1, 'col_2': 1}},
            {OPERATION: 'UPDATE', ROW: {'col_1': 2, 'col_2': 2}, PRIMARY_KEY_PREVIOUS_VALUE: {'col_1': 1, 'col_2': 2}}
        ],
        EXPECTED_RESULT: [
            {'col_1': 2, 'col_2': 2}
        ]
    }),
    ({
        PRIMARY_KEY_COLUMNS: ['col_1', 'col_2'],
        SETUP_OPERATIONS: [],
        CDC_OPERATIONS: [
            {OPERATION: 'INSERT', ROW: {'col_1': 1, 'col_2': 1}},
            {OPERATION: 'UPDATE', ROW: {'col_1': 2, 'col_2': 2}, PRIMARY_KEY_PREVIOUS_VALUE: {'col_1': 1, 'col_2': 1}},
            {OPERATION: 'UPDATE', ROW: {'col_1': 3, 'col_2': 3}, PRIMARY_KEY_PREVIOUS_VALUE: {'col_1': 2, 'col_2': 2}},
            {OPERATION: 'INSERT', ROW: {'col_1': 1, 'col_2': 1}},
            {OPERATION: 'UPDATE', ROW: {'col_1': 5, 'col_2': 5}, PRIMARY_KEY_PREVIOUS_VALUE: {'col_1': 1, 'col_2': 1}}
        ],
        EXPECTED_RESULT: [
            {'col_1': 3, 'col_2': 3}, {'col_1': 5, 'col_2': 5}
        ]
    }),
    ({
        PRIMARY_KEY_COLUMNS: ['col_1', 'col_2'],
        SETUP_OPERATIONS: [
            {OPERATION: 'INSERT', ROW: {'col_1': 1, 'col_2': 1}}
        ],
        CDC_OPERATIONS: [
            {OPERATION: 'DELETE', ROW: {'col_1': 1, 'col_2': 1}},
            {OPERATION: 'INSERT', ROW: {'col_1': 1, 'col_2': 2}},
            {OPERATION: 'DELETE', ROW: {'col_1': 1, 'col_2': 2}},
            {OPERATION: 'INSERT', ROW: {'col_1': 1, 'col_2': 1}},
            {OPERATION: 'UPDATE', ROW: {'col_1': 2, 'col_2': 2}, PRIMARY_KEY_PREVIOUS_VALUE: {'col_1': 1, 'col_2': 1}},
            {OPERATION: 'UPDATE', ROW: {'col_1': 3, 'col_2': 3}, PRIMARY_KEY_PREVIOUS_VALUE: {'col_1': 2, 'col_2': 2}},
            {OPERATION: 'UPDATE', ROW: {'col_1': 4, 'col_2': 4}, PRIMARY_KEY_PREVIOUS_VALUE: {'col_1': 3, 'col_2': 3}},
            {OPERATION: 'DELETE', ROW: {'col_1': 4, 'col_2': 4}},
            {OPERATION: 'INSERT', ROW: {'col_1': 1, 'col_2': 1}}
        ],
        EXPECTED_RESULT: [
            {'col_1': 1, 'col_2': 1}
        ]
    }),
]

CDC_MULTI_TABLE_TEST_CASES = [
    ({
        PRIMARY_KEY_COLUMNS: ['col_1'],
        SETUP_OPERATIONS: [],
        CDC_OPERATIONS: [
            {TABLE: 0, OPERATION: 'INSERT', ROW: {'col_1': 1, 'col_2': 10}},
            {TABLE: 1, OPERATION: 'INSERT', ROW: {'col_1': 2, 'col_2': 20}},
            {TABLE: 0, OPERATION: 'INSERT', ROW: {'col_1': 3, 'col_2': 30}},
            {TABLE: 1, OPERATION: 'INSERT', ROW: {'col_1': 4, 'col_2': 40}},
            {TABLE: 0, OPERATION: 'UPDATE', ROW: {'col_1': 1, 'col_2': 50}},
            {TABLE: 1, OPERATION: 'UPDATE', ROW: {'col_1': 2, 'col_2': 60}},
            {TABLE: 0, OPERATION: 'UPDATE', ROW: {'col_1': 3, 'col_2': 70}},
            {TABLE: 1, OPERATION: 'DELETE', ROW: {'col_1': 4, 'col_2': 40}}
        ],
        EXPECTED_RESULT: {
            0: [{'col_1': 1, 'col_2': 50}, {'col_1': 3, 'col_2': 70}],
            1: [{'col_1': 2, 'col_2': 60}]
        }
    })
]


class DataLoadingPipelineHandlerCreator:
    def __init__(self, sdc_builder: DataCollector, sdc_executor: DataCollector, stage_name: str, cleanup: Callable):
        self.sdc_builder = sdc_builder
        self.sdc_executor = sdc_executor
        self.stage_name = stage_name
        self.cleanup = cleanup

    def create(self):
        """
            Creates a DataLoadingPipelineBuilder with its own pipeline builder.
        """
        pipeline_builder = self.sdc_builder.get_pipeline_builder()
        return DataLoadingPipelineHandler(self.sdc_executor, pipeline_builder, self.stage_name, self.cleanup)


class DataLoadingPipelineHandler:
    def __init__(self, sdc_executor: DataCollector, pipeline_builder: PipelineBuilder, stage_name: str,
                 cleanup: Callable):
        self.sdc_executor = sdc_executor
        self.pipeline_builder = pipeline_builder
        self.stage_name = stage_name
        self.cleanup = cleanup
        self.pipeline = None

    def build(self, environment=None):
        """
            Builds the pipeline.
        """
        self.pipeline = self.pipeline_builder.build()
        if environment is not None:
            self.pipeline.configure_for_environment(environment)
        return self

    def run(self, timeout_sec: int = 300):
        """
            Runs the given pipeline.
        """
        if self.pipeline is None:
            raise RuntimeError("Pipeline was not yet built, it cannot be run")
        self.cleanup(self.sdc_executor.remove_pipeline, self.pipeline)
        self.sdc_executor.add_pipeline(self.pipeline)
        self.cleanup(self.sdc_executor.stop_pipeline, self.pipeline)
        self.sdc_executor.start_pipeline(pipeline=self.pipeline).wait_for_finished(timeout_sec=timeout_sec)

    def benchmark(self, record_count: int = 2_000_000):
        """
            Benchmarks the pipeline. Needed in performance tests.
        """
        if self.pipeline is None:
            raise RuntimeError("Pipeline was not yet built, it cannot be run")
        self.cleanup(self.sdc_executor.remove_pipeline, self.pipeline)
        self.sdc_executor.benchmark_pipeline(self.pipeline, record_count=record_count)

    def get_pipeline_history(self) -> History:
        """
            Retrieves the pipeline history.
        """
        return self.sdc_executor.get_pipeline_history(self.pipeline)

    def dev_raw_data_source_origin(self, rows: list, data_format: str = 'JSON',
                                   stop_after_first_batch: bool = True) -> Stage:
        """
            Adds a 'Dev Raw Data Source' stage instance to the pipeline.
        """
        dev_raw_data_source = self.pipeline_builder.add_stage('Dev Raw Data Source')
        raw_data = '\n'.join(json.dumps(row) for row in rows)
        dev_raw_data_source.set_attributes(raw_data=raw_data,
                                           data_format=data_format,
                                           stop_after_first_batch=stop_after_first_batch)
        return dev_raw_data_source

    def dev_data_generator(self, records_to_be_generated: int = 1000, batch_size: int = 1000,
                           number_of_threads: int = 1, fields_to_generate: list = None) -> Stage:
        """
            Adds a 'Dev Data Generator' stage instance to the pipeline.
        """
        dev_data_generator = self.pipeline_builder.add_stage('Dev Data Generator')
        if fields_to_generate is None:
            fields_to_generate = [{"type": "LONG_SEQUENCE", "field": "id"},
                                  {"type": "STRING", "field": "name"}]
        dev_data_generator.set_attributes(
            records_to_be_generated=records_to_be_generated,
            batch_size=batch_size,
            number_of_threads=number_of_threads,
            fields_to_generate=fields_to_generate
        )
        return dev_data_generator

    def field_type_converter(self, field_type_converter_configs: list) -> Stage:
        """
            Adds a 'Field Type Converter' stage instance to the pipeline.
        """
        field_type_converter = self.pipeline_builder.add_stage('Field Type Converter', type='processor')
        field_type_converter.set_attributes(field_type_converter_configs=field_type_converter_configs)
        return field_type_converter

    def null_field_replacer(self, fields: list) -> Stage:
        """
            Adds a 'Field Replacer' stage instance to the pipeline.
        """
        field_replacer = self.pipeline_builder.add_stage('Field Replacer')
        field_replacer.replacement_rules = [{'setToNull': True, 'fields': f'/{field}'} for field in fields]
        return field_replacer

    def decimal_precision_expression_evaluator(self, decimal_fields: list, precision: int = 5, scale: int = 0) -> Stage:
        """
            Adds a 'Expression Evaluator' stage instance to the pipeline, which will add a default precision
            and scale header used by DECIMAL fields.
        """
        expression_evaluator = self.pipeline_builder.add_stage('Expression Evaluator')
        field_attribute_expressions = []
        for field in decimal_fields:
            field_attribute_expressions.append(
                {
                    'fieldToSet': f'/{field}',
                    'attributeToSet': 'precision',
                    'fieldAttributeExpression': str(precision)
                })
            field_attribute_expressions.append({
                'fieldToSet': f'/{field}',
                'attributeToSet': 'scale',
                'fieldAttributeExpression': str(scale)
            })
        expression_evaluator.set_attributes(field_attribute_expressions=field_attribute_expressions)
        return expression_evaluator

    def insert_operation_evaluator(self, primary_key_columns: list) -> Stage:
        """
            Adds a 'Expression Evaluator' stage instance to the pipeline.
            This is used in basic CDC tests, where all OPs are INSERTS (1).
        """
        expression_evaluator = self.pipeline_builder.add_stage('Expression Evaluator')
        expression_evaluator.set_attributes(
            header_attribute_expressions=[
                {'attributeToSet': SDC_OPERATION_TYPE, 'headerAttributeExpression': str(OPERATION_TYPE.get('INSERT'))},
                {'attributeToSet': PRIMARY_KEY_SPECIFICATION,
                 'headerAttributeExpression': json.dumps({key: {} for key in primary_key_columns})}])
        return expression_evaluator

    def expression_evaluator(self, header_attribute_expressions: list) -> Stage:
        """
            Adds a 'Expression Evaluator' stage instance to the pipeline.
        """
        expression_evaluator = self.pipeline_builder.add_stage('Expression Evaluator')
        expression_evaluator.set_attributes(header_attribute_expressions=header_attribute_expressions)
        return expression_evaluator

    def field_remover(self, fields: list) -> Stage:
        """
            Adds a 'Expression Evaluator' stage instance to the pipeline.
        """
        field_remover = self.pipeline_builder.add_stage('Field Remover')
        field_remover.set_attributes(fields=fields)
        return field_remover

    def wiretap(self) -> Stage:
        """
            Adds the DataLoading Destination stage instance to the pipeline and using attributes as the stage config.
        """
        wiretap = self.pipeline_builder.add_wiretap()
        return wiretap

    def add_benchmark_stages(self, number_of_threads: int, batch_size_in_recs: int) -> Stage:
        """
            Adds the Benchmark stages to the pipeline.
        """
        benchmark_stages = self.pipeline_builder.add_benchmark_stages()
        benchmark_stages.origin.set_attributes(number_of_threads=number_of_threads,
                                               batch_size_in_recs=batch_size_in_recs)
        return benchmark_stages

    def destination(self, attributes: dict) -> Stage:
        """
            Adds the DataLoading Destination stage instance to the pipeline and using attributes as the stage config.
        """
        destination = self.pipeline_builder.add_stage(name=self.stage_name)
        destination.set_attributes(**attributes)
        return destination


@pytest.fixture
def data_loading_pipeline_handler_creator(sdc_builder: DataCollector, sdc_executor: DataCollector, stage_name: str,
                                          cleanup: Callable) -> DataLoadingPipelineHandlerCreator:
    return DataLoadingPipelineHandlerCreator(sdc_builder, sdc_executor, stage_name, cleanup)


class CDCRecordCreator:
    def __init__(self, cdc_test_case: dict):
        self.setup_operations = cdc_test_case.get(SETUP_OPERATIONS)
        self.cdc_operations = cdc_test_case.get(CDC_OPERATIONS)
        self.primary_key_columns = cdc_test_case.get(PRIMARY_KEY_COLUMNS)
        self.primary_key_definition = json.dumps({key: {} for key in self.primary_key_columns})

    def cdc_field_remover_fields(self) -> list:
        """
            Generates the field remover configuration to set corresponding CDC Headers from the values
        """
        def prepend_slash(items: list):
            return [f'/{item}' for item in items]

        return prepend_slash(CDC_METADATA_RECORD_FIELDS)

    def cdc_header_expressions(self) -> list:
        """
            Generates the expression evaluator configuration to set corresponding CDC Headers
        """
        cdc_header_expressions = [
            {'attributeToSet': TABLE,
             'headerAttributeExpression': f"${{record:value('/{TABLE}')}}"},
            {'attributeToSet': SDC_OPERATION_TYPE,
             'headerAttributeExpression': f"${{record:value('/{OPERATION}')}}"},
            {'attributeToSet': PRIMARY_KEY_SPECIFICATION,
             'headerAttributeExpression': f"${{record:value('/{PRIMARY_KEY_DEFINITION}')}}"}
        ]

        for primary_key_column in self.primary_key_columns:
            cdc_header_expressions.append({
                'attributeToSet': f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.{primary_key_column}',
                'headerAttributeExpression':
                    f"${{record:value('/{PRIMARY_KEY_VALUE_UPDATE}/{PRIMARY_KEY_COLUMN_OLD_VALUE}.{primary_key_column}')}}"
            })
            cdc_header_expressions.append({
                'attributeToSet': f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.{primary_key_column}',
                'headerAttributeExpression':
                    f"${{record:value('/{PRIMARY_KEY_VALUE_UPDATE}/{PRIMARY_KEY_COLUMN_NEW_VALUE}.{primary_key_column}')}}"})

        return cdc_header_expressions

    def generate_primary_key_update_header(self, row_operations: dict) -> list:
        """
            Created the Primary Key updates in the form of {old_1: X, new_1: Y, old_2: Z, ...}
        """
        primary_key_update_header = []
        for row_operation in row_operations:
            row_primary_key_update_header = {}

            primary_key_previous_value = row_operation.get(PRIMARY_KEY_PREVIOUS_VALUE)
            if primary_key_previous_value is not None:
                for primary_key_column in primary_key_previous_value.keys():
                    current_operation_value = row_operation.get(ROW).get(primary_key_column)
                    row_primary_key_update_header.update({
                        f'{PRIMARY_KEY_COLUMN_OLD_VALUE}.{primary_key_column}': primary_key_previous_value.get(
                            primary_key_column),
                        f'{PRIMARY_KEY_COLUMN_NEW_VALUE}.{primary_key_column}': current_operation_value}
                    )

            # one header per record
            primary_key_update_header.append(row_primary_key_update_header)
        return primary_key_update_header

    def generate_setup_records(self, generate_metadata: bool = True) -> list:
        return self.generate_records(self.setup_operations, generate_metadata)

    def generate_cdc_records(self, generate_metadata: bool = True) -> list:
        return self.generate_records(self.cdc_operations, generate_metadata)

    def generate_records(self, row_operations: dict, generate_metadata: bool = True) -> list:
        """
            Generates records and headers from the operations defined.
        """
        records = []
        primary_key_values_updates = self.generate_primary_key_update_header(row_operations)
        for index in range(0, len(row_operations)):
            row_operation = row_operations[index]

            record = {**row_operation.get(ROW)}
            if generate_metadata:
                record.update({
                    TABLE: row_operation.get(TABLE),
                    PRIMARY_KEY_DEFINITION: self.primary_key_definition,
                    OPERATION: OPERATION_TYPE.get(row_operation.get(OPERATION)),
                    PRIMARY_KEY_VALUE_UPDATE: primary_key_values_updates[index]
                })

            records.append(record)
        return records


@pytest.fixture
def data_loading_cdc_record_creator(cdc_test_case: dict) -> CDCRecordCreator:
    return CDCRecordCreator(cdc_test_case)


class DataType:
    def __init__(self, sdc_type: str, database_type: str, sdc_data: any, database_data: any):
        self.database_type = database_type
        self.sdc_type = sdc_type
        self.sdc_data = sdc_data
        self.database_data = database_data

    def __repr__(self):
        return self.database_type


class ColumnDefinition:
    def __init__(self, name: str, type: str):
        self.name = name
        self.type = type


class DataTypeRecordCreator:
    def __init__(self, data_types: list):
        self.data_types = data_types
        (self.test_data,
         self.expected_data,
         self.expected_null_data,
         self.columns_definition,
         self.field_type_converter_configs) = self.generate_data_types_test_resources()

    def generate_data_types_test_resources(self) -> (dict, list, list, list, list):
        """Generates Data Types test cases records and configs"""
        test_data = {}
        expected_data = [{}]
        expected_null_data = [{}]
        columns_definition = []
        field_type_converter_configs = []

        for i in range(0, len(self.data_types)):
            column_name = f'{DATA_TYPE_TEST_COLUMN}_{i}'

            # Update test data with column name and corresponding data type
            test_data.update({column_name: self.data_types[i].sdc_data})

            # Append the data type to the list for expected data
            expected_data[0].update({column_name: self.data_types[i].database_data})

            # Append the None value we would have for columns if null was inserted
            expected_null_data[0].update({column_name: None})

            # Generate column definition for database table creation
            columns_definition.append(ColumnDefinition(column_name, self.data_types[i].database_type))

            # Define field type converter configs to use
            field_type_converter_configs.append({
                'fields': [f'/{column_name}'],
                'targetType': self.data_types[i].sdc_type,
                **self.get_extra_configs_for_type(self.data_types[i].sdc_type)
            })

        return test_data, expected_data, expected_null_data, columns_definition, field_type_converter_configs

    def get_extra_configs_for_type(self, converter_type: str) -> dict:
        if converter_type == 'TIME':
            return {
                'dataLocale': 'en,US',
                'dateFormat': 'OTHER',
                'otherDateFormat': 'hh:mm:ss',
                'zonedDateTimeFormat': 'ISO_OFFSET_DATE_TIME',
                'scale': 2
            }
        elif converter_type == 'DATETIME':
            return {
                'dataLocale': 'en,US',
                'dateFormat': 'YYYY_MM_DD_HH_MM_SS',
                'zonedDateTimeFormat': 'ISO_OFFSET_DATE_TIME',
                'scale': 2
            }
        else:
            return {}


@pytest.fixture
def data_loading_data_type_record_creator(data_types: list) -> DataTypeRecordCreator:
    return DataTypeRecordCreator(data_types)


class MultithreadRecordCreator:
    def __init__(self, number_of_threads: int, number_of_tables: int, records_to_be_generated: int):
        self.number_of_threads = number_of_threads
        self.number_of_tables = number_of_tables
        self.records_to_be_generated = records_to_be_generated
        self.table_expression_placeholder = f'STF_TABLE_%s_{get_random_string(string.ascii_uppercase, 10)}'
        self.table_names = [self.table_expression_placeholder % idx for idx in range(0, self.number_of_tables)]

    def generate_table_expression(self, long_sequence_column_name: str = None) -> str:
        """Returns the table expression used in the pipeline"""
        if long_sequence_column_name is None:
            long_sequence_column_name = 'id'
        return self.table_expression_placeholder % (
                    "${record:value('/" + long_sequence_column_name + "') % " + str(self.number_of_tables) + "}")

    def get_wiretap_data_per_table(self, wiretap: Stage, long_sequence_column_name: str = None) -> dict:
        """Retrieves all the fields from the wiretap and organises them in tables"""
        if long_sequence_column_name is None:
            long_sequence_column_name = 'id'
        wiretap_data_per_table = {table_name: [] for table_name in self.table_names}
        for record in wiretap.output_records:
            table_name = self.table_names[int(str(record.field[long_sequence_column_name])) % self.number_of_tables]
            wiretap_data_per_table[table_name].append(record.field)
        return wiretap_data_per_table

    def sort_by_column_values(self, rows: list, column_names: list = None) -> list:
        """Sorts columns by the given columns"""
        if column_names is None:
            column_names = ['id', 'name']
        return sorted(rows, key=lambda row: tuple([row[c] for c in column_names]))


class OnRecordErrorStatus:
    def __init__(self, error_message: str, input_records: int, output_records: int, error_records: int):
        self.error_message = error_message
        self.input_records = input_records
        self.output_records = output_records
        self.error_records = error_records


@pytest.fixture
def data_loading_multithread_record_creator(number_of_threads: int, number_of_tables: int,
                                            records_to_be_generated: int) -> MultithreadRecordCreator:
    return MultithreadRecordCreator(number_of_threads, number_of_tables, records_to_be_generated)


class OnRecordErrorHandler:
    def __init__(self, on_record_error: str, on_record_error_status: OnRecordErrorStatus):
        self.on_record_error = on_record_error
        self.on_record_error_status = on_record_error_status

    def run_and_handle_on_record_error(self, pipeline_handler: DataLoadingPipelineHandler):
        """Asserts that the On Record Error configuration was honored"""
        if self.on_record_error == 'STOP_PIPELINE':
            self.handle_stop_pipeline(pipeline_handler)
        elif self.on_record_error == 'DISCARD':
            self.handle_discard(pipeline_handler)
        elif self.on_record_error == 'TO_ERROR':
            self.handle_to_error(pipeline_handler)
        else:
            pytest.fail(f'Unrecognised ON RECORD ERROR value {self.on_record_error}')

        history = pipeline_handler.get_pipeline_history()

        output_records = history.latest.metrics.counter('pipeline.batchOutputRecords.counter').count
        assert output_records == self.on_record_error_status.output_records,\
            f'Pipeline has {output_records} output records, but it should have {self.on_record_error_status.output_records}'

        input_records = history.latest.metrics.counter('pipeline.batchInputRecords.counter').count
        assert input_records == self.on_record_error_status.input_records, \
            f'Pipeline has {input_records} input records, but it should have {self.on_record_error_status.input_records}'

        error_records = history.latest.metrics.counter('pipeline.batchErrorRecords.counter').count
        assert error_records == self.on_record_error_status.error_records,\
            f'Pipeline has {error_records} error records, but it should have {self.on_record_error_status.error_records}'

    def handle_stop_pipeline(self, pipeline_handler: DataLoadingPipelineHandler):
        """Asserts that the On Record Error STOP_PIPELINE was honored, and returns error received"""
        try:
            pipeline_handler.run()
            pytest.fail('Pipeline should have failed, but it did not. Test should never reach here')
        except (RunError, RunningError) as error:
            assert self.on_record_error_status.error_message in error.message,\
                f'Pipeline should have failed with "{self.on_record_error_status.error_message}", but failed with "{error.message}"'

    def handle_discard(self, pipeline_handler: DataLoadingPipelineHandler):
        """Asserts that the On Record Error DISCARD was honored """
        pipeline_handler.run()

    def handle_to_error(self, pipeline_handler: DataLoadingPipelineHandler):
        """Asserts that the On Record Error TO_ERROR was honored """
        pipeline_handler.run()


@pytest.fixture
def data_loading_on_record_error_handler(on_record_error: str, on_record_error_status: OnRecordErrorStatus) -> OnRecordErrorHandler:
    return OnRecordErrorHandler(on_record_error, on_record_error_status)
