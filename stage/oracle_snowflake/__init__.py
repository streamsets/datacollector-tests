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

import pytest

from typing import Callable

from streamsets.testframework.utils import get_random_string
from streamsets.testframework.markers import snowflake, database, sdc_min_version
from streamsets.testframework.sdc import DataCollector
from streamsets.testframework.sdc_models import PipelineBuilder, Stage, Pipeline
from stage.utils.connection_manager import (
    SqlConnectionManager,
)
from stage.utils.data_loading_stages import (
    OPERATION,
    OPERATION_TYPE,
    SETUP_OPERATIONS,
    CDC_OPERATIONS,
    ROW,
    TABLE,
    PRIMARY_KEY_COLUMNS,
    PRIMARY_KEY_PREVIOUS_VALUE
)

logger = logging.getLogger(__name__)

pytestmark = [snowflake, database('oracle', 'cdc'), sdc_min_version('5.11.0')]

ORACLE_STAGE_NAME = 'com_streamsets_pipeline_stage_origin_jdbc_cdc_description_OracleCDCDOrigin'
SNOWFLAKE_STAGE_NAME = 'com_streamsets_pipeline_stage_destination_snowflake_SnowflakeDTarget'

STORAGE_BUCKET_CONTAINER = 'snowflake'

INTERNAL_STAGING_LOCATION = ['INTERNAL']
EXTERNAL_STAGING_LOCATIONS = ['AWS_S3', 'BLOB_STORAGE', 'GCS']
ALL_STAGING_LOCATIONS = INTERNAL_STAGING_LOCATION + EXTERNAL_STAGING_LOCATIONS


@pytest.fixture
def oracle_stage_name() -> str:
    return ORACLE_STAGE_NAME


@pytest.fixture
def snowflake_stage_name() -> str:
    return SNOWFLAKE_STAGE_NAME


@pytest.fixture
def snowflake_sql_connection_manager(snowflake, cleanup: Callable) -> SqlConnectionManager:
    return SqlConnectionManager(snowflake.engine, cleanup)


@pytest.fixture
def database_sql_connection_manager(database, cleanup: Callable) -> SqlConnectionManager:
    return SqlConnectionManager(database.engine, cleanup)


def create_stage(snowflake, staging_location):
    stage_name = f'STF_STAGE_{get_random_string(string.ascii_uppercase, 5)}'
    storage_path = f'{STORAGE_BUCKET_CONTAINER}/{get_random_string(string.ascii_letters, 10)}'
    snowflake.create_stage(stage_name, storage_path, stage_location=staging_location)
    return stage_name


class RecordMerger:
    def __init__(self, database_sql_connection_manager: SqlConnectionManager, cdc_test_case: dict):
        self.database_sql_connection_manager = database_sql_connection_manager
        self.cdc_test_case = cdc_test_case

    def merge_records(self, tables: list) -> int:
        """
            Performs the operations in OPERATION for the records on the given table columns.
            When updating the value of a Primary Key, we use the PRIMARY_KEY_PREVIOUS_VALUE.
            Returns the number of records merged.
        """
        records = self.cdc_test_case.get(SETUP_OPERATIONS) + self.cdc_test_case.get(CDC_OPERATIONS)
        for record in records:

            operation = record.get(OPERATION)
            table = tables[record.get(TABLE) if record.get(TABLE) is not None else 0]
            where_clauses = [f'{primary_key} = {record.get(ROW).get(primary_key)}' for
                             primary_key in self.cdc_test_case.get(PRIMARY_KEY_COLUMNS)]

            if operation == 'INSERT':
                self.database_sql_connection_manager.insert(table, [record.get(ROW)])

            elif operation == 'DELETE':
                self.database_sql_connection_manager.delete(table, [record.get(ROW)], where_clauses)

            elif operation == 'UPDATE':
                if record.get(PRIMARY_KEY_PREVIOUS_VALUE) is not None:
                    where_clauses = [f'{primary_key} = {record.get(PRIMARY_KEY_PREVIOUS_VALUE).get(primary_key)}' for
                                     primary_key in self.cdc_test_case.get(PRIMARY_KEY_COLUMNS)]
                self.database_sql_connection_manager.update(table, [record.get(ROW)], where_clauses)

            elif operation in ('UPSERT', 'UNSUPPORTED', 'UNDELETE', 'REPLACE', 'MERGE', 'LOAD'):
                pytest.skip(f'Oracle CDC does not support the given operation type: {operation} for record {record}')

            else:
                pytest.fail(f'Unsupported operation type: {operation} for record {record}')

        return len(records)


@pytest.fixture
def record_merger(database_sql_connection_manager: SqlConnectionManager, cdc_test_case: dict) -> RecordMerger:
    return RecordMerger(database_sql_connection_manager, cdc_test_case)


class PipelineHandlerCreator:
    def __init__(self, sdc_builder: DataCollector, sdc_executor: DataCollector, oracle_stage_name: str,
                 snowflake_stage_name: str, cleanup: Callable):
        self.sdc_builder = sdc_builder
        self.sdc_executor = sdc_executor
        self.oracle_stage_name = oracle_stage_name
        self.snowflake_stage_name = snowflake_stage_name
        self.cleanup = cleanup

    def create(self):
        """
            Creates a PipelineBuilder with its own pipeline builder.
        """
        pipeline_builder = self.sdc_builder.get_pipeline_builder()
        return PipelineHandler(self.sdc_executor, pipeline_builder, self.oracle_stage_name,
                                          self.snowflake_stage_name, self.cleanup)


class PipelineHandler:
    def __init__(self, sdc_executor: DataCollector, pipeline_builder: PipelineBuilder, oracle_stage_name: str,
                 snowflake_stage_name: str, cleanup: Callable):
        self.sdc_executor = sdc_executor
        self.pipeline_builder = pipeline_builder
        self.oracle_stage_name = oracle_stage_name
        self.snowflake_stage_name = snowflake_stage_name
        self.cleanup = cleanup
        self.pipeline = None

    def build(self, *environments):
        """
            Builds the pipeline.
        """
        self.pipeline = self.pipeline_builder.build()
        [self.pipeline.configure_for_environment(environment) for environment in environments]
        return self

    def run(self):
        """
            Runs the given pipeline
        """
        self.start().wait().stop()

    def run_until(self, metric: str = 'input_record_count', count: int = 1, timeout_sec: int = 300):
        """
            Runs the given pipeline
        """
        self.start().wait(metric=metric, count=count, timeout_sec=timeout_sec).stop()

    def start(self):
        """
            Starts the given pipeline.
        """
        if self.pipeline is None:
            raise RuntimeError("Pipeline was not yet built, it cannot be run")
        self.cleanup(self.sdc_executor.remove_pipeline, self.pipeline)
        self.sdc_executor.add_pipeline(self.pipeline)
        self.cleanup(self.sdc_executor.stop_pipeline, self.pipeline)
        self.sdc_executor.start_pipeline(pipeline=self.pipeline)
        return self

    def wait(self, metric: str = 'input_record_count', count: int = 1, timeout_sec: int = 300):
        """
            Waits for the given pipeline.
        """
        self.sdc_executor.wait_for_pipeline_metric(pipeline=self.pipeline, metric=metric, value=count,
                                                   timeout_sec=timeout_sec)
        return self

    def stop(self):
        """
            Stops the given pipeline.
        """
        self.sdc_executor.stop_pipeline(pipeline=self.pipeline)
        return self

    def origin(self, attributes: dict) -> Stage:
        """
            Adds the Origin stage instance to the pipeline and using attributes as the stage config.
        """
        origin = self.pipeline_builder.add_stage(name=self.oracle_stage_name)
        origin.set_attributes(**attributes)
        return origin

    def destination(self, attributes: dict) -> Stage:
        """
            Adds the DataLoading Destination stage instance to the pipeline and using attributes as the stage config.
        """
        destination = self.pipeline_builder.add_stage(name=self.snowflake_stage_name)
        destination.set_attributes(**attributes)
        return destination


@pytest.fixture
def pipeline_handler_creator(sdc_builder: DataCollector, sdc_executor: DataCollector, oracle_stage_name: str,
                             snowflake_stage_name: str, cleanup: Callable) -> PipelineHandlerCreator:
    return PipelineHandlerCreator(sdc_builder, sdc_executor, oracle_stage_name, snowflake_stage_name, cleanup)
