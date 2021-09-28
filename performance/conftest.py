# Copyright 2021 StreamSets Inc.
"""
The classes and fixtures in this file are provided as examples for how loading and unloading of datasets can be
managed efficiently.  They may be merged into STF in the future.

TODO STF-1674
"""

import logging
import string
from collections import deque

import pytest
import sqlalchemy
from kafka.admin import KafkaAdminClient, NewTopic
from sqlalchemy.dialects.mssql import DATETIME2
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.SDC_JAVA_OPTS = '-Xmx8192m -Xms8192m'
        data_collector.sdc_properties['production.maxBatchSize'] = '100000'
    return hook


class DatabaseTable:
    """Configures a test table with a schema for a `dataset`, and optionally loads the table with test records."""
    def __init__(self, database, sdc_builder, sdc_executor, stage_type, dataset):
        self._database = database
        self._sdc_builder = sdc_builder
        self._sdc_executor = sdc_executor
        self._dataset = dataset
        self._stage_type = stage_type
        self._loaded_records = 0

        if self._stage_type == 'origin':
            self.name = dataset.origin_name
        elif self._stage_type == 'destination':
            self.name = dataset.destination_name
        else:
            raise ValueError(f'Invalid stage type: {self._stage_type}. Valid types are: origin, destination')

        self.table = None

    def _avro_to_sql_columns(self):
        columns = []

        for field in self._dataset.avro_schema['fields']:
            name = field['name']

            if name == self._dataset.primary_key:
                columns.append(sqlalchemy.Column(name, sqlalchemy.Integer, primary_key=True))
                continue

            if isinstance(field['type'][1], dict):
                field_type = field['type'][1]['logicalType']
            else:
                field_type = field['type'][1]

            if field_type in ['int', 'long']:
                columns.append(sqlalchemy.Column(name, sqlalchemy.Integer))
            elif field_type == 'string':
                columns.append(sqlalchemy.Column(name, sqlalchemy.String(255)))
            elif field_type == 'bool':
                columns.append(sqlalchemy.Column(name, sqlalchemy.BOOLEAN))
            elif field_type == 'timestamp-millis':
                if self._database.type == 'SQLServer':
                    columns.append(sqlalchemy.Column(name, DATETIME2))
                else:
                    columns.append(sqlalchemy.Column(name, sqlalchemy.TIMESTAMP))
            elif field_type == 'float':
                columns.append(sqlalchemy.Column(name, sqlalchemy.FLOAT))
        return columns

    def _create_table(self, run_stmt_before_create_table=None):
        table = sqlalchemy.Table(self.name,
                                 sqlalchemy.MetaData(),
                                 *self._avro_to_sql_columns())

        table.drop(self._database.engine, checkfirst=True)

        if run_stmt_before_create_table:
            self._database.engine.connect().execute(run_stmt_before_create_table)

        table.create(self._database.engine)
        return table

    def load_records(self, record_count, run_stmt_before_create_table=None, run_stmt_after_create_table=None):
        # This method is typically called from module-scoped fixtures.  It aims to reduce the number of times a
        # dataset is loaded within that scope.
        if record_count > self._dataset.num_records:
            raise ValueError(f'Requested origin table record count ({record_count}) exceeds records in'
                             f'dataset ({self._dataset.num_records}).')

        if record_count <= self._loaded_records and not run_stmt_after_create_table:
            return

        self.table = self._create_table(run_stmt_before_create_table)

        if run_stmt_after_create_table:
            self._database.engine.connect().execute(run_stmt_after_create_table)

        pipeline_builder = self._sdc_builder.get_pipeline_builder()
        benchmark_stages = pipeline_builder.add_benchmark_stages()
        benchmark_stages.origin.set_dataset(self._dataset)
        benchmark_stages.origin.set_attributes(number_of_threads=self._dataset.num_partitions)

        jdbc_producer = pipeline_builder.add_stage('JDBC Producer', type='destination')
        jdbc_producer.set_attributes(default_operation='INSERT', field_to_column_mapping=[], table_name=self.name)

        if self._database.type == 'MySQL':
            jdbc_producer.use_multi_row_operation = True
            jdbc_producer.statement_parameter_limit = 32768  # bind variable limit for older versions of the database
        elif self._database.type == 'Oracle':
            jdbc_producer.enclose_object_names = True
            jdbc_producer.use_multi_row_operation = False
        elif self._database.type == 'PostgreSQL':
            jdbc_producer.enclose_object_names = True
            jdbc_producer.statement_parameter_limit = 32768  # bind variable limit for older versions of the database
        elif self._database.type == 'SQLServer':
            jdbc_producer.use_multi_row_operation = False
            jdbc_producer.init_query = f'SET IDENTITY_INSERT dbo.{self.name} ON'

        benchmark_stages.origin >> jdbc_producer

        pipeline = (pipeline_builder.build(f'dataset loader for {self._dataset.origin_name}')
                                    .configure_for_environment(self._database))

        try:
            self._sdc_executor.add_pipeline(pipeline)
            self._sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(record_count,
                                                                                               timeout_sec=1800)
            self._loaded_records = record_count
        finally:
            try:
                self._sdc_executor.stop_pipeline(pipeline)
            except Exception as e:
                logger.warning('Unable to stop pipeline: %s', str(e))
            finally:
                self._sdc_executor.remove_pipeline(pipeline)

    def drop(self):
        if self.table is not None:
            logger.debug('Dropping table %s', self.name)
            self.table.drop(self._database.engine)
            self.table = None


class KafkaTopic:
    """Configures a test topic and optionally loads the topic with test records."""
    def __init__(self, cluster, sdc_builder, sdc_executor, stage_type, dataset, data_format='AVRO'):
        self._cluster = cluster
        self._sdc_builder = sdc_builder
        self._sdc_executor = sdc_executor
        self._dataset = dataset
        self._data_format = data_format
        self._stage_type = stage_type
        self._loaded_records = 0

        if self._stage_type == 'origin':
            self.name = dataset.origin_name
        elif self._stage_type == 'destination':
            self.name = dataset.destination_name
        else:
            raise ValueError(f'Invalid stage type: {self._stage_type}. Valid types are: origin, destination')

        self.topic = None

    def _create_topic(self):
        if self.name in self._cluster.kafka.consumer().topics():
            logger.warning('Asked to create a topic that already exists: %s. Deleting the topic first.', self.name)
            self.delete()
        logger.debug('Creating new topic %s', self.name)
        topic = NewTopic(name=self.name, num_partitions=self._dataset.num_partitions, replication_factor=1)
        admin_client = KafkaAdminClient(bootstrap_servers=self._cluster.kafka.brokers, request_timeout_ms=30000)
        admin_client.create_topics(new_topics=[topic], timeout_ms=60000)
        self.topic = topic

    def load_records(self, record_count):
        if record_count > self._dataset.num_records:
            raise ValueError(f'Requested origin topic record count ({record_count}) exceeds records in'
                             f'dataset ({self._dataset.num_records}).')

        if record_count <= self._loaded_records:
            return

        if self.topic is None:
            self._create_topic()

        pipeline_builder = self._sdc_builder.get_pipeline_builder()
        benchmark_stages = pipeline_builder.add_benchmark_stages()
        benchmark_stages.origin.set_dataset(self._dataset)
        benchmark_stages.origin.set_attributes(number_of_threads=self._dataset.num_partitions)

        kafka_producer = pipeline_builder.add_stage(name='com_streamsets_pipeline_stage_destination_kafka_KafkaDTarget',
                                                    type='destination',
                                                    library=self._cluster.kafka.standalone_stage_lib)
        kafka_producer.topic = self.name
        kafka_producer.set_attributes(topic=self.name, data_format=self._data_format)

        if self._data_format == 'AVRO':
            kafka_producer.avro_schema_location = 'HEADER'
        elif self._data_format == 'DELIMITED':
            kafka_producer.header_line = 'WITH_HEADER'

        if self._data_format == 'TEXT':
            data_generator = pipeline_builder.add_stage('Data Generator')
            data_generator.set_attributes(target_field='/text', output_type='STRING', data_format='JSON')

            benchmark_stages.origin >> data_generator >> kafka_producer
        else:
            benchmark_stages.origin >> kafka_producer

        pipeline = (pipeline_builder.build(f'dataset loader for {self._dataset.origin_name}')
                                    .configure_for_environment(self._cluster))

        try:
            self._sdc_executor.add_pipeline(pipeline)
            self._sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(record_count,
                                                                                               timeout_sec=1800)
            self._loaded_records = record_count
        finally:
            try:
                self._sdc_executor.stop_pipeline(pipeline)
            except Exception as e:
                logger.warning('Unable to stop pipeline: %s', str(e))
            finally:
                self._sdc_executor.remove_pipeline(pipeline)

    def delete(self):
        logger.debug('Deleting topic %s', self.name)
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=self._cluster.kafka.brokers, request_timeout_ms=300000)
            admin_client.delete_topics([self.name])
        except Exception as e:
            logger.warning('Unable to delete Kafka topic %s: %s', self.name, str(e))
        finally:
            self.topic = None


@pytest.fixture(scope='module')
def origin_table(database, sdc_builder, sdc_executor, datasets, keep_data):
    # This fixture is module-scoped so that data can be loaded once for all tests in a given module, then cleaned
    # up to minimize resource utilization for Docker-based STEs.
    # When using it in tests, try to use the same value for origin_table.load_records() for all tests in the module.
    table = None
    try:
        table = DatabaseTable(database, sdc_builder, sdc_executor, stage_type='origin', dataset=datasets.default)
        yield table
    finally:
        if not keep_data and table is not None:
            table.drop()


@pytest.fixture(scope='function')
def destination_table(database, sdc_builder, sdc_executor, datasets, keep_data):
    # This fixture is function-scoped so that the written data is cleaned up as quickly as possible as to minimize
    # resource utilization for Docker-based STEs.
    table = None
    try:
        table = DatabaseTable(database, sdc_builder, sdc_executor, stage_type='destination', dataset=datasets.default)
        yield table
    finally:
        if not keep_data and table is not None:
            table.drop()


@pytest.fixture(scope='module')
def origin_topic(cluster, sdc_builder, sdc_executor, datasets, keep_data):
    # This fixture is module-scoped so that data can be loaded once for all tests in a given module, then cleaned
    # up to minimize resource utilization for Docker-based STEs.
    # When using it in tests, try to use the same value for origin_topic.load_records() for all tests in the module.
    topic = None
    try:
        topic = KafkaTopic(cluster, sdc_builder, sdc_executor, stage_type='origin', dataset=datasets.default)
        yield topic
    finally:
        if not keep_data and topic is not None:
            topic.delete()


@pytest.fixture(scope='function')
def destination_topic(cluster, sdc_builder, sdc_executor, datasets, keep_data):
    # This fixture is function-scoped so that the written data is cleaned up as quickly as possible as to minimize
    # resource utilization for Docker-based STEs.
    topic = None
    try:
        topic = KafkaTopic(cluster, sdc_builder, sdc_executor, stage_type='destination', dataset=datasets.default)
        yield topic
    finally:
        if not keep_data and topic is not None:
            topic.delete()

@pytest.fixture(scope='module')
def elasticsearch_origin_index(elasticsearch, benchmark_args, keep_data):
    # This fixture is module-scoped so that data can be loaded once for all tests in a given module, then cleaned
    # up to minimize resource utilization for Docker-based STEs.
    index = benchmark_args.get('INDEX_NAME') or get_random_string(string.ascii_lowercase, 20)

    # There is currently a bug in the framework for pipelines that auto-stop (e.g. batch pipelines)
    # So we add 100k records to the source so that it won't automatically stop but will wait on the
    # framework to stop it.
    upper = benchmark_args.get('RECORD_COUNT', 5_000_000) + 100_000

    def generator():
        for i in range(1, upper):
            if i % 10_000 == 0:
                logger.info(f'Inserting document with id/number {i}')

            yield {
                "_index": index,
                "_type": "data",
                "_source": {"number": i}
            }

    logger.info('Creating index %s', index)
    elasticsearch.client.create_index(index)
    logger.info('Populating index %s', index)
    deque(elasticsearch.client.parallel_bulk(generator(), thread_count=10))
    logger.info('Refreshing index %s', index)
    elasticsearch.client.refresh_index(index)

    yield index

    if not keep_data:
        elasticsearch.client.delete_index(index)


@pytest.fixture(scope='function')
def elasticsearch_destination_index(elasticsearch, benchmark_args, keep_data):
    # This fixture is function-scoped so that the written data is cleaned up as quickly as possible as to minimize
    # resource utilization for Docker-based STEs.
    index = benchmark_args.get('INDEX_NAME') or get_random_string(string.ascii_lowercase, 20)
    elasticsearch.client.create_index(index)

    yield index

    if not keep_data:
        elasticsearch.client.delete_index(index)