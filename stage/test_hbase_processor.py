# Copyright 2018 StreamSets Inc.
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
import logging
import string

import pytest

from streamsets.testframework.markers import cluster
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Specify a port for SDC RPC stages to use.
SDC_RPC_PORT = 20000


@cluster('cdh', 'hdp')
def test_hbase_lookup_processor(sdc_builder, sdc_executor, cluster):
    """Simple HBase Lookup processor test.
    Pipeline will enrich records with the name of Grand Tours by adding a field containing the year
    of their first editions, which will come from an HBase table.
    dev_raw_data_source >> hbase_lookup >> trash
    """
    # Generate some silly data.
    bike_races = [dict(name='Tour de France', first_edition='1903'),
                  dict(name="Giro d'Italia", first_edition='1909'),
                  dict(name='Vuelta a Espana', first_edition='1935')]

    # Convert to raw data for the Dev Raw Data Source.
    raw_data = '\n'.join(bike_race['name'] for bike_race in bike_races)

    # Generate HBase Lookup's attributes.
    lookup_parameters = [dict(rowExpr="${record:value('/text')}",
                              columnExpr='info:first_edition',
                              outputFieldPath='/founded',
                              timestampExpr='')]

    # Get random table name to avoid collisions.
    table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Create Dev Raw Data Source stage.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data)

    # Create HBase Lookup processor.
    hbase_lookup = pipeline_builder.add_stage('HBase Lookup')
    hbase_lookup.set_attributes(lookup_parameters=lookup_parameters, table_name=table_name)

    # Create trash destination.
    trash = pipeline_builder.add_stage('Trash')

    # Build pipeline.
    dev_raw_data_source >> hbase_lookup >> trash
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating HBase table %s ...', table_name)
        cluster.hbase.client.create_table(name=table_name, families={'info': {}})

        # Use HappyBase's `Batch` instance to avoid unnecessary calls to HBase.
        batch = cluster.hbase.client.table(table_name).batch()
        for bike_race in bike_races:
            # Use of str.encode() below is because HBase (and HappyBase) speaks in byte arrays.
            batch.put(bike_race['name'].encode(), {b'info:first_edition': bike_race['first_edition'].encode()})
        batch.send()

        # Take a pipeline snapshot.
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        # Validate output.
        assert [dict(name=record.value2['text'],
                     first_edition=record.value2['founded'])
                for record in snapshot[hbase_lookup.instance_name].output] == bike_races

    finally:
        # Remove pipeline and delete HBase table.
        sdc_executor.remove_pipeline(pipeline)
        logger.info('Deleting HBase table %s ...', table_name)
        cluster.hbase.client.delete_table(name=table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_lookup_processor_empty_batch(sdc_builder, sdc_executor, cluster):
    """HBase Lookup processor test.
    pipeline will receive an empty batch, not errors would be shown
    dev_raw_data_source >> hbase_lookup >> trash
    """
    # Create empty input data.
    raw_data = ""

    # Generate HBase Lookup's attributes.
    lookup_parameters = [dict(rowExpr="${record:value('/text')}",
                              columnExpr='info:empty',
                              outputFieldPath='/founded',
                              timestampExpr='')]

    # Get random table name to avoid collisions.
    table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Create Dev Raw Data Source stage.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data)

    # Create HBase Lookup processor.
    hbase_lookup = pipeline_builder.add_stage('HBase Lookup')
    hbase_lookup.set_attributes(lookup_parameters=lookup_parameters, table_name=table_name)

    # Create trash destination.
    trash = pipeline_builder.add_stage('Trash')

    # Build pipeline.
    dev_raw_data_source >> hbase_lookup >> trash
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating HBase table %s ...', table_name)
        cluster.hbase.client.create_table(name=table_name, families={'info': {}})

        # Run preview.
        preview = sdc_executor.run_pipeline_preview(pipeline).preview
        assert preview is not None

        assert preview.issues.issues_count == 0

    finally:
        # Remove pipeline and delete HBase table.
        sdc_executor.remove_pipeline(pipeline)
        logger.info('Deleting HBase table %s ...', table_name)
        cluster.hbase.client.delete_table(name=table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_lookup_processor_invalid_url(sdc_builder, sdc_executor, cluster):
    """HBase Lookup processor test.
    pipeline will have an invalid url, not errors would be shown
    dev_raw_data_source >> hbase_lookup >> trash
    """
    # Generate some silly data.
    bike_races = [dict(name='Tour de France', first_edition='1903'),
                  dict(name="Giro d'Italia", first_edition='1909'),
                  dict(name='Vuelta a Espana', first_edition='1935')]

    # Convert to raw data for the Dev Raw Data Source.
    raw_data = '\n'.join(bike_race['name'] for bike_race in bike_races)

    # Generate HBase Lookup's attributes.
    lookup_parameters = [dict(rowExpr="${record:value('/text')}",
                              columnExpr='info:empty',
                              outputFieldPath='/founded',
                              timestampExpr='')]

    # Get random table name to avoid collisions.
    table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Create Dev Raw Data Source stage.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data)

    # Create HBase Lookup processor.
    hbase_lookup = pipeline_builder.add_stage('HBase Lookup')
    hbase_lookup.set_attributes(lookup_parameters=lookup_parameters, table_name=table_name)
    hbase_lookup.zookeeper_quorum = None

    # Create trash destination.
    trash = pipeline_builder.add_stage('Trash')

    # Build pipeline.
    dev_raw_data_source >> hbase_lookup >> trash
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating HBase table %s ...', table_name)
        cluster.hbase.client.create_table(name=table_name, families={'info': {}})

        # Use HappyBase's `Batch` instance to avoid unnecessary calls to HBase.
        batch = cluster.hbase.client.table(table_name).batch()
        for bike_race in bike_races:
            # Use of str.encode() below is because HBase (and HappyBase) speaks in byte arrays.
            batch.put(bike_race['name'].encode(), {b'info:first_edition': bike_race['first_edition'].encode()})
        batch.send()

        # Run preview.
        preview = sdc_executor.run_pipeline_preview(pipeline).preview
        assert preview is not None

        assert preview.issues.issues_count == 0

    finally:
        # Remove pipeline and delete HBase table.
        sdc_executor.remove_pipeline(pipeline)
        logger.info('Deleting HBase table %s ...', table_name)
        cluster.hbase.client.delete_table(name=table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_lookup_processor_invalid_table_name(sdc_builder, sdc_executor, cluster):
    """HBase Lookup processor test.
    pipeline will have an invalid table name, not errors would be shown
    dev_raw_data_source >> hbase_lookup >> trash
    """
    # Generate some silly data.
    bike_races = [dict(name='Tour de France', first_edition='1903'),
                  dict(name="Giro d'Italia", first_edition='1909'),
                  dict(name='Vuelta a Espana', first_edition='1935')]

    # Convert to raw data for the Dev Raw Data Source.
    raw_data = '\n'.join(bike_race['name'] for bike_race in bike_races)

    # Generate HBase Lookup's attributes.
    lookup_parameters = [dict(rowExpr="${record:value('/text')}",
                              columnExpr='info:empty',
                              outputFieldPath='/founded',
                              timestampExpr='')]

    # Get invalid table name.
    table_name = 'randomTable'

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Create Dev Raw Data Source stage.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data)

    # Create HBase Lookup processor.
    hbase_lookup = pipeline_builder.add_stage('HBase Lookup')
    hbase_lookup.set_attributes(lookup_parameters=lookup_parameters, table_name=table_name)
    hbase_lookup.zookeeper_quorum = None

    # Create trash destination.
    trash = pipeline_builder.add_stage('Trash')

    # Build pipeline.
    dev_raw_data_source >> hbase_lookup >> trash
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating HBase table %s ...', table_name)
        cluster.hbase.client.create_table(name=table_name, families={'info': {}})

        # Use HappyBase's `Batch` instance to avoid unnecessary calls to HBase.
        batch = cluster.hbase.client.table(table_name).batch()
        for bike_race in bike_races:
            # Use of str.encode() below is because HBase (and HappyBase) speaks in byte arrays.
            batch.put(bike_race['name'].encode(), {b'info:first_edition': bike_race['first_edition'].encode()})
        batch.send()

        # Run preview.
        preview = sdc_executor.run_pipeline_preview(pipeline).preview
        assert preview is not None

        assert preview.issues.issues_count == 0

    finally:
        # Remove pipeline and delete HBase table.
        sdc_executor.remove_pipeline(pipeline)
        logger.info('Deleting HBase table %s ...', table_name)
        cluster.hbase.client.delete_table(name=table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_empty_key_expression(sdc_builder, sdc_executor, cluster):
    """Check empty key expression in hbase lookup processor gives a configuration issue
    dev_raw_data_source >> hbase_lookup >> trash
    """
    # Generate some silly data.
    bike_races = [dict(name='Tour de France', first_edition='1903'),
                  dict(name="Giro d'Italia", first_edition='1909'),
                  dict(name='Vuelta a Espana', first_edition='1935')]

    # Convert to raw data for the Dev Raw Data Source.
    raw_data = '\n'.join(bike_race['name'] for bike_race in bike_races)

    # Generate HBase Lookup's attributes.
    lookup_parameters = [dict(rowExpr='',
                              columnExpr='info:first_edition',
                              outputFieldPath='/founded',
                              timestampExpr='')]

    # Get random table name to avoid collisions.
    table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Create Dev Raw Data Source stage.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data)

    # Create HBase Lookup processor.
    hbase_lookup = pipeline_builder.add_stage('HBase Lookup')
    hbase_lookup.set_attributes(lookup_parameters=lookup_parameters, table_name=table_name)

    # Create trash destination.
    trash = pipeline_builder.add_stage('Trash')

    # Build pipeline.
    dev_raw_data_source >> hbase_lookup >> trash
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating HBase table %s ...', table_name)
        cluster.hbase.client.create_table(name=table_name, families={'info': {}})

        issues = sdc_builder.api_client.export_pipeline(pipeline.id)['pipelineConfig']['issues']
        assert 0 == issues['issueCount']

        # Start pipeline.
        with pytest.raises(Exception) as e:
            sdc_executor.start_pipeline(pipeline)
            sdc_executor.stop_pipeline(pipeline)
        assert 'HBASE_35' in e.value.message
        assert 'HBASE_35 - Row key field has empty value' in e.value.message

    finally:
        # Remove pipeline and delete HBase table.
        sdc_executor.remove_pipeline(pipeline)
        logger.info('Deleting HBase table %s ...', table_name)
        cluster.hbase.client.delete_table(name=table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_get_empty_key_to_discard(sdc_builder, sdc_executor, cluster):
    """Check no error record when there is no key in the record and ignore row missing field is set to true
    dev_raw_data_source >> hbase_lookup >> trash
    """

    data = {'row_key': 11, 'columnField': 'cf1:column'}
    json_data = json.dumps(data)

    # Generate HBase Lookup's attributes.
    lookup_parameters = [dict(rowExpr="${record:value('/row_key')}",
                              columnExpr="${record:value('/columnField')}",
                              outputFieldPath='/output',
                              timestampExpr='')]

    # Get random table name to avoid collisions.
    table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Create Dev Raw Data Source stage.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = json_data

    # Create HBase Lookup processor.
    hbase_lookup = pipeline_builder.add_stage('HBase Lookup')
    hbase_lookup.set_attributes(lookup_parameters=lookup_parameters, table_name=table_name, on_record_error='TO_ERROR')

    # Create trash destination.
    trash = pipeline_builder.add_stage('Trash')

    # Build pipeline.
    dev_raw_data_source >> hbase_lookup >> trash
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating HBase table %s ...', table_name)
        cluster.hbase.client.create_table(name=table_name, families={'cf1': {}})

        # Take a pipeline snapshot.
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        scan = cluster.hbase.client.table(table_name).scan()

        assert 0 == len(list(scan))

        stage = snapshot[hbase_lookup.instance_name]
        logger.info('Error records %s ...', stage.error_records)

        assert len(stage.error_records) == 0

    finally:
        # Remove pipeline and delete HBase table.
        sdc_executor.remove_pipeline(pipeline)
        logger.info('Deleting HBase table %s ...', table_name)
        cluster.hbase.client.delete_table(name=table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_get_empty_key_to_error(sdc_builder, sdc_executor, cluster):
    """Check record is sent to error when there is no key in the record and ignore row missing field is set to false
    dev_raw_data_source >> hbase_lookup >> trash
    """

    data = {'columnField': 'cf1:column'}
    json_data = json.dumps(data)

    # Generate HBase Lookup's attributes.
    lookup_parameters = [dict(rowExpr="${record:value('/row_key')}",
                              columnExpr="${record:value('/columnField')}",
                              outputFieldPath='/output',
                              timestampExpr='')]

    # Get random table name to avoid collisions.
    table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Create Dev Raw Data Source stage.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = json_data

    # Create HBase Lookup processor.
    hbase_lookup = pipeline_builder.add_stage('HBase Lookup')
    hbase_lookup.set_attributes(lookup_parameters=lookup_parameters, table_name=table_name, on_record_error='TO_ERROR',
                                ignore_row_missing_field=False)

    # Create trash destination.
    trash = pipeline_builder.add_stage('Trash')

    # Build pipeline.
    dev_raw_data_source >> hbase_lookup >> trash
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating HBase table %s ...', table_name)
        cluster.hbase.client.create_table(name=table_name, families={'cf1': {}})

        # Take a pipeline snapshot.
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        scan = cluster.hbase.client.table(table_name).scan()

        assert 0 == len(list(scan))

        stage = snapshot[hbase_lookup.instance_name]
        logger.info('Error records %s ...', stage.error_records)

        assert len(stage.error_records) == 1

    finally:
        # Remove pipeline and delete HBase table.
        sdc_executor.remove_pipeline(pipeline)
        logger.info('Deleting HBase table %s ...', table_name)
        cluster.hbase.client.delete_table(name=table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_lookup_processor_invalid_column_family(sdc_builder, sdc_executor, cluster):
    """HBase Lookup processor test.
    pipeline will have an invalid column family, HBase_37 error expected ()
    dev_raw_data_source >> hbase_lookup >> trash
    """
    # Generate some silly data.
    bike_races = [dict(name='Tour de France', first_edition='1903'),
                  dict(name="Giro d'Italia", first_edition='1909'),
                  dict(name='Vuelta a Espana', first_edition='1935')]

    # Convert to raw data for the Dev Raw Data Source.
    raw_data = '\n'.join(bike_race['name'] for bike_race in bike_races)

    # Generate HBase Lookup's attributes.
    lookup_parameters = [
        dict(rowExpr="${record:value('/text')}", columnExpr='info:first_edition', outputFieldPath='/founded',
             timestampExpr=''),
        dict(rowExpr="${record:value('/text')}", columnExpr='invalid:column', outputFieldPath='/founded',
             timestampExpr='')]

    # Get random table name to avoid collisions.
    table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Create Dev Raw Data Source stage.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data, )

    # Create HBase Lookup processor.
    hbase_lookup = pipeline_builder.add_stage('HBase Lookup')
    hbase_lookup.set_attributes(on_record_error='TO_ERROR', lookup_parameters=lookup_parameters, table_name=table_name)

    # Create trash destination.
    trash = pipeline_builder.add_stage('Trash')

    # Build pipeline.
    dev_raw_data_source >> hbase_lookup >> trash
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating HBase table %s ...', table_name)
        cluster.hbase.client.create_table(name=table_name, families={'info': {}})

        # Start pipeline.
        with pytest.raises(Exception) as e:
            sdc_executor.start_pipeline(pipeline)
            sdc_executor.stop_pipeline(pipeline)
        assert 'HBASE_36' in e.value.message

    finally:
        # Remove pipeline and delete HBase table.
        sdc_executor.remove_pipeline(pipeline)
        logger.info('Deleting HBase table %s ...', table_name)
        cluster.hbase.client.delete_table(name=table_name, disable=True)


@cluster('cdh', 'hdp')
def test_hbase_lookup_processor_get_row(sdc_builder, sdc_executor, cluster):
    """HBase Lookup processor test.
    pipeline will be poroperly configured, will get the expected rows
    dev_raw_data_source >> hbase_lookup >> trash
    """
    # Generate some silly data.
    bike_races = [dict(name='Tour de France', first_edition='1903'),
                  dict(name='Giro d Italia', first_edition='1909'),
                  dict(name='Vuelta a Espana', first_edition='1935')]

    expected = [(b'Giro d Italia', {b'info:first_edition': b'1909'}),
                (b'Tour de France', {b'info:first_edition': b'1903'}),
                (b'Vuelta a Espana', {b'info:first_edition': b'1935'})]

    # Convert to raw data for the Dev Raw Data Source.
    raw_data = '\n'.join(bike_race['name'] for bike_race in bike_races)

    # Generate HBase Lookup's attributes.
    lookup_parameters = [dict(rowExpr="${record:value('/text')}",
                              columnExpr='info:first_edition',
                              outputFieldPath='/founded',
                              timestampExpr='')]

    # Get random table name to avoid collisions.
    table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    # Create Dev Raw Data Source stage.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data)

    # Create HBase Lookup processor.
    hbase_lookup = pipeline_builder.add_stage('HBase Lookup')
    hbase_lookup.set_attributes(lookup_parameters=lookup_parameters, table_name=table_name)

    # Create trash destination.
    trash = pipeline_builder.add_stage('Trash')

    # Build pipeline.
    dev_raw_data_source >> hbase_lookup >> trash
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating HBase table %s ...', table_name)
        cluster.hbase.client.create_table(name=table_name, families={'info': {}})

        # Use HappyBase's `Batch` instance to avoid unnecessary calls to HBase.
        batch = cluster.hbase.client.table(table_name).batch()
        for bike_race in bike_races:
            # Use of str.encode() below is because HBase (and HappyBase) speaks in byte arrays.
            batch.put(bike_race['name'].encode(), {b'info:first_edition': bike_race['first_edition'].encode()})
        batch.send()

        # Take a pipeline snapshot.
        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        # Validate output.
        assert [dict(name=record.value2['text'],
                     first_edition=record.value2['founded'])
                for record in snapshot[hbase_lookup.instance_name].output] == bike_races

        # Validate output.
        result_list = list(cluster.hbase.client.table(table_name).scan())
        assert result_list == expected

    finally:
        # Remove pipeline and delete HBase table.
        sdc_executor.remove_pipeline(pipeline)
        logger.info('Deleting HBase table %s ...', table_name)
        cluster.hbase.client.delete_table(name=table_name, disable=True)
