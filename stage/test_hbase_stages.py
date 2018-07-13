# Copyright 2017 StreamSets Inc.
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

from streamsets.testframework.markers import cluster
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Specify a port for SDC RPC stages to use.
SDC_RPC_PORT = 20000


@cluster('cdh', 'hdp')
def test_hbase_destination(sdc_builder, sdc_executor, cluster):
    """Simple HBase destination test.
    dev_raw_data_source >> hbase
    """
    dumb_haiku = ['I see you driving',
                  'Round town with the girl I love',
                  "And I'm like haiku."]
    random_table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'TEXT'
    dev_raw_data_source.raw_data = '\n'.join(dumb_haiku)

    hbase = pipeline_builder.add_stage('HBase', type='destination')
    hbase.table_name = random_table_name
    hbase.row_key = '/text'
    hbase.fields = [dict(columnValue='/text',
                         columnStorageType='TEXT',
                         columnName='cf1:cq1')]

    dev_raw_data_source >> hbase
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
    sdc_executor.add_pipeline(pipeline)

    try:
        logger.info('Creating HBase table %s ...', random_table_name)
        cluster.hbase.client.create_table(name=random_table_name, families={'cf1': {}})

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        assert [record.value['value']['text']['value']
                for record in snapshot[dev_raw_data_source.instance_name].output] == dumb_haiku
    finally:
        logger.info('Deleting HBase table %s ...', random_table_name)
        cluster.hbase.client.delete_table(name=random_table_name, disable=True)


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
    table_name = get_random_string(string.ascii_letters, 10)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data)

    hbase_lookup = pipeline_builder.add_stage('HBase Lookup')
    hbase_lookup.set_attributes(lookup_parameters=lookup_parameters, table_name=table_name)

    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> hbase_lookup >> trash
    pipeline = pipeline_builder.build().configure_for_environment(cluster)
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

        snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
        sdc_executor.stop_pipeline(pipeline)

        assert [dict(name=record.value['value']['text']['value'],
                     first_edition=record.value['value']['founded']['value'])
                for record in snapshot[hbase_lookup.instance_name].output] == bike_races
    finally:
        logger.info('Deleting HBase table %s ...', table_name)
        cluster.hbase.client.delete_table(name=table_name, disable=True)
