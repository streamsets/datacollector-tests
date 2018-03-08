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

"""A module to test various SDC stages of InfluxDB."""

import json
import logging
import string

from streamsets.testframework.markers import influxdb
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@influxdb
def test_influxdb_destination(sdc_builder, sdc_executor, influxdb):
    """Test for InfluxDB destination stage. The pipeline looks like:

        dev_raw_data_source >> influxdb_destination
    """
    client = influxdb.client

    measurement = get_random_string(string.ascii_letters, 10)
    raw_dict = [{'measurement': measurement, 'record': {'time':1439897881000, 'butterflies': 12, 'honeybees': 23,
                                                        'location': '1', 'scientist': 'langstroth'}},
                {'measurement': measurement, 'record': {'time':1439096081000, 'butterflies': 3, 'honeybees': 28,
                                                        'location': '1', 'scientist': 'perpetua'}},
                {'measurement': measurement, 'record': {'time':1439898883000, 'butterflies': 1, 'honeybees': 10,
                                                        'location': '2', 'scientist': 'langstroth'}},
                {'measurement': measurement, 'record': {'time':1439899885000, 'butterflies': 8, 'honeybees': 23,
                                                        'location': '2', 'scientist': 'perpetua'}}]
    raw_data = json.dumps(raw_dict)
    # Determine if database already exists or not. If it does not, then only have the pipeline create it.
    create_db = not any(database['name'] == influxdb.database for database in client.get_list_database())

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', json_content='ARRAY_OBJECTS', raw_data=raw_data)
    influxdb_destination = builder.add_stage('InfluxDB', type='destination')
    influxdb_destination.set_attributes(auto_create_database=create_db, record_mapping='CUSTOM',
                                        measurement_field='/measurement',
                                        time_field='/record/time',
                                        time_unit='MILLISECONDS',
                                        tag_fields=["/record/location", "/record/scientist"],
                                        value_fields=["/record/butterflies", "/record/honeybees"])

    dev_raw_data_source >> influxdb_destination
    pipeline = builder.build(title='InfluxDB Destination pipeline').configure_for_environment(influxdb)
    sdc_executor.add_pipeline(pipeline)

    try:
        if create_db:
            logger.info('Creating InfluxDB database %s with measurement %s ...', influxdb.database, measurement)
        else:
            logger.info('Creating InfluxDB measurement %s in the database %s ...', measurement, influxdb.database)

        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_output_records_count(len(raw_dict))
        sdc_executor.stop_pipeline(pipeline)

        # read data from InfluxDB and assert to what pipeline ingested
        raw_records = [item['record'] for item in raw_dict]
        raw_records_sorted = sorted(raw_records, key=lambda d: (d['location'], d['scientist']))

        result = client.query(f'select * from {measurement}', epoch='ms')
        result_records = list(result.get_points())
        result_records_sorted = sorted(result_records, key=lambda d: (d['location'], d['scientist']))

        assert raw_records_sorted == result_records_sorted
    finally:
        logger.info('Dropping InfluxDB measurement %s in the database %s ...', measurement, influxdb.database)
        influxdb.drop_measurement(measurement)
        if create_db:
            logger.info('Dropping InfluxDB database %s ...', influxdb.database)
            client.drop_database(influxdb.database)
