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
from string import ascii_letters

import pytest
from streamsets.testframework.markers import gcp, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.category('nonstandard')]

DEFAULT_COLUMN_FAMILY_NAME = 'cf'  # for Google Bigtable


@gcp
@sdc_min_version('2.7.0.0')
def test_google_bigtable_destination(sdc_builder, sdc_executor, gcp):
    """
    Send text to Google bigtable from Dev Raw Data Source and
    confirm that Google bigtable successfully reads them using happybase connection from gcp.

    The pipeline looks like:
        dev_raw_data_source >> google_bigtable
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    # Data with 3 distinct TransactionIDs
    data = ['TransactionID,Type,UserID', '0003420301,01,1001', '0003420302,02,1002', '0003420303,03,1003',
            '0003420301,04,1004', '0003420302,05,1005', '0003420303,06,1006']
    dev_raw_data_source.set_attributes(data_format='DELIMITED',
                                       header_line='WITH_HEADER',
                                       raw_data='\n'.join(data))

    table_name = get_random_string(ascii_letters, 5)
    google_bigtable = pipeline_builder.add_stage('Google Bigtable', type='destination')
    # Field paths, name of columns, storage types
    fields_list = [{'source': '/TransactionID', 'storageType': 'TEXT', 'column': 'TransactionID'},
                   {'source': '/Type', 'storageType': 'TEXT', 'column': 'Type'},
                   {'source': '/UserID', 'storageType': 'TEXT', 'column': 'UserID'}]
    google_bigtable.set_attributes(create_table_and_column_families=True,
                                   default_column_family_name=DEFAULT_COLUMN_FAMILY_NAME,
                                   explicit_column_family_mapping=False,
                                   fields=fields_list,
                                   row_key='/TransactionID',
                                   table_name=table_name)

    dev_raw_data_source >> google_bigtable
    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)
    pipeline.rate_limit = 1

    instance = gcp.bigtable_instance
    table = instance.table(table_name)

    try:
        # Produce Google Bigtable records using pipeline.
        logger.info('Starting Big table pipeline and waiting for it to produce records ...')
        sdc_executor.start_pipeline(pipeline).wait_for_pipeline_batch_count(1)

        logger.info('Stopping Big table pipeline and getting the count of records produced in total ...')
        sdc_executor.stop_pipeline(pipeline)

        logger.info('Reading contents from table %s ...', table_name)
        partial_rows = table.read_rows()
        partial_rows.consume_all()

        read_data = [(f'{row.cells["cf"]["TransactionID".encode()][0].value.decode()},'
                      f'{row.cells["cf"]["Type".encode()][0].value.decode()},'
                      f'{row.cells["cf"]["UserID".encode()][0].value.decode()}')
                     for row_key, row in partial_rows.rows.items()]

        logger.debug('read_data = {}'.format(read_data))

        # Verify: Note we expect only 3 rows since there are only 3 distinct TransactionIDs in data.
        # The result expected is the latest inserted data which are the last 3 rows.
        # Reason is in Google Bigtable, each row is indexed by a single row key resulting in
        # each row/column intersection can contain multiple cells at different timestamps,
        # providing a record of how the stored data has been altered over time.
        assert data[4:] == read_data
    finally:
        table.delete()


@gcp
@sdc_min_version('2.7.2.0')
def test_google_bigtable_destination_multiple_types(sdc_builder, sdc_executor, gcp):
    """Simple big table destination test with INSERT operation.
    The pipeline inserts 1000 records of multiple types.
    The pipeline should look like:
        dev_data_generator >> record_deduplicator >> google_bigquery, wiretap
        record_deduplicator >> trash

    record_deduplicator is added to avoid duplications

    """

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_data_generator = pipeline_builder.add_stage('Dev Data Generator')
    dev_data_generator.fields_to_generate = [
        {'field': 'field1', 'type': 'STRING'},
        {'field': 'field2', 'type': 'INTEGER'}
    ]
    batch_size = 1000
    dev_data_generator.set_attributes(delay_between_batches=3000, batch_size=batch_size)

    table_name = get_random_string(ascii_letters, 5)

    google_bigtable = pipeline_builder.add_stage('Google Bigtable', type='destination')
    # Field paths, name of columns, storage types
    fields_list = [{'source': '/field1', 'storageType': 'TEXT', 'column': 'field1'},
                   {'source': '/field2', 'storageType': 'BINARY', 'column': 'field2'}]

    google_bigtable.set_attributes(create_table_and_column_families=True,
                                   default_column_family_name=DEFAULT_COLUMN_FAMILY_NAME,
                                   explicit_column_family_mapping=False,
                                   fields=fields_list,
                                   row_key='/field1',
                                   table_name=table_name)

    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    record_deduplicator.set_attributes(compare='SPECIFIED_FIELDS',
                                       fields_to_compare=['/field1'])

    wiretap = pipeline_builder.add_wiretap()
    trash = pipeline_builder.add_stage('Trash')

    dev_data_generator >> record_deduplicator >> [google_bigtable, wiretap.destination]
    record_deduplicator >> trash

    pipeline = pipeline_builder.build().configure_for_environment(gcp)
    sdc_executor.add_pipeline(pipeline)
    pipeline.rate_limit = 1

    instance = gcp.bigtable_instance
    table = instance.table(table_name)

    try:
        # Produce Google Bigtable records using pipeline.
        logger.info('Starting Big table pipeline and waiting for it to produce records ...')
        sdc_executor.start_pipeline(pipeline, timeout_sec=240)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'input_record_count', batch_size)

        logger.info('Stopping Big table Destination pipeline and getting the count of records produced in total ...')
        sdc_executor.stop_pipeline(pipeline)

        logger.info('Reading contents from table %s ...', table_name)
        partial_rows = table.read_rows()
        partial_rows.consume_all()

        # https://cloud.google.com/bigtable/docs/overview#data-types Bigtable stores strings as bytechar arrays
        # and integers are converted to signed bytes.
        read_data = [(row.cells["cf"]["field1".encode()][0].value.decode(),
                      int.from_bytes(row.cells["cf"]["field2".encode()][0].value, "big", signed=True))
                     for row_key, row in partial_rows.rows.items()]

        # Verify no errors were generated
        assert len(wiretap.error_records) == 0

        # Verify quantity of records in both destinations
        wiretap_data = [(rec.field['field1'],rec.field['field2'])
                         for rec in wiretap.output_records]
        assert len(read_data) == len(wiretap_data)
        assert all([element in read_data for element in wiretap_data])

    finally:
        table.delete()
