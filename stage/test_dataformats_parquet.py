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

import logging
import pytest
import json
import tempfile
import base64
import os
import pyarrow as pa
import pyarrow.parquet as pq

from streamsets.testframework.markers import sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

@sdc_min_version('5.7.0')
@pytest.mark.parametrize('parquet_schema_location', ['HEADER', 'INLINE'])
def test_generate_parquet(sdc_builder, sdc_executor, parquet_schema_location):
    """Basic test to check we are able to save records in a parquet file.

       raw data source [>> schema generator] >> LocalFS

       We use pyarrow.parquet to check parquet file is properly formatted
    """

    temp_dir = sdc_executor.execute_shell(f'mktemp -d').stdout.rstrip()
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # create raw data
    num_records = 1000
    data =[]
    for id in range(num_records):
       data.append({"id": id, "text": get_random_string()})

    # dev raw data
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    raw_data = '\n'.join(json.dumps(row) for row in data)
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       stop_after_first_batch=True)
    stage = dev_raw_data_source

    if parquet_schema_location == 'HEADER':
        # schema generator
        schema_generator = pipeline_builder.add_stage('Schema Generator')
        schema_generator.set_attributes(schema_name="test_schema",
                                        schema_type="PARQUET")
        dev_raw_data_source >> schema_generator
        stage = schema_generator

    # local FS
    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='PARQUET',
                            parquet_schema_location=parquet_schema_location,
                            directory_template=temp_dir)
    if parquet_schema_location == 'INLINE':
        local_fs.set_attributes(parquet_schema='message test_schema { required int32 id; required binary text (UTF8); }')

    stage >> local_fs

    pipeline = pipeline_builder.build()

    try:
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        with tempfile.NamedTemporaryFile() as tmp:
             base64_parquet = sdc_executor.execute_shell(f'cat {temp_dir}/* | openssl base64 -A').stdout
             tmp.write(base64.b64decode(base64_parquet))
             tmp.flush()
             parquet = pq.read_table(tmp)

        assert parquet.num_rows == len(data), 'Wrong number of records!'
        assert parquet.num_columns == len(data[0].keys()), 'Wrong number of fields!'
        for parquet_record, record in zip(parquet.to_pylist(), data):
            assert parquet_record == record, f'Wrong record found: "{parquet_record}"'

    finally:
        sdc_executor.execute_shell(f'rm -rf {temp_dir}')


@sdc_min_version('5.7.0')
def test_parse_parquet(sdc_builder, sdc_executor):
    """Basic test to check we are able to read records from a parquet file.

       Directory >> Wiretap

       We use pyarrow.parquet to write a parquet file
    """

    temp_dir = sdc_executor.execute_shell(f'mktemp -d').stdout.rstrip()
    temp_prefix = get_random_string()
    temp_base64 = os.path.join(temp_dir, temp_prefix + ".base64")
    temp_parquet = os.path.join(temp_dir, temp_prefix + ".parquet")
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # create raw data
    num_records = 1000
    data = []
    for id in range(num_records):
        data.append({"id": id, "text": get_random_string()})

    # build parquet data
    schema = pa.schema([('id', pa.uint32()), ('text', pa.string())])
    batch = pa.RecordBatch.from_arrays(
        [pa.array([record[key] for record in data]) for key in data[0].keys()],
        names=schema.names
    )
    table = pa.Table.from_batches([batch])

    # write parquet data to sdc executor
    with tempfile.NamedTemporaryFile(mode='r+b') as fd:
        pq.write_table(table, fd.name)
        fd.flush()
        fd.seek(0)
        data_base64 = base64.b64encode(fd.read())
    data_str = data_base64.decode()
    sdc_executor.write_file(temp_base64, data_str)
    sdc_executor.execute_shell(f'base64 --decode < {temp_base64} > {temp_parquet}')

    source = pipeline_builder.add_stage('Directory')
    source.set_attributes(files_directory=temp_dir,
                          file_name_pattern="*.parquet",
                          data_format="PARQUET")

    wiretap = pipeline_builder.add_wiretap()

    source >> wiretap.destination
    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    try:
        sdc_executor.start_pipeline(pipeline)
        sdc_executor.wait_for_pipeline_metric(pipeline, 'output_record_count', num_records, timeout_sec=60)
        output_records = wiretap.output_records
        assert len(output_records) == len(data), 'Wrong number of records!'
        for out_record, record in zip(output_records, data):
            assert out_record.field == record, 'Wrong record!'

    finally:
        if pipeline and sdc_executor.get_pipeline_status(pipeline).response.json().get('status') == 'RUNNING':
            sdc_executor.stop_pipeline(pipeline)
        sdc_executor.execute_shell(f'rm -rf {temp_dir}')

