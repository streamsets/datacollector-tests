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
import pyarrow.parquet as pq

from streamsets.testframework.markers import sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

@sdc_min_version('5.7.0')
def test_generate_parquet(sdc_builder, sdc_executor):
    """Basic test to check we are able to save records in a parquet file.

       raw data source >> schema generator >> LocalFS

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

    # schema generator
    schema_generator = pipeline_builder.add_stage('Schema Generator')
    schema_generator.set_attributes(schema_name="test_schema",
                                    header_attribute="avroSchema")

    # local FS
    local_fs = pipeline_builder.add_stage('Local FS', type='destination')
    local_fs.set_attributes(data_format='PARQUET',
                            directory_template=temp_dir)

    dev_raw_data_source >> schema_generator >> local_fs

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
