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
import os

import pytest
from streamsets.testframework.markers import azure, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

pytestmark = [azure('datalake'), sdc_min_version('5.5.0')]

STAGE_NAME = 'com_streamsets_pipeline_stage_origin_client_datalake_DataLakeStorageGen2DSource'


@pytest.mark.parametrize('threads', [1, 5, 15])
def test_data_lake_origin(sdc_builder, sdc_executor, azure, threads, keep_data):
    """Benchmark ADLS Gen2 client origin loading speed"""

    directory_name = f'stf_perf_{get_random_string()}'
    fs = azure.datalake.file_system

    try:
        pipeline_builder = sdc_builder.get_pipeline_builder()
        benchmark_stages = pipeline_builder.add_benchmark_stages()
        azure_data_lake_origin = pipeline_builder.add_stage(name=STAGE_NAME)
        azure_data_lake_origin.set_attributes(data_format='TEXT',
                                              common_path=f'/{directory_name}',
                                              number_of_threads=threads)
        azure_data_lake_origin >> benchmark_stages.destination
        pipeline = pipeline_builder.build().configure_for_environment(azure)

        # Populate the Azure directory with 100 subdirectories with 10 files each.
        fs.mkdir(directory_name)

        for _ in range(100):
            folder_name = get_random_string(length=100)
            for _ in range(10):
                file_name = "{}.txt".format(get_random_string(length=100))
                file_path = os.path.join(directory_name, folder_name, file_name)
                try:
                    logger.debug("Creating new file: %s ...", file_path)
                    res1 = fs.touch(file_path)
                    res2 = fs.write(file_path, file_path)
                    if not (res1.response.ok and res2.response.ok):
                        raise RuntimeError(f'Could not create file: {file_path}')
                except Exception as e:
                    logger.error("Could not create file: %s: %s", file_path, str(e))

        sdc_executor.benchmark_pipeline(pipeline, record_count=1000)

    finally:
        if not keep_data:
            logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
            fs.rmdir(directory_name, recursive=True)
