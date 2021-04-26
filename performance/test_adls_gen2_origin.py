# Copyright 2019 StreamSets Inc.
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

from streamsets.testframework.markers import azure, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)

ADLS_GEN2_ORIGIN = 'com_streamsets_pipeline_stage_origin_datalake_gen2_DataLakeGen2DSource'


@azure('datalake')
@sdc_min_version('3.9.0')
def test_initial_scan(sdc_builder, sdc_executor, azure, benchmark):
    """Performance test for ADLS Gen2 origin.

    The test populates a random ADLS folder with a total of 1000 random files evenly distributed in 100
    subdirectories. Then it is measured the time a pipeline takes to recursively scan this directory tree and
    populate the internal queue of files found. This corresponds to the time the pipeline takes to reach the
    RUNNING state.

    Pipeline: adls_orig >> trash

    """
    directory_name = f'stf_perf_{get_random_string()}'
    fs = azure.datalake.file_system

    try:
        # Build the origin pipeline
        builder = sdc_builder.get_pipeline_builder()
        trash = builder.add_stage('Trash')
        adls_orig = builder.add_stage(name=ADLS_GEN2_ORIGIN)
        adls_orig.set_attributes(data_format='TEXT',
                                 files_directory=f'/{directory_name}',
                                 file_name_pattern='*',
                                 read_order='TIMESTAMP',
                                 process_subdirectories=True)
        adls_orig >> trash
        pipeline = builder.build().configure_for_environment(azure)

        # Populate the Azure directory employed in the test.
        _adls_populate_dir(fs, directory_name, num_dirs=100, num_files=10)

        def benchmark_pipeline(executor, pipeline):
            executor.add_pipeline(pipeline)
            executor.start_pipeline(pipeline).wait_for_status('RUNNING', timeout_sec=1200)
            executor.stop_pipeline(pipeline).wait_for_stopped()
            executor.remove_pipeline(pipeline)

        benchmark.pedantic(benchmark_pipeline, args=(sdc_executor, pipeline), rounds=5)

    finally:
        logger.info('Azure Data Lake directory %s and underlying files will be deleted.', directory_name)
        fs.rmdir(directory_name, recursive=True)


def _adls_populate_dir(adls_client, path, num_dirs=100, num_files=10):
    """Populate a directory with random subdirectories and files. If `path` does not exist, it is created by the
    function. Then it is populated with `num_dir` subdirectories, each one containing `num_files` random
    files. The content of each file is just its path.

    """
    adls_client.mkdir(path)

    for _ in range(num_dirs):
        folder_name = get_random_string(length=10)
        for _ in range(num_files):
            file_name = "{}.txt".format(get_random_string(length=10))
            file_path = os.path.join(path, folder_name, file_name)
            try:
                logger.info("Creating new file: %s...", file_path)
                _adls_create_file(adls_client, file_path, file_path)
            except Exception as e:
                logger.error("Could not create blob: %s: %s", file_path, str(e))


def _adls_create_file(adls_client, file_content, file_path):
    """Create a file in ADLS with the specified content.  If the file already exist, overrite content with
    `file_content`.

    """
    res1 = adls_client.touch(file_path)
    res2 = adls_client.write(file_path, file_content)
    if not (res1.response.ok and res2.response.ok):
        raise RuntimeError(f'Could not create file: {file_path}')
