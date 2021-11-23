# Copyright 2021 StreamSets Inc.
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
import pytest
from os.path import dirname, join, realpath
from pathlib import Path
from streamsets.testframework.markers import sdc_min_version
from streamsets.sdk.exceptions import InternalServerError

STANDALONE_PIPELINE_PATH = Path(f'{dirname(__file__)}/resources/sdc-pipeline.json')
BATCH_PIPELINE_PATH = Path(f'{dirname(__file__)}/resources/tx-pipeline.json')


@sdc_min_version('4.4.0')
def test_import_standalone_pipeline(sdc_builder):
    """Test importing allowed pipeline execution mode."""
    with open(STANDALONE_PIPELINE_PATH) as f:
        imported_pipeline_json = json.loads(f.read())

    pipeline_builder = sdc_builder.get_pipeline_builder()
    imported_pipeline = pipeline_builder.import_pipeline(pipeline=imported_pipeline_json).build()
    try:
        sdc_builder.add_pipeline(imported_pipeline)
    except InternalServerError as error:
        pytest.fail(f'Should not reach here, pipeline should have been imported correctly. Error: {error.text}')


@sdc_min_version('4.4.0')
def test_import_batch_pipeline(sdc_builder):
    """Test importing not allowed pipeline execution mode."""
    with open(BATCH_PIPELINE_PATH) as f:
        imported_pipeline_json = json.loads(f.read())

    pipeline_builder = sdc_builder.get_pipeline_builder()
    imported_pipeline = pipeline_builder.import_pipeline(pipeline=imported_pipeline_json).build()
    try:
        sdc_builder.add_pipeline(imported_pipeline)
        pytest.fail(f'Should not reach here, pipeline should not have been imported correctly.')
    except InternalServerError as error:
        assert "Pipeline execution mode 'Batch' is not valid for the current engine 'sdc'" in error.text, \
            'The pipeline should not have been imported correctly as it is in mode \'Batch\''
