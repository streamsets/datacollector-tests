# Copyright 2020 StreamSets Inc.
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

import pytest


def test_pipeline_downgrade(sdc_executor):
    """Ensure that when user tries to downgrade pipeline, we issue a proper error message."""
    builder = sdc_executor.get_pipeline_builder()

    generator = builder.add_stage(label='Dev Data Generator')
    trash = builder.add_stage(label='Trash')
    generator >> trash
    pipeline = builder.build()
    # We manually alter the pipeline version to some really high number
    # TLKT-561: PipelineInfo doesn't seem to be exposed in the APIs
    pipeline._data['pipelineConfig']['info']['sdcVersion'] = '99.99.99'

    sdc_executor.add_pipeline(pipeline)

    with pytest.raises(Exception) as e:
        sdc_executor.validate_pipeline(pipeline)

    assert 'VALIDATION_0096' in e.value.issues
