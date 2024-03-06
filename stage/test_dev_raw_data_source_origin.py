# Copyright 2024 StreamSets Inc.
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

from streamsets.sdk.exceptions import ValidationError
from streamsets.testframework.markers import sdc_min_version


@sdc_min_version('5.10.0')
def test_invalid_raw_data(sdc_builder, sdc_executor):
    """
    Verify that error SERVICE_ERROR_001 is thrown when Raw Data field is invalid.

    The pipeline looks like:
      dev_raw_data_source >> trash
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()

    # dev raw data
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    raw_data = '{"field1": "value1", "field2": "value2", "field3":}'   # invalid json
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=raw_data,
                                       on_record_error='STOP_PIPELINE',
                                       stop_after_first_batch=True)

    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> trash

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    with pytest.raises(ValidationError) as error:
        sdc_executor.validate_pipeline(pipeline)
    assert error.value.issues['issueCount'] == 1, "Wrong number of issues found"
    assert 'SERVICE_ERROR_001' in error.value.issues['stageIssues']["DevRawDataSource_01"][0]['message'], \
        "Expected error not found"
