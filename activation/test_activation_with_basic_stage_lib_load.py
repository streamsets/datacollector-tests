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

# This module starts SDC with only default stage libs like basic and runs pipelines for tests.

import logging

import pytest
from streamsets.testframework.markers import sdc_activation, sdc_min_version

from .utils.utils_activation import ACTIVATION_SUPPORT_SDC_MIN_VERSION, register_and_activate_sdc

# Skip all tests in this module if --sdc-version < 3.15.0
pytestmark = sdc_min_version(ACTIVATION_SUPPORT_SDC_MIN_VERSION)

logger = logging.getLogger(__name__)


@sdc_activation
@pytest.mark.parametrize('activate_sdc', [False, True])
def test_with_basic_stage_loaded(sdc_executor, activate_sdc, jms):
    """SDC is loaded only with default basic stages.

    The pipelines with basic lib stages should be able to run when SDC is not activated. activate_sdc = False
    The pipelines with basic lib stages should be able to run when SDC is activated. activate_sdc = true
    """
    if activate_sdc:
        register_and_activate_sdc(sdc_executor)
    _test_basic_stage(sdc_executor)


def _test_basic_stage(sdc_executor):
    pipeline_builder = sdc_executor.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.data_format = 'JSON'
    dev_raw_data_source.raw_data = '{"emp_id" :"123456"}'
    dev_raw_data_source.stop_after_first_batch = True
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> wiretap.destination

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    assert len(wiretap.output_records) == 1
    assert wiretap.output_records[0].field['emp_id'].value == '123456'
