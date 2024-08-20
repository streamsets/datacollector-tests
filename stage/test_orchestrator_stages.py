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

import logging
import pytest
import string
import uuid

from streamsets.sdk.exceptions import ValidationError
from streamsets.sdk.utils import get_random_string
from streamsets.testframework.markers import sdc_min_version
from streamsets.testframework.utils import Version

logger = logging.getLogger(__name__)


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        # Add the orchestrator library, but only for version 3.11.0 and higher, on older versions the test itself
        # will be properly skipped due to sdc_min_version annotation.
        if Version(data_collector.version) >= Version("3.11.0"):
            data_collector.add_stage_lib('streamsets-datacollector-orchestrator-lib')

    return hook


@sdc_min_version('3.11.0')
def test_cron_scheduler_origin(sdc_builder, sdc_executor):
    """Test Cron Scheduler Origin. The pipeline would look like:

        cron_scheduler_source >> wiretap

    With Cron Expression "0/2 * * 1/1 * ? *", Cron Scheduler Origin should generate record with timestamp(DateTime)
    filed every two seconds.

    Also, testing multiple pipelines with Cron Scheduler origin works without any issue.
    """

    pipeline1, wiretap1 = _create_cron_pipeline(sdc_builder, 'Cron Scheduler Sample Pipeline1')
    sdc_executor.add_pipeline(pipeline1)
    sdc_executor.validate_pipeline(pipeline1)

    pipeline2, wiretap2 = _create_cron_pipeline(sdc_builder, 'Cron Scheduler Sample Pipeline2')
    sdc_executor.add_pipeline(pipeline2)
    sdc_executor.validate_pipeline(pipeline2)

    sdc_executor.start_pipeline(pipeline1)
    sdc_executor.wait_for_pipeline_metric(pipeline1, 'data_batch_count', 1)
    sdc_executor.stop_pipeline(pipeline1)

    sdc_executor.start_pipeline(pipeline2)
    sdc_executor.wait_for_pipeline_metric(pipeline2, 'data_batch_count', 1)
    sdc_executor.stop_pipeline(pipeline2)

    _validate_cron_scheduler_output(wiretap1)
    _validate_cron_scheduler_output(wiretap2)


def _create_cron_pipeline(sdc_builder, title):
    pipeline_builder = sdc_builder.get_pipeline_builder()
    cron_scheduler_source = pipeline_builder.add_stage('Cron Scheduler')
    cron_scheduler_source.cron_schedule = '0/2 * * 1/1 * ? *'
    wiretap = pipeline_builder.add_wiretap()
    cron_scheduler_source >> wiretap.destination
    return pipeline_builder.build(title), wiretap


def _validate_cron_scheduler_output(wiretap):
    # Assert Cron Scheduler generated record output
    timestamp_field = wiretap.output_records[0].field['timestamp']
    assert timestamp_field.type == 'DATETIME'


@sdc_min_version('3.11.0')
def test_control_hub_api_processor_invalid_credentials(sdc_builder, sdc_executor):
    """Test Control Hub API Processor. The pipeline would look like:

        dev_raw_data_source >> control_hub_api_processor >> trash

    With invalid Control Hub credentials, Control Hub API Processor sends the record to error records list.
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.stop_after_first_batch = True

    control_hub_api_processor = pipeline_builder.add_stage('Control Hub API')
    control_hub_api_processor.control_hub_api_url = 'https://cloud.streamsets.com/security/rest/v1/currentUser'
    control_hub_api_processor.output_field = "/output"
    control_hub_api_processor.control_hub_user_name = "invalid user"
    control_hub_api_processor.password = "invalid password"
    if Version(sdc_builder.version) >= Version('4.0.0'):
        control_hub_api_processor.authentication_type = 'USER_PASSWORD'

    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> control_hub_api_processor >> wiretap.destination

    pipeline = pipeline_builder.build('Control Hub API Processor Sample Pipeline')
    sdc_executor.add_pipeline(pipeline)

    if Version(sdc_executor.version) >= Version('5.6.0'):
        with pytest.raises(ValidationError) as ex:
            sdc_executor.validate_pipeline(pipeline)

        assert 'CONTROL_HUB_08' in ex.value.issues['stageIssues'][control_hub_api_processor.instance_name][0]['message']
    else:
        sdc_executor.validate_pipeline(pipeline)

        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Assert Cron Scheduler generated record output
        assert len(wiretap.output_records) == 0
        assert len(wiretap.error_records) == 1

