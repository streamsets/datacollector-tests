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
import pytest
from streamsets.testframework.utils import get_random_string


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.sdc_properties['stage.conf_com.streamsets.pipeline.stage.executor'
                                      '.shell.impersonation_mode'] = 'current_user'

    return hook


def test_shell_executor_impersonation(sdc_builder, sdc_executor):
    """Test proper impersonation on the Shell executor side. This is a dual pipeline test to test the executor
    side effect."""
    # Build a pipeline writing the name of the user executing shell commands to a random file under /tmp.
    # The Dev Raw Data Source is basically a noop origin and we use a Pipeline Finisher Executor to stop after 1 batch.
    filepath = f'/tmp/{get_random_string()}'

    builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = builder.add_stage('Dev Raw Data Source').set_attributes(data_format='JSON', raw_data='{}')
    shell = builder.add_stage('Shell').set_attributes(script=f'echo `whoami` > {filepath}')
    pipeline_finisher_executor = builder.add_stage('Pipeline Finisher Executor')
    dev_raw_data_source >> [shell, pipeline_finisher_executor]

    shell_pipeline = builder.build()
    sdc_executor.add_pipeline(shell_pipeline)
    sdc_executor.start_pipeline(shell_pipeline).wait_for_finished()

    # Use str.strip to get rid of any trailing newlines from echo.
    assert sdc_executor.read_file(filepath).strip() == sdc_executor.username
