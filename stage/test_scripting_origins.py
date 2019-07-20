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

import pytest
from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        data_collector.add_stage_lib('streamsets-datacollector-jython_2_7-lib')
        data_collector.add_stage_lib('streamsets-datacollector-basic-lib')
        data_collector.add_stage_lib('streamsets-datacollector-groovy_2_4-lib')

    return hook


@sdc_min_version('3.11.0')
@pytest.mark.parametrize('stage_name', ['Groovy Scripting', 'JavaScript Scripting', 'Jython Scripting'])
def test_scripting_origin_default_script(sdc_builder, sdc_executor, stage_name):
    """ Test a scripting origin with the default script. Ensure that it generates records as expected."
    """
    pb = sdc_builder.get_pipeline_builder()
    origin = pb.add_stage(stage_name, type='origin')
    origin.set_attributes(batch_size=7)
    trash = pb.add_stage('Trash')

    origin >> trash

    pipeline = pb.build()
    sdc_executor.add_pipeline(pipeline)
    snapshot1 = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot
    sdc_executor.stop_pipeline(pipeline)
    records = snapshot1[origin.instance_name].output
    assert len(records) == 7
    assert records[6].field == ':7'
