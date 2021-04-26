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
    wiretap = pb.add_wiretap()

    origin >> wiretap.destination

    pipeline = pb.build()
    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline)
    sdc_executor.wait_for_pipeline_metric(pipeline, 'output_record_count', 1)
    sdc_executor.stop_pipeline(pipeline)
    records = [int(str(record.field)[1:]) for record in wiretap.output_records]
    records.sort()
    assert records == [i+1 for i in range(len(wiretap.output_records))]
