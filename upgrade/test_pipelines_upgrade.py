# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import glob
import logging
import pytest
from os.path import dirname, join, realpath

from testframework import sdc, sdc_models
from testframework.markers import upgrade

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@upgrade
def test_pipeline_upgrade(sdc_executor, pipeline_full_path):
    with sdc.DataCollector(version=sdc_executor.version) as data_collector:
        pipeline = sdc_models.Pipeline(pipeline_full_path)
        data_collector.configure_for_pipeline(pipeline)
        data_collector.add_pipeline(pipeline)
        data_collector.start()
        export_json = data_collector.api_client.export_pipeline(pipeline.id)

    issues = export_json['pipelineConfig']['issues']

    if issues['issueCount']:
        pytest.fail(str(issues))


# pytest_generate_tests helps to parametrize pipelines which we will read from disk.
# More about pytest parametrization at http://doc.pytest.org/en/latest/parametrize.html
def pytest_generate_tests(metafunc):
    pipelines = glob.iglob(join(dirname(realpath(__file__)), 'pipelines', '**', '*.json'),
                           recursive=True)
    metafunc.parametrize('pipeline_full_path', pipelines)
