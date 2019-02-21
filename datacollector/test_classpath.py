# Copyright 2017 StreamSets Inc.
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
import logging
import pytest
import urllib.request

from javaproperties import Properties

from streamsets.testframework.markers import cluster, sdc_min_version

# Skip all tests in this module if --sdc-version < 3.1.0.0
pytestmark = sdc_min_version('3.1.0.0-SNAPSHOT')

logger = logging.getLogger(__name__)

# Set of properties that needs to be enabled to perform the validation as we need to
ENABLE_CLASSPATH_VALIDATION = {
    # Make sure that the validation is enabled
    'stagelibs.classpath.validation.enable': 'true',
    # But never die, this test will validate result over REST
    'stagelibs.classpath.validation.terminate': 'false'
}

# List of stage libraries that have their own tests rather then the generic one
EXCLUDE_LIBS = {
    # Disable all MapR by default
    'streamsets-datacollector-mapr_5_1-lib',
    'streamsets-datacollector-mapr_5_2-lib',
    'streamsets-datacollector-mapr_6_0-lib',
    'streamsets-datacollector-mapr_6_1-lib',
    'streamsets-datacollector-mapr_6_0-mep4-lib',
    'streamsets-datacollector-mapr_6_0-mep5-lib',
    'streamsets-datacollector-mapr_6_1-mep6-lib',
    'streamsets-datacollector-mapr_spark_2_1_mep_3_0-lib'
}


@pytest.fixture(scope='module')
def sdc_common_hook():
    def hook(data_collector):
        # Add all the stage libraries that we should
        for stage_library in get_all_stage_libs():
            data_collector.add_stage_lib(stage_library)

        # Enable classpath validation itself
        data_collector.sdc_properties.update(ENABLE_CLASSPATH_VALIDATION)

    return hook


# Parametrize generator
def pytest_generate_tests(metafunc):
    if 'stagelib' in metafunc.fixturenames:
        metafunc.parametrize("stagelib", get_all_stage_libs())


# Return all stage libraries that should be loaded at once (outside of the explicitly excluded ones).
# Current implementation is temporary and will be replaced later on.
def get_all_stage_libs():
    raw_stagelibs = urllib.request.urlopen("http://nightly.streamsets.com.s3-us-west-2.amazonaws.com/datacollector/latest/tarball/stage-lib-manifest.properties")
    p = Properties()
    p.load(raw_stagelibs)
    return [lib for lib in [lib.replace('stage-lib.', '') for lib in p if 'stage-lib.' in lib] if lib not in EXCLUDE_LIBS]


def test_classpath(sdc_executor, stagelib):
    # Validate that
    if sdc_executor.server_url:
        pytest.skip('This test is only applicable to Docker-based SDC test.')

    # Validate that we can get classpath health report from the rest
    result_list = sdc_executor.api_client.get_classpath_health()
    assert result_list

    # And that we have report for our current stage
    result_stage = [r for r in result_list if r['name'] == stagelib]
    assert result_stage

    result_stage = result_stage[0]
    logger.info('Health report: %s', json.dumps(result_stage, indent=4))
    assert not result_stage['unparseablePaths']
    assert not result_stage['versionCollisions']
    assert result_stage['valid'] == True


@pytest.mark.skip(reason="See explanation in SDC-10319.")
@cluster('mapr')
def test_mapr_classpath(sdc_executor, cluster):
    for stagelib in cluster.sdc_stage_libs:
        test_classpath(sdc_executor, stagelib)
