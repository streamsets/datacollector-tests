# Copyright 2018 StreamSets Inc.
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

from streamsets.testframework.markers import sdc_min_version

logger = logging.getLogger(__name__)


# SDC-10376
@sdc_min_version('3.5.2')
def test_preview_with_events(sdc_builder, sdc_executor, cluster):
    """Try preview of simple pipeline with events - both on origin & destination side.
       Ensure that there are no validation or other other issues.
    """

    builder = sdc_builder.get_pipeline_builder()
    source = builder.add_stage('Dev Data Generator')
    destination = builder.add_stage('To Event')
    trash_source = builder.add_stage('Trash')
    trash_destination = builder.add_stage('Trash')

    source >> destination
    source >= trash_source
    destination >= trash_destination

    pipeline = builder.build(title='Preview With Events')
    pipeline.configuration['shouldRetry'] = False
    sdc_executor.add_pipeline(pipeline)

    preview = sdc_executor.run_pipeline_preview(pipeline).preview
    assert preview is not None
    assert preview.issues.issues_count == 0
