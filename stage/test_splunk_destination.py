# Copyright 2025 StreamSets Inc.
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
import json
import string

from streamsets.testframework.markers import splunk, sdc_min_version
from streamsets.testframework.utils import get_random_string

logger = logging.getLogger(__name__)


@splunk
def test_splunk_destination(sdc_builder, sdc_executor, splunk):
    pipeline_builder = sdc_builder.get_pipeline_builder()
    pipeline_builder.add_error_stage('Discard')

    input_data = {
        "index": "main",
        "sourcetype": "_json",
        "event": {
            "message": get_random_string(string.ascii_letters, 10),
            "severity": "INFO"
        }
    }

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data=json.dumps(input_data),
                                       stop_after_first_batch=True)

    splunk_destination = pipeline_builder.add_stage('Splunk')

    dev_raw_data_source >> splunk_destination

    pipeline = pipeline_builder.build().configure_for_environment(splunk)

    try:
        sdc_executor.add_pipeline(pipeline)
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        events = splunk.get_events(input_data.get('event').get('message')).get('results')
        assert len(events) == 1
        assert input_data.get('event').get('message') in events[0].get('_raw')

    finally:
        logger.info('Dropping events in splunk server...')
        splunk.delete_events(input_data.get('event').get('message'))
