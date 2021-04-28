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
import json


def test_capture_snapshot(sdc_builder, sdc_executor):
    """Verify the most basic use case for capture_snapshot where we capture the data and assert with the original."""
    pipeline_builder = sdc_builder.get_pipeline_builder()

    data = [
        {'NAME': 'Alex Sanchez', 'ROLE': 'Tech Lead', 'AGE': 27, 'TEAM': 'Data Plane'},
        {'NAME': 'Martin Balzamo', 'ROLE': 'Manager', 'AGE': 50, 'TEAM': 'BCN'},
        {'NAME': 'Tucu', 'ROLE': 'Distinguished Engineer', 'AGE': 50, 'TEAM': 'Innovation'},
        {'NAME': 'Xavi Baques', 'ROLE': 'Tech Lead', 'AGE': 28, 'TEAM': 'Enterprise'},
    ]

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       raw_data='\n'.join((json.dumps(row) for row in data)),
                                       stop_after_first_batch=True)
    trash = pipeline_builder.add_stage('Trash')

    dev_raw_data_source >> trash

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, snapshot_name='name', start_pipeline=True, wait=False).snapshot

    assert data == [record.field for record in snapshot[pipeline.stages[0]].output]


