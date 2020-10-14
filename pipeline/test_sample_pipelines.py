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

from streamsets.testframework.markers import sdc_min_version


@sdc_min_version('3.19.0')
def test_dateconversions_pipeline(sdc_executor):
    """Test the Date Conversions sample pipeline."""
    try:
        sample = sdc_executor.sample_pipelines.get(title='Date Conversions')

        # Before testing a sample pipeline, you have to first make a copy of it.
        copy_of_sample = sdc_executor.get_pipeline_builder().import_pipeline(
            pipeline=sample._data).build(title='Copy of {}'.format(sample.title))

        copy_of_sample.origin_stage.stop_after_first_batch = True

        sdc_executor.add_pipeline(copy_of_sample)

        snapshot = sdc_executor.capture_snapshot(pipeline=copy_of_sample,
                                                 start_pipeline=True).snapshot

        assert(snapshot['FieldTypeConverter_01'].output[0].field['date1'].type == 'DATETIME')
        assert(snapshot['FieldTypeConverter_01'].output[0].field['date2'].type == 'DATETIME')

    finally:
        sdc_executor.delete_pipeline(copy_of_sample)
