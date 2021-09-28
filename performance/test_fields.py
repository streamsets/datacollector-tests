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

import json


def test_many_field_processor_stages(sdc_builder, sdc_executor):
    """Benchmark a pipeline with many field processor stages"""

    raw_data = """
    {
      "first": {
        "a": "one",
        "b": {
          "b1": 1.1,
          "b2": null,
          "b3": 1.3
        },
        "c": [3, 4, 5]
      },
      "second": {
        "d": "two",
        "e": {
          "b1": 2.1,
          "b2": null,
          "b3": 2.3
        },
        "f": [6, 7, 8]
      },
      "third": {
        "g": "three",
        "h": {
          "b1": 3.1,
          "b2": null,
          "b3": 3.3
        },
        "i": [9, 10, 11]
      }
    }
    """

    pipeline_builder = sdc_builder.get_pipeline_builder()
    benchmark_stages = pipeline_builder.add_benchmark_stages()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)

    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.set_attributes(fields=['/second/f'], action='REMOVE')

    value_replacer = pipeline_builder.add_stage('Value Replacer', type='processor')
    value_replacer.set_attributes(replace_null_values=[{'fields': ['/*/*/*2'],
                                                        'newValue': '42.0'}])

    field_type_converter = pipeline_builder.add_stage('Field Type Converter')
    field_type_converter.set_attributes(conversion_method='BY_FIELD',
                                        field_type_converter_configs=[{'fields': ['/*/*/*2'],
                                                                       'targetType': 'DOUBLE',
                                                                       'dataLocale': 'en,US'}])

    field_hasher = pipeline_builder.add_stage('Field Hasher')
    field_hasher.set_attributes(hash_entire_record=False,
                                hash_to_target=[{'sourceFieldsToHash': ['/third/h*', '/third/g'],
                                                 'hashType': 'MD5',
                                                 'targetField': '/thirdHash'}])

    field_masker = pipeline_builder.add_stage('Field Masker')
    field_masker.set_attributes(field_mask_configs=[{'fields': ['/second/d'],
                                                     'maskType': 'FIXED_LENGTH',
                                                     'regex': '(.*)',
                                                     'groupsToShow': '1'}])

    dev_raw_data_source >> field_remover >> value_replacer >> field_type_converter >> field_hasher
    field_hasher >> field_masker >> benchmark_stages.destination

    pipeline = pipeline_builder.build()

    sdc_executor.benchmark_pipeline(pipeline, record_count=100_000)


def test_large_number_of_fields(sdc_builder, sdc_executor):
    """Benchmark a pipeline with one processor that removes many fields from records with a large number of fields"""

    raw_data_obj = {f'field{i}': i for i in range(1, 250)}
    raw_data = json.dumps(raw_data_obj)

    pipeline_builder = sdc_builder.get_pipeline_builder()
    benchmark_stages = pipeline_builder.add_benchmark_stages()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON', raw_data=raw_data)

    field_remover = pipeline_builder.add_stage('Field Remover')
    field_remover.set_attributes(fields=['/field1*'], action='REMOVE')

    dev_raw_data_source >> field_remover >> benchmark_stages.destination

    pipeline = pipeline_builder.build()

    sdc_executor.benchmark_pipeline(pipeline, record_count=100_000)
