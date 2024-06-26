# Copyright 2024 StreamSets Inc.
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
import re
from streamsets.testframework.markers import sdc_min_version



logger = logging.getLogger(__name__)


@sdc_min_version('6.0.0')
def test_dev_synthetic_data_generator_field_generation_boolean(sdc_builder, sdc_executor):
    """Test synthetic_data_generator can generate boolean fields
    and an attribute is added to a record

    Pipeline: synthetic_data_generator >> wiretap
    """

    builder = sdc_builder.get_pipeline_builder()
    dev_synthetic_data_generator = builder.add_stage('Dev Synthetic Data Generator')
    dev_synthetic_data_generator.set_attributes(
        record_fields=[{'fieldName': 'f1', 'fieldType': 'System__Field__Boolean'}],
        record_header_attributes=[{'attributeName': 'AN1', 'attributeValue': 'an1'}],
        records_to_generate=100)

    wiretap = builder.add_wiretap()

    dev_synthetic_data_generator >> wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    output_records = [rec.field['f1'] for rec in wiretap.output_records]
    output_attributes = [rec.header.values['AN1'] for rec in wiretap.output_records]

    for record in output_records:
        assert (record == False) or (record == True)
    for attribute in output_attributes:
        assert attribute == 'an1'
    assert len(output_records) == 100


@sdc_min_version('6.0.0')
def test_dev_synthetic_data_generator_field_generation_integer(sdc_builder, sdc_executor):
    """Test synthetic_data_generator can generate an integer field
    which value is 1, batch size is 10 and maximum number of threads.

    Pipeline: synthetic_data_generator >> wiretap
    """

    builder = sdc_builder.get_pipeline_builder()
    dev_synthetic_data_generator = builder.add_stage('Dev Synthetic Data Generator')
    dev_synthetic_data_generator.set_attributes(
        record_fields=[{'fieldName': 'f1', 'fieldType': 'System__Field__Integer', 'fieldValue': '1'}],
        record_header_attributes=[], batch_size=10, generator_threads = 3,
        records_to_generate=100)

    wiretap = builder.add_wiretap()

    dev_synthetic_data_generator >> wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    output_records = [rec.field['f1'] for rec in wiretap.output_records]

    for record in output_records:
        assert record == 1

    assert len(output_records) == 100


@sdc_min_version('6.0.0')
def test_dev_synthetic_data_generator_field_generation_long(sdc_builder, sdc_executor):
    """Test synthetic_data_generator can generate long field

    Pipeline: synthetic_data_generator >> wiretap
    """

    builder = sdc_builder.get_pipeline_builder()
    dev_synthetic_data_generator = builder.add_stage('Dev Synthetic Data Generator')
    dev_synthetic_data_generator.set_attributes(
        record_fields=[{'fieldName': 'f1', 'fieldType': 'System__Field__Long'}],
        record_header_attributes=[],
        records_to_generate=100)

    wiretap = builder.add_wiretap()

    dev_synthetic_data_generator >> wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    output_records = [rec.field['f1'] for rec in wiretap.output_records]
    for record in output_records:
        try:
            aux = int(str(record)) + 1
        except Exception as e:
            assert False, str(e)

    assert len(output_records) == 100

@sdc_min_version('6.0.0')
def test_dev_synthetic_data_generator_field_generation_email(sdc_builder, sdc_executor):
    """Test synthetic_data_generator can generate email values and
    events are generated and checked.

    Pipeline: synthetic_data_generator >> wiretap
    """

    email_format: str = r"(^[a-zA-Z0-9'_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)"

    builder = sdc_builder.get_pipeline_builder()
    dev_synthetic_data_generator = builder.add_stage('Dev Synthetic Data Generator')
    dev_synthetic_data_generator.set_attributes(
        record_fields=[{'fieldName': 'f1', 'fieldType': 'Base__Internet__EmailAddress'}],
        record_header_attributes=[],
        records_to_generate=100)

    wiretap = builder.add_wiretap()

    dev_synthetic_data_generator >> wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    output_records = [rec.field['f1'] for rec in wiretap.output_records]

    for record in output_records:
        assert re.match(email_format, str(record), re.IGNORECASE)

    assert len(output_records) == 100


@sdc_min_version('6.0.0')
def test_dev_synthetic_data_generator_events(sdc_builder, sdc_executor):
    """Test synthetic_data_generator can generate events

    Pipeline: synthetic_data_generator >= wiretap
    """

    builder = sdc_builder.get_pipeline_builder()
    dev_synthetic_data_generator = builder.add_stage('Dev Synthetic Data Generator')
    dev_synthetic_data_generator.set_attributes(
        record_fields=[{'fieldName': 'f1', 'fieldType': 'System__Field__String', 'fieldValue':'event'}],
        record_header_attributes=[],
        event_name = 'my-event',
        records_to_generate=5)

    wiretap = builder.add_wiretap()

    trash = builder.add_stage("Trash")

    dev_synthetic_data_generator >> trash
    dev_synthetic_data_generator >= wiretap.destination

    pipeline = builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()
    print(wiretap.output_records)

    output_records = [rec.field['f1'] for rec in wiretap.output_records]
    for record in output_records:
        assert record == 'event'

    assert len(output_records) == 5
