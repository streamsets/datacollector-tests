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

logger = logging.getLogger(__name__)


RECORD_DEDUPLICATOR_DATA = [dict(name='Jean-Luc Picard', rank='Captain'),
                            dict(name='Will Riker', rank='#1'),
                            dict(name='Data', rank='Commander'),
                            dict(name='Geordie LaForge', rank='Commander'),
                            dict(name='Wesley Crusher', rank='Shut Up')]
RECORD_DEDUPLICATIOR_DATA_EXTRA_RIKER = [dict(name='Will Riker', rank='First Officer')]
RECORD_DEDUPLICATIOR_DATA_EXTRA_WESLEY = [dict(name='Wesley Crusher', rank='Shut Up')]
RECORD_DEDUPLICATIOR_RAW_DATA = (RECORD_DEDUPLICATOR_DATA
                                 + RECORD_DEDUPLICATIOR_DATA_EXTRA_RIKER
                                 + RECORD_DEDUPLICATIOR_DATA_EXTRA_WESLEY)


def test_record_deduplicator_all_fields(sdc_builder, sdc_executor):
    """
    Test that uses a Record Deduplicator to filter out a duplicate record (matching all fields).  In this case, it
    results in the second 'Wesley Crusher' record being filtered out because both 'Wesley Crusher' records have the same
    /name and /rank fields.
    Pipeline looks like:

        dev_raw_data_source >> record_deduplicator >> wiretap_1
                                                   >> wiretap_2
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       json_content='ARRAY_OBJECTS',
                                       raw_data=json.dumps(RECORD_DEDUPLICATIOR_RAW_DATA),
                                       stop_after_first_batch=True)
    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    wiretap_1 = pipeline_builder.add_wiretap()
    wiretap_2 = pipeline_builder.add_wiretap()

    dev_raw_data_source >> record_deduplicator >> wiretap_1.destination
    record_deduplicator >> wiretap_2.destination

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    expected_unique_data = RECORD_DEDUPLICATOR_DATA + RECORD_DEDUPLICATIOR_DATA_EXTRA_RIKER
    unique_data = wiretap_1.output_records
    assert len(expected_unique_data) == len(unique_data)
    unique_data = [{key: value for key, value in record.field.items()} for record in unique_data]
    assert expected_unique_data == unique_data

    expected_duplicate_data = RECORD_DEDUPLICATIOR_DATA_EXTRA_WESLEY
    duplicate_data = wiretap_2.output_records
    assert len(expected_duplicate_data) == len(duplicate_data)
    duplicate_data = [{key: value for key, value in record.field.items()} for record in duplicate_data]
    assert expected_duplicate_data == duplicate_data


def test_record_deduplicator_single_field(sdc_builder, sdc_executor):
    """
    Test that uses a Record Deduplicator to filter out a duplicate record (matching a single field).  In this case, it's
    the /name field, resulting in the second 'Will Riker' and 'Wesley Crusher' records being filtered out. The 'Data'
    and 'Geordie LaForge' records are not considered duplicates because only their /rank field is the same.
    Pipeline looks like:

        dev_raw_data_source >> record_deduplicator >> wiretap_1
                                                   >> wiretap_2
    """
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='JSON',
                                       json_content='ARRAY_OBJECTS',
                                       raw_data=json.dumps(RECORD_DEDUPLICATIOR_RAW_DATA),
                                       stop_after_first_batch=True)
    record_deduplicator = pipeline_builder.add_stage('Record Deduplicator')
    record_deduplicator.set_attributes(compare='SPECIFIED_FIELDS',
                                       fields_to_compare=['/name'])
    wiretap_1 = pipeline_builder.add_wiretap()
    wiretap_2 = pipeline_builder.add_wiretap()

    dev_raw_data_source >> record_deduplicator >> wiretap_1.destination
    record_deduplicator >> wiretap_2.destination

    pipeline = pipeline_builder.build()
    sdc_executor.add_pipeline(pipeline)

    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    expected_unique_data = RECORD_DEDUPLICATOR_DATA
    unique_data = wiretap_1.output_records
    assert len(expected_unique_data) == len(unique_data)
    unique_data = [{key: value for key, value in record.field.items()} for record in unique_data]
    assert expected_unique_data == unique_data

    expected_duplicate_data = RECORD_DEDUPLICATIOR_DATA_EXTRA_RIKER + RECORD_DEDUPLICATIOR_DATA_EXTRA_WESLEY
    duplicate_data = wiretap_2.output_records
    assert len(expected_duplicate_data) == len(duplicate_data)
    duplicate_data = [{key: value for key, value in record.field.items()} for record in duplicate_data]
    assert expected_duplicate_data == duplicate_data
