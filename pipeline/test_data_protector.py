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

"""A module to test StreamSets Data Protector pipelines."""

import logging

import pytest

from streamsets.testframework.markers import sdc_min_version, sdp


@sdp
@sdc_min_version('3.4.0')
def test_schema_generator_processor(sdc_executor):
    """
    Test our example pipeline fragment.

    raw >> Discovery (ours) >> Discovery (custom) >> Protection >> Catcher >> Normal Destination
                                                                           >> Security Quarantine

    Input are 4 records - first two should be classified by our engine, last two should be classified
    by the custom engine. We will protect only first and third record, so only 2 records should end up
    in destination and the other two in security quarantine.
    """
    # Pipeline with various origins
    builder = sdc_executor.get_pipeline_builder()

    raw_source = builder.add_stage('Dev Raw Data Source')
    raw_source.stop_after_first_batch = True
    raw_source.data_format = 'JSON'
    raw_source.raw_data = """
        {"ssn": "123-45-6789"}
        {"email": "sales@streamsets.com"}
        {"patient": "Elon Musk"}
        {"nothing": "REAL-1234"}
    """

    sts_discovery = builder.add_stage(label='Data Discovery', library='streamsets-datacollector-dataprotector-lib')

    custom_discovery = builder.add_stage(label='Data Discovery', library='streamsets-datacollector-dataprotector-lib')
    custom_discovery.classification_engine = 'CUSTOM_PATTERNS'
    custom_discovery.patterns = [
        {
            "category" : "CUST_PATIENT",
            "score" : 1,
            "fieldPath" : "/patient",
            "fieldValue": ".*"
        }, {
            "category" : "CUST_REAL",
            "score" : 1,
            "fieldPath" : ".*",
            "fieldValue" : "REAL-[0-9]+",
        },
    ]

    protector = builder.add_stage(label='Data Protector', library='streamsets-datacollector-dataprotector-lib')
    protector.rules = [{
        "source" : "CLASSIFICATION_RESULT",
        "categoryPattern" : "US_SSN",
        "scoreThreshold" : 0.5,
        "transformer" : "REPLACE",
        "transformerConfig.REPLACE.targetType" : "STRING",
        "transformerConfig.REPLACE.targetValue" : "REPLACED"
      }, {
        "source" : "CLASSIFICATION_RESULT",
        "categoryPattern" : "CUST_PATIENT",
        "scoreThreshold" : 0.5,
        "transformer" : "REPLACE",
        "transformerConfig.REPLACE.targetType" : "STRING",
        "transformerConfig.REPLACE.targetValue" : "REPLACED"
      }]

    catcher = builder.add_stage(label='Data Catcher', library='streamsets-datacollector-dataprotector-lib')

    destination = builder.add_stage('Trash')

    quarantine = builder.add_stage('Trash')

    raw_source >> sts_discovery >> custom_discovery >> protector >> catcher >> destination
    catcher >> quarantine

    pipeline = builder.build('StreamSets Data Protector')
    sdc_executor.add_pipeline(pipeline)

    snapshot = sdc_executor.capture_snapshot(pipeline, start_pipeline=True).snapshot

    def exists_attribute(attribute, field):
        return attribute in field.attributes

    # Output of our discovery should be the same 4 records, first two classified, last two not
    output = snapshot[sts_discovery].output
    assert len(output) == 4
    assert exists_attribute('sdp.classification.US_SSN', output[0].field['ssn'])
    assert exists_attribute('sdp.classification.EMAIL', output[1].field['email'])
    assert not output[2].field['patient'].attributes and not output[3].field['nothing'].attributes

    # Custom discovery should add the remaining two classifications
    output = snapshot[custom_discovery].output
    assert len(output) == 4
    assert exists_attribute('sdp.classification.US_SSN', output[0].field['ssn'])
    assert exists_attribute('sdp.classification.EMAIL', output[1].field['email'])
    assert exists_attribute('sdp.classification.CUST_PATIENT', output[2].field['patient'])
    assert exists_attribute('sdp.classification.CUST_REAL', output[3].field['nothing'])

    # Protection should change first and third record
    output = snapshot[protector].output
    ssn = output[0].field['ssn']
    email = output[1].field['email']
    patient = output[2].field['patient']
    nothing = output[3].field['nothing']
    assert len(output) == 4
    assert (len(ssn.attributes) == 2 and exists_attribute('sdp.classification.US_SSN', ssn) and
            exists_attribute('sdp.protected', ssn))
    assert len(email.attributes) == 1 and exists_attribute('sdp.classification.EMAIL', email)
    assert (len(patient.attributes) == 2 and exists_attribute('sdp.protected', patient) and
            exists_attribute('sdp.classification.CUST_PATIENT', patient))
    assert len(nothing.attributes) == 1 and exists_attribute('sdp.classification.CUST_REAL', nothing)
    assert patient == 'REPLACED'

    # Normal catcher output
    output = snapshot[catcher].output_lanes[catcher.output_lanes[0]]
    ssn = output[0].field['ssn']
    patient = output[1].field['patient']
    assert len(output) == 2
    assert (len(ssn.attributes) == 2 and exists_attribute('sdp.classification.US_SSN', ssn) and
            exists_attribute('sdp.protected', ssn))
    assert (len(patient.attributes) == 2 and
            exists_attribute('sdp.classification.CUST_PATIENT', patient) and exists_attribute('sdp.protected', patient))
    assert ssn == 'REPLACED'
    assert patient == 'REPLACED'

    # Security quarantine
    output = snapshot[catcher].output_lanes[catcher.output_lanes[1]]
    email = output[0].field['email']
    nothing = output[1].field['nothing']
    assert len(output) == 2
    assert len(email.attributes) == 1 and exists_attribute('sdp.classification.EMAIL', email)
    assert len(nothing.attributes) == 1 and exists_attribute('sdp.classification.CUST_REAL', nothing)
