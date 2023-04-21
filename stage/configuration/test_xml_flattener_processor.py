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
import pytest

from streamsets.sdk.utils import Version
from streamsets.testframework.decorators import stub


def test_attribute_delimiter(sdc_builder, sdc_executor):
    """Test XML flattener processor.
    The pipeline would look like:

        dev_raw_data_source >> xml_flattener >> wiretap
    """
    raw_data = """<contacts>
                      <contact>
                          <name type="maiden">NAME1</name>
                          <phone>(111)111-1111</phone>
                          <phone>(222)222-2222</phone>
                      </contact>
                      <contact>
                          <name type="maiden">NAME2</name>
                          <phone>(333)333-3333</phone>
                          <phone>(444)444-4444</phone>
                      </contact>
                  </contacts>"""
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT',
                                       raw_data=raw_data,
                                       custom_delimiter='</dummy>',
                                       use_custom_delimiter=True,
                                       stop_after_first_batch=True)
    # Set attribute delimiter
    xml_flattener = pipeline_builder.add_stage('XML Flattener', type='processor')
    xml_flattener.set_attributes(field_to_flatten='/text',
                                 record_delimiter='contact',
                                 attribute_delimiter='::',
                                 keep_original_fields=False)
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> xml_flattener >> wiretap.destination
    pipeline = pipeline_builder.build('XML flattener test attribute delimiter pipeline')

    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Gather wiretap data as a list for verification.
        items = [record.field for record in wiretap.output_records]

        expected_data = [{'contact.name::type': 'maiden',
                          'contact.name': 'NAME1',
                          'contact.phone(0)': '(111)111-1111',
                          'contact.phone(1)': '(222)222-2222'},
                         {'contact.name::type': 'maiden',
                          'contact.name': 'NAME2',
                          'contact.phone(0)': '(333)333-3333',
                          'contact.phone(1)': '(444)444-4444'}]

        assert items == expected_data
    finally:
        sdc_executor.remove_pipeline(pipeline)


def test_field_delimiter(sdc_builder, sdc_executor):
    """Test XML flattener processor.
    The pipeline would look like:

        dev_raw_data_source >> xml_flattener >> wiretap
    """
    raw_data = """<contacts>
                      <contact>
                          <name type="maiden">NAME1</name>
                          <phone>(111)111-1111</phone>
                          <phone>(222)222-2222</phone>
                      </contact>
                      <contact>
                          <name type="maiden">NAME2</name>
                          <phone>(333)333-3333</phone>
                          <phone>(444)444-4444</phone>
                      </contact>
                  </contacts>"""
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT',
                                       raw_data=raw_data,
                                       custom_delimiter='</dummy>',
                                       use_custom_delimiter=True,
                                       stop_after_first_batch=True)
    # Set field delimiter
    xml_flattener = pipeline_builder.add_stage('XML Flattener', type='processor')
    xml_flattener.set_attributes(field_to_flatten='/text',
                                 record_delimiter='contact',
                                 field_delimiter=':',
                                 keep_original_fields=False)
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> xml_flattener >> wiretap.destination
    pipeline = pipeline_builder.build('XML flattener test field delimiter pipeline')

    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Gather wiretap data as a list for verification.
        items = [record.field for record in wiretap.output_records]

        expected_data = [{'contact:name#type': 'maiden',
                          'contact:name': 'NAME1',
                          'contact:phone(0)': '(111)111-1111',
                          'contact:phone(1)': '(222)222-2222'},
                         {'contact:name#type': 'maiden',
                          'contact:name': 'NAME2',
                          'contact:phone(0)': '(333)333-3333',
                          'contact:phone(1)': '(444)444-4444'}]

        assert items == expected_data
    finally:
        sdc_executor.remove_pipeline(pipeline)


@stub
def test_field_to_flatten(sdc_builder, sdc_executor):
    pass


@pytest.mark.parametrize('stage_attributes', [{'ignore_attributes': False}, {'ignore_attributes': True}])
def test_ignore_attributes(sdc_builder, sdc_executor, stage_attributes):
    """Test XML flattener processor.
    The pipeline would look like:

        dev_raw_data_source >> xml_flattener >> wiretap
    """
    raw_data = """<contact type="person">
                    <name type="maiden" xmlns="http://blah.com/blah.xml">NAME1</name>
                    <phone>(111)111-1111</phone>
                    <phone>(222)222-2222</phone>
                  </contact>"""
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT',
                                       raw_data=raw_data,
                                       custom_delimiter='</dummy>',
                                       use_custom_delimiter=True,
                                       stop_after_first_batch=True)
    # Set ignore_attributes
    xml_flattener = pipeline_builder.add_stage('XML Flattener', type='processor')
    xml_flattener.set_attributes(field_to_flatten='/text',
                                 record_delimiter='contact',
                                 keep_original_fields=False,
                                 ignore_attributes=stage_attributes['ignore_attributes']
                                 )
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> xml_flattener >> wiretap.destination
    pipeline = pipeline_builder.build('XML flattener ignore attributes pipeline')

    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Gather wiretap data as a list for verification.
        items = [record.field for record in wiretap.output_records]

        if stage_attributes['ignore_attributes']==True:
            expected_data = [{'contact.name#xmlns': 'http://blah.com/blah.xml',
                              'contact.name': 'NAME1',
                              'contact.phone(0)': '(111)111-1111',
                              'contact.phone(1)': '(222)222-2222'}]
        elif stage_attributes['ignore_attributes']==False:
            expected_data = [{'contact#type': 'person',
                              'contact.name#type': 'maiden',
                              'contact.name#xmlns': 'http://blah.com/blah.xml',
                              'contact.name': 'NAME1',
                              'contact.phone(0)': '(111)111-1111',
                              'contact.phone(1)': '(222)222-2222'}]

        assert items == expected_data
    finally:
        sdc_executor.remove_pipeline(pipeline)


@pytest.mark.parametrize('stage_attributes', [{'ignore_namespace_uri': False}, {'ignore_namespace_uri': True}])
def test_ignore_namespace_uri(sdc_builder, sdc_executor, stage_attributes):
    """Test XML flattener processor.
    The pipeline would look like:

        dev_raw_data_source >> xml_flattener >> wiretap
    """
    raw_data = """<contact type="person">
                    <name type="maiden" xmlns="http://blah.com/blah.xml">NAME1</name>
                    <phone>(111)111-1111</phone>
                    <phone>(222)222-2222</phone>
                  </contact>"""
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT',
                                       raw_data=raw_data,
                                       custom_delimiter='</dummy>',
                                       use_custom_delimiter=True,
                                       stop_after_first_batch=True)
    # Set ignore_namespace_uri
    xml_flattener = pipeline_builder.add_stage('XML Flattener', type='processor')
    xml_flattener.set_attributes(field_to_flatten='/text',
                                 record_delimiter='contact',
                                 keep_original_fields=False,
                                 ignore_namespace_uri=stage_attributes['ignore_namespace_uri']
                                 )
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> xml_flattener >> wiretap.destination
    pipeline = pipeline_builder.build('XML flattener ignore namespace uri pipeline')

    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Gather wiretap data as a list for verification.
        items = [record.field for record in wiretap.output_records]


        if stage_attributes['ignore_namespace_uri']==True:
            expected_data = [{'contact#type': 'person',
                              'contact.name#type': 'maiden',
                              'contact.name': 'NAME1',
                              'contact.phone(0)': '(111)111-1111',
                              'contact.phone(1)': '(222)222-2222'}]
        elif stage_attributes['ignore_namespace_uri']==False:
            expected_data = [{'contact#type': 'person',
                              'contact.name#type': 'maiden',
                              'contact.name#xmlns': 'http://blah.com/blah.xml',
                              'contact.name': 'NAME1',
                              'contact.phone(0)': '(111)111-1111',
                              'contact.phone(1)': '(222)222-2222'}]

        assert items == expected_data
    finally:
        sdc_executor.remove_pipeline(pipeline)


@pytest.mark.parametrize('stage_attributes', [{'keep_original_fields': False}, {'keep_original_fields': True}])
def test_keep_original_fields(sdc_builder, sdc_executor, stage_attributes):
    """Test XML flattener processor.
    The pipeline would look like:

        dev_raw_data_source >> xml_flattener >> wiretap
    """
    raw_data = """<contact><name>NAME1</name><phone>(111)111-1111</phone></contact>"""
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT',
                                       raw_data=raw_data,
                                       custom_delimiter='</dummy>',
                                       use_custom_delimiter=True,
                                       stop_after_first_batch=True)
    # Set keep_original_fields
    xml_flattener = pipeline_builder.add_stage('XML Flattener', type='processor')
    xml_flattener.set_attributes(field_to_flatten='/text',
                                 record_delimiter='contact',
                                 keep_original_fields=stage_attributes['keep_original_fields']
                                 )
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> xml_flattener >> wiretap.destination
    pipeline = pipeline_builder.build('XML flattener test keep original fields pipeline')

    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Gather wiretap data as a list for verification.
        items = [record.field for record in wiretap.output_records]

        if stage_attributes['keep_original_fields'] == False:
            expected_data = [{'contact.name':'NAME1' ,
                              'contact.phone': '(111)111-1111'}]
        elif stage_attributes['keep_original_fields'] == True:
            expected_data = [{'text': '<contact><name>NAME1</name><phone>(111)111-1111</phone></contact>',
                              'contact.name':'NAME1' ,
                              'contact.phone': '(111)111-1111'}]

        assert items == expected_data
    finally:
        sdc_executor.remove_pipeline(pipeline)


@stub
@pytest.mark.parametrize('stage_attributes', [{'on_record_error': 'DISCARD'},
                                              {'on_record_error': 'STOP_PIPELINE'},
                                              {'on_record_error': 'TO_ERROR'}])
def test_on_record_error(sdc_builder, sdc_executor, stage_attributes):
    pass


@pytest.mark.parametrize('stage_attributes', [{'keep_original_fields': True}])
def test_output_field(sdc_builder, sdc_executor, stage_attributes):
    """Test XML flattener processor.
    The pipeline would look like:

        dev_raw_data_source >> xml_flattener >> wiretap
    """
    raw_data = """<contact type="person">
                    <name type="maiden" xmlns="http://blah.com/blah.xml">NAME1</name>
                    <phone>(111)111-1111</phone>
                    <phone>(222)222-2222</phone>
                  </contact>"""
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT',
                                       raw_data=raw_data,
                                       custom_delimiter='</dummy>',
                                       use_custom_delimiter=True,
                                       stop_after_first_batch=True)
    # When keep_original_fields is true, only then output_field can be set.
    xml_flattener = pipeline_builder.add_stage('XML Flattener', type='processor')
    xml_flattener.set_attributes(field_to_flatten='/text',
                                 record_delimiter='contact',
                                 keep_original_fields=stage_attributes['keep_original_fields'],
                                 output_field='contacts'
                                 )
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> xml_flattener >> wiretap.destination
    pipeline = pipeline_builder.build('XML flattener test output field pipeline')

    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Gather wiretap data as a list for verification.
        items = [record.field['contacts'] for record in wiretap.output_records]
        expected_data = [{'contact#type': 'person',
                          'contact.name#type': 'maiden',
                          'contact.name#xmlns': 'http://blah.com/blah.xml',
                          'contact.name': 'NAME1',
                          'contact.phone(0)': '(111)111-1111',
                          'contact.phone(1)': '(222)222-2222'}]

        assert items == expected_data
    finally:
        sdc_executor.remove_pipeline(pipeline)


@pytest.mark.parametrize('stage_attributes', [{'keep_original_fields': True, 'overwrite_existing_fields': False},
                                              {'keep_original_fields': True, 'overwrite_existing_fields': True}])
def test_overwrite_existing_fields(sdc_builder, sdc_executor, stage_attributes):
    """Test XML flattener processor.
    The pipeline would look like:

        dev_raw_data_source >> xml_flattener >> wiretap
    """
    if Version(sdc_builder.version) < Version('5.6.0') and stage_attributes['overwrite_existing_fields']:
        pytest.skip("The parameter 'overwrite_existing_fields' = True,"
                    " behaves differently only from SDC version >= 5.6.0")

    raw_data = """<contact><name>NAME1</name><phone>111111</phone><phone>222222</phone></contact>"""
    pipeline_builder = sdc_builder.get_pipeline_builder()
    # Note that since the delimiter text '</dummy>' does not exist in input XML, the output of this stage is whole XML.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT',
                                       raw_data=raw_data,
                                       custom_delimiter='</dummy>',
                                       use_custom_delimiter=True,
                                       stop_after_first_batch=True)
    # When keep_original_fields is true, then only overwrite_existing_fields can be set
    xml_flattener = pipeline_builder.add_stage('XML Flattener', type='processor')
    xml_flattener.set_attributes(field_to_flatten='/text',
                                 record_delimiter='contact',
                                 keep_original_fields=stage_attributes['keep_original_fields'],
                                 overwrite_existing_fields=stage_attributes['overwrite_existing_fields'])
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> xml_flattener >> wiretap.destination
    pipeline = pipeline_builder.build('XML flattener test overwrite existing fields pipeline')

    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Gather wiretap data as a list for verification.
        items = [record.field for record in wiretap.output_records]

        if stage_attributes['overwrite_existing_fields'] == False:
            expected_data = [{'text': '<contact><name>NAME1</name><phone>111111</phone><phone>222222</phone></contact>',
                              'contact.name':'NAME1',
                              'contact.phone(0)': '111111',
                              'contact.phone(1)': '222222'}]
        elif stage_attributes['overwrite_existing_fields'] == True:
            expected_data = [{'text': '<contact><name>NAME1</name><phone>111111</phone><phone>222222</phone></contact>',
                              'contact.name':'NAME1',
                              'contact.phone': '222222'}]
        assert items == expected_data
    finally:
        sdc_executor.remove_pipeline(pipeline)


@stub
def test_preconditions(sdc_builder, sdc_executor):
    pass


def test_record_delimiter(sdc_builder, sdc_executor):
    """Test XML flattener processor.
    The pipeline would look like:

        dev_raw_data_source >> xml_flattener >> wiretap
    """
    raw_data = """<contacts>
                      <contact>
                          <name type="maiden">NAME1</name>
                          <phone>(111)111-1111</phone>
                          <phone>(222)222-2222</phone>
                      </contact>
                      <contact>
                          <name type="maiden">NAME2</name>
                          <phone>(333)333-3333</phone>
                          <phone>(444)444-4444</phone>
                      </contact>
                  </contacts>"""
    pipeline_builder = sdc_builder.get_pipeline_builder()

    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT',
                                       raw_data=raw_data,
                                       custom_delimiter='</dummy>',
                                       use_custom_delimiter=True,
                                       stop_after_first_batch=True)
    # Set record delimiter
    xml_flattener = pipeline_builder.add_stage('XML Flattener', type='processor')
    xml_flattener.set_attributes(field_to_flatten='/text',
                                 record_delimiter='contact',
                                 keep_original_fields=False)
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> xml_flattener >> wiretap.destination
    pipeline = pipeline_builder.build('XML flattener test record delimiter pipeline')

    sdc_executor.add_pipeline(pipeline)
    try:
        sdc_executor.start_pipeline(pipeline).wait_for_finished()

        # Gather wiretap data as a list for verification.
        items = [record.field for record in wiretap.output_records]

        expected_data = [{'contact.name#type': 'maiden',
                          'contact.name': 'NAME1',
                          'contact.phone(0)': '(111)111-1111',
                          'contact.phone(1)': '(222)222-2222'},
                         {'contact.name#type': 'maiden',
                          'contact.name': 'NAME2',
                          'contact.phone(0)': '(333)333-3333',
                          'contact.phone(1)': '(444)444-4444'}]

        assert items == expected_data
    finally:
        sdc_executor.remove_pipeline(pipeline)


@stub
def test_required_fields(sdc_builder, sdc_executor):
    pass

