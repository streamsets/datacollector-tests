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

import logging
from xml.etree import ElementTree

logger = logging.getLogger(__name__)

# pylint: disable=pointless-statement, too-many-locals


def test_xml_parser(sdc_builder, sdc_executor):
    """Test XML parser processor. XML in this test is simple.
       The pipeline would look like:

           dev_raw_data_source >> xml_parser >> wiretap
    """
    raw_data = """<?xml version="1.0" encoding="UTF-8"?>
                  <root>
                      <msg>
                          <time>8/12/2016 6:01:00</time>
                          <request>GET /index.html 200</request>
                      </msg>
                      <msg>
                          <time>8/12/2016 6:03:43</time>
                          <request>GET /images/sponsored.gif 304</request>
                      </msg>
                  </root>"""
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    # Note that since the delimiter text '</dummy>' does not exist in input XML, the output of this stage is whole XML.
    dev_raw_data_source.set_attributes(data_format='TEXT', raw_data=raw_data, custom_delimiter='</dummy>',
                                       use_custom_delimiter=True,
                                       stop_after_first_batch=True)
    xml_parser = pipeline_builder.add_stage('XML Parser', type='processor')
    xml_parser.set_attributes(field_to_parse='/text', ignore_control_characters=True, target_field='/text')
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> xml_parser >> wiretap.destination
    pipeline = pipeline_builder.build('XML parser pipeline')

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    # Gather wiretap data in a list for verification.
    item_list = wiretap.output_records[0].field['text']['msg']
    rows_from_records = [{item['time'][0]['value'].value: item['request'][0]['value'].value}
                          for item in item_list]

    # Parse input xml data to verify results from wiretap.
    root = ElementTree.fromstring(raw_data)
    expected_data = [{msg.find('time').text: msg.find('request').text}
                     for msg in root.iter('msg')]

    assert rows_from_records == expected_data


def test_xml_parser_namespace_xpath(sdc_builder, sdc_executor):
    """Test XML parser processor. XML in this test contains namesapces.
    The pipeline would look like:

        dev_raw_data_source >> xml_parser >> wiretap
    """
    raw_data = """<?xml version="1.0" encoding="UTF-8"?>
                  <root>
                      <a:data xmlns:a="http://www.companyA.com">
                          <msg>
                              <time>8/12/2016 6:01:00</time>
                              <request>GET /index.html 200</request>
                          </msg>
                          </a:data>
                      <c:data xmlns:c="http://www.companyC.com">
                          <sale>
                              <item>Shoes</item>
                              <item>Magic wand</item>
                              <item>Tires</item>
                          </sale>
                      </c:data>
                      <a:data xmlns:a="http://www.companyA.com">
                          <msg>
                              <time>8/12/2016 6:03:43</time>
                              <request>GET /images/sponsored.gif 304</request>
                          </msg>
                      </a:data>
                  </root>"""
    pipeline_builder = sdc_builder.get_pipeline_builder()
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    # Note that since the delimiter text '</dummy>' does not exist in input XML, the output of this stage is whole XML.
    dev_raw_data_source.set_attributes(data_format='TEXT',
                                       raw_data=raw_data,
                                       custom_delimiter='</dummy>',
                                       use_custom_delimiter=True,
                                       stop_after_first_batch=True)

    xml_parser = pipeline_builder.add_stage('XML Parser', type='processor')
    # Specify xpath in delimiter to generate multiple records from the XML document.
    # Also namespaces are used since input XML contains the same.
    xml_parser.set_attributes(field_to_parse='/text',
                              ignore_control_characters=True,
                              target_field='/text',
                              delimiter_element='/root/a:data/msg',
                              multiple_values_behavior='ALL_AS_LIST',
                              namespaces=[{'key': 'a', 'value': 'http://www.companyA.com'}])
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> xml_parser >> wiretap.destination
    pipeline = pipeline_builder.build('XML parser namespace pipeline')

    sdc_executor.add_pipeline(pipeline)
    sdc_executor.start_pipeline(pipeline).wait_for_finished()

    # Gather wiretap data as a list for verification.
    item_list = wiretap.output_records[0].field['text']
    rows_from_records = [{item['time'][0]['value'].value: item['request'][0]['value'].value}
                          for item in item_list]

    # Parse input xml data to verify results from wiretap using xpath for search.
    root = ElementTree.fromstring(raw_data)
    expected_data = [{msg.find('time').text: msg.find('request').text}
                     for msg in root.findall('.//msg')]

    assert rows_from_records == expected_data


def test_xml_flattener(sdc_builder, sdc_executor):
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
    # Note that since the delimiter text '</dummy>' does not exist in input XML, the output of this stage is whole XML.
    dev_raw_data_source = pipeline_builder.add_stage('Dev Raw Data Source')
    dev_raw_data_source.set_attributes(data_format='TEXT',
                                       raw_data=raw_data,
                                       custom_delimiter='</dummy>',
                                       use_custom_delimiter=True,
                                       stop_after_first_batch=True)
    # Specify a record delimiter to generate multiple records from the XML document.
    xml_flattener = pipeline_builder.add_stage('XML Flattener', type='processor')
    xml_flattener.set_attributes(field_to_flatten='/text',
                                 record_delimiter='contact',
                                 keep_original_fields=False)
    wiretap = pipeline_builder.add_wiretap()

    dev_raw_data_source >> xml_flattener >> wiretap.destination
    pipeline = pipeline_builder.build('XML flattener pipeline')

    sdc_executor.add_pipeline(pipeline)
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
