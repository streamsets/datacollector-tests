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

# A module providing utils for working with XML data formats


# If Preserve Root Element is set to true in the origin, this method
# will navigate the root elements to find the expected data element
def get_xml_output_field(origin, output_field, *root_elements):
    if getattr(origin, 'preserve_root_element', False):
        for element in root_elements:
            output_field = output_field[element]
    return output_field
