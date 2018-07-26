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

import logging

from resources.protobuf import addressbook_pb2

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# Barnum generates US data but that's ok for the example
name = 'Alex'
last_name = 'Sanchez'
phones = '123123123'
postcodes = '08905'
streets = 'Barcelona'

contacts = []
address = {'postcode': postcodes, 'address_lines': streets}
# Simulate the fact that postcode are optionals

phone_numbers = [{
    'type': 'MOBILE',
    'number': phones
}]
contacts.append({
    'first_name': name,
    'last_name': last_name,
    'address': address,
    'phone_numbers': phone_numbers
})


# Main procedure:  Reads the entire address book from a file,
#   adds one person based on user input, then writes it back out to the same
#   file.
def get_protobuf_data():
    """Need to protobuf-ize the data"""
    pb_contacts = []
    for contact in contacts:
        address = addressbook_pb2.Address()
        if contact['address'].get('address_lines'):
            address.address_lines.extend(contact['address']['address_lines'])
        if 'postcode' in contact['address']:
            address.postcode = contact['address']['postcode']
        pb_contacts.append(addressbook_pb2.Contact(
            first_name=contact['first_name'],
            last_name=contact['last_name'],
            address=address,
            phone_numbers=[
                addressbook_pb2.Phone(
                    type=addressbook_pb2.MOBILE if num['type'] == 'MOBILE' else addressbook_pb2.LANDLINE,
                    number=num['number']
                )
                for num in contact['phone_numbers']
            ]
        ))
    return addressbook_pb2.AddressBook(contacts=pb_contacts)


def get_multiple_protobufs():
    return str(get_protobuf_data()) + str(get_protobuf_data())
