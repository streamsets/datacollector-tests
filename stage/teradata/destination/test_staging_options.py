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

import pytest

from . import (
    pytestmark,
)


def test_purge_file():
    """
        Assert that Purge File works as expected.
    """
    pytest.skip('We do not have yet the ability to gather staging file information. The way of accessing each '
                'staging location should be different, depending on the system. Getting to acknowledge if those files '
                'were removed or not might not be straightforward, as we do not expose the information of the files '
                'created.')
