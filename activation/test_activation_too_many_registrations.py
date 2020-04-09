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

# This module starts SDC with only default stage libs like basic and runs pipelines for tests.

import logging
import pytest


from streamsets.sdk.exceptions import BadRequestError
from streamsets.testframework.markers import sdc_activation, sdc_min_version

from .utils.utils_activation import ACTIVATION_SUPPORT_SDC_MIN_VERSION, register_sdc

# Skip all tests in this module if --sdc-version < 3.15.0
pytestmark = sdc_min_version(ACTIVATION_SUPPORT_SDC_MIN_VERSION)

logger = logging.getLogger(__name__)

REGISTRATION_MAX_ATTEMPTS = 3

@sdc_activation
def test_with_basic_stage_too_many_registrations(sdc_executor):
    """
    Attempt to register more than the maximum allowed and verify the error message.
    """
    for _ in range(REGISTRATION_MAX_ATTEMPTS):
        register_sdc(sdc_executor)

    with pytest.raises(BadRequestError) as http_error:
        register_sdc(sdc_executor)
    logger.error(http_error)
    assert 'Too many attempts' in http_error.value.args[0]
