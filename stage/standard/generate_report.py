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
import logging
import os
import re
import argparse
from termcolor import colored

logger = logging.getLogger(__name__)

# TODOs:
# * Add more logging statements (now when they can be filtered out)
# * Add way to report test per stages (to see what is implemented and where)

#
# Quick and dirty script to display some sort of "report" of our standard test suite. I'm expecting that eventually
# this script gets discarded and it's logic gets folded into bifocals. But for the time being, at least some ability
# to get stats on what we have and what we are missing.
#

# Various constants used in this script
UNKNOWN = 'Unknown'
ORIGIN = 'Origin'
PROCESSOR = 'Processor'
DESTINATION = 'Destination'
EXECUTOR = 'Executor'
STUB = "Stub"
SKIPPED = "Skipped"
IMPLEMENTED = "Implemented"

# Expected tests
EXPECTED_CATEGORIES_BASELINE = ['test_data_type', 'test_object_name', 'test_dataflow_event', 'test_multiple_batch', 'test_data_format']
EXPECTED_CATEGORIES = {
    ORIGIN: EXPECTED_CATEGORIES_BASELINE + ['test_resume_offset', 'test_empty_object'],
    PROCESSOR: EXPECTED_CATEGORIES_BASELINE + ['test_push_pull', 'test_field_format', 'test_lookup'],
    DESTINATION: EXPECTED_CATEGORIES_BASELINE + ['test_push_pull'],
    EXECUTOR: EXPECTED_CATEGORIES_BASELINE + ['test_push_pull', 'test_start', 'test_stop']
}
ALL_CATEGORIES = list(dict.fromkeys(EXPECTED_CATEGORIES[ORIGIN] + EXPECTED_CATEGORIES[PROCESSOR] + EXPECTED_CATEGORIES[DESTINATION] + EXPECTED_CATEGORIES[EXECUTOR]))

def _text_variants(variants):
    if len(variants) == 0:
        return ""

    parts = []
    for name, implementation in variants.items():
        color = 'green'
        if implementation == STUB:
            color = 'red'
        elif implementation == SKIPPED:
            color = 'yellow'

        if name != '':
            parts.append(f"{colored(name, color)}")

    return "(" + ",".join(parts) + ")" if len(parts) > 0 else ''


def _text_for_variants(variant):
    if len(variant) == 0:
        return f"{colored('completely missing', 'red', attrs=['bold'])}"

    total = len(variant.keys())
    sum = 0
    for implementation in variant.values():
        if implementation == IMPLEMENTED or implementation == SKIPPED:
            sum = sum + 1

    if sum == total:
        return f"{colored('fully implemented', 'green', attrs=['bold'])} {_text_variants(variant)}"
    elif sum == 0:
        return f"{colored('is not implemented at all', 'red', attrs=['bold'])} {_text_variants(variant)}"
    elif sum / total > 0.75:
        return f"{colored('is partially implemented', 'yellow', attrs=['bold'])} {_text_variants(variant)}"
    else:
        return f"{colored('is partially implemented', 'red', attrs=['bold'])} {_text_variants(variant)}"


def _format_count_and_total(count, total):
    percentage = count / total * 100
    color = 'red'
    if percentage >= 75:
        color = 'green'
    elif percentage >= 50:
        color = 'yellow'
    return f"{count}/{colored(total, 'white', attrs=['bold'])} ({colored('%.2f' % percentage, color, attrs=['bold'])}%)"



class TestFile:
    """Describes content of a test file (what is/isn't implemented, ...)"""
    def __init__(self, test_name):
        # Test name is pretty much the file name
        self.test_name = test_name

        # We derive the  stage type from the filename too
        self.stage_type = UNKNOWN
        if "_origin" in file_path:
            self.stage_type = ORIGIN
        elif "_processor" in file_path:
            self.stage_type = PROCESSOR
        elif "_destination" in file_path:
            self.stage_type = DESTINATION
        elif "_executor" in file_path:
            self.stage_type = EXECUTOR
        else:
            raise Exception(f"Unknown stage type for test file {file_path}")

        # Test is structure of expect test "category" which is itself a hash of "variant" and implementation state
        self.tests = {}
        for test in EXPECTED_CATEGORIES[self.stage_type]:
            self.tests[test] = {}

    def add_test_method(self, test_method, implementation_state):
        """Add test method to this file container."""
        # First we have to understand what test category the method belongs to, we do that by looking for known
        # prefixes that we already have in the self.tests dictionary
        test_category = None
        for key in self.tests.keys():
            if test_method.startswith(key):
                test_category = key
        if test_category is None:
            raise Exception(f"Unknown test category for test method {test_method}")

        # Now we have to get variant of the test in case that there are more variants. We generally remove useless
        # prefixes of (es_) - not all characters might show up, but they frequently do.
        test_variant = test_method[len(test_category):]
        if test_variant.startswith("e"):
            test_variant = test_variant[1:]
        if test_variant.startswith("s"):
            test_variant = test_variant[1:]
        if test_variant.startswith("_"):
            test_variant = test_variant[1:]

        # And finally persist everything down
        self.tests[test_category][test_variant] = implementation_state

    def implemented_categories(self):
        """Return count of fully implemented categories."""
        sum = 0
        for variant in self.tests.values():
            if len(variant.keys()) == 0:
                pass # We don't count empty non-implemented test categories
            else:
                all = True
                for implementation in variant.values():
                    if implementation != IMPLEMENTED and implementation != SKIPPED:
                        all = False


                if all:
                    sum = sum + 1
        return sum

    def implemented_variants(self):
        """Return count of fully implemented variants."""
        sum = 0
        for variant in self.tests.values():
            if len(variant.keys()) == 0:
                sum = sum + 1
            else:
                for implementation in variant.values():
                    if implementation == IMPLEMENTED or implementation == SKIPPED:
                        sum = sum + 1
        return sum

    def categories(self):
        """Return count of categories for this file."""
        return len(self.tests.keys())

    def variants(self):
        """Return count of variants for this file."""
        sum = 0
        for variant in self.tests.values():
            if len(variant.keys()) == 0:
                sum = sum + 1
            else:
                sum = sum + len(variant.keys())
        return sum

    def print(self):
        print(f"Stage: {colored(self.test_name, 'white', attrs=['bold'])} ({self.stage_type})")
        print(f"\tCategories {_format_count_and_total(self.implemented_categories(), self.categories())}, Variants {_format_count_and_total(self.implemented_variants(), self.variants())}")
        for category, variants in self.tests.items():
            print(f"\tTest category {colored(category, 'white', attrs=['bold'])} is {_text_for_variants(variants)}")


class Report:
    """Report for the whole directory"""
    def __init__(self, tests):
        self.tests = tests

    def stages_for_type(self, stage_type):
        return [f for f in self.tests if f.stage_type == stage_type]

    def print_summary(self):
        print(f"Stages: {colored(len(self.tests), 'white', attrs=['bold'])}")
        print(f"\tOrigins: {colored(len(self.stages_for_type(ORIGIN)), 'white', attrs=['bold'])}, Processors: {colored(len(self.stages_for_type(PROCESSOR)), 'white', attrs=['bold'])}, Destinations: {colored(len(self.stages_for_type(DESTINATION)), 'white', attrs=['bold'])}, Executors: {colored(len(self.stages_for_type(EXECUTOR)), 'white', attrs=['bold'])}")
        print('')
        total_categories = 0
        total_variants = 0
        implemented_categories = 0
        implemented_variants = 0
        for test in self.tests:
            total_categories = total_categories + test.categories()
            total_variants = total_variants + test.variants()
            implemented_categories = implemented_categories + test.implemented_categories()
            implemented_variants = implemented_variants + test.implemented_variants()

        print(f"\tTest categories: {_format_count_and_total(implemented_categories, total_categories)}")
        print(f"\tTest variants: {_format_count_and_total(implemented_variants, total_variants)}")
        print('')

    def print_stages(self):
        print('Per Stage Stats')
        for test in self.tests:
            test.print()
            print('')

    def print_categories(self):
        print('Per Test Category Stats')
        for category in ALL_CATEGORIES:
            print(f"Category {colored(category, 'white', attrs=['bold'])}")
            for test in self.tests:
                if category in test.tests:
                    variants = test.tests[category]
                    print(f"\tTest file {colored(test.test_name, 'white', attrs=['bold'])} is {_text_for_variants(variants)}")

            print()


# Parse Command line options
parser = argparse.ArgumentParser(description='Generate various coverage reports for StreamSets Standard Tests')
parser.add_argument('--dir', dest='test_dir', help='Directory of standard tests (current working directory by default)')
parser.add_argument('--verbose', dest='verbose', action='store_true', help='Log details while parsing test files')
parser.add_argument('--summary', dest='summary', action='store_true', help='Print summary report for all test files')
parser.add_argument('--stages', dest='stages', action='store_true', help='Print report for al individual stages')
parser.add_argument('--categories', dest='categories', action='store_true', help='Print summary report pivoted for each test category')

args = parser.parse_args()
if not args.summary and not args.stages and not args.tests:
    raise Exception('At least one of --summary, --stages, --tests is required')

# Configure logging per the arguments
log_level = logging.DEBUG if args.verbose else logging.WARN
logging.basicConfig(level=log_level)

# Get current working directory - we assume that we're generating report for files there
test_dir = os.getcwd() if args.test_dir is None else args.test_dir
logger.info(f"Loading tests from {test_dir}")

# Let's load and parse the files around
tests = []
test_files = [f for f in os.listdir(test_dir) if os.path.isfile(os.path.join(test_dir, f)) and "test_" in f]
for file_path in test_files:
    logger.info(f"Processing file {file_path}")
    with open(os.path.join(test_dir, file_path), "r") as file:
        lines = file.readlines()

        # Structure representing the test file
        test_file = TestFile(file_path)

        # Stateful reading of the file
        stub = False
        for i in range(len(lines)):
            if "@stub" in lines[i]:
                stub = True
            match = re.search("def (test_[a-z0-9_]+)", lines[i])
            if match:
                type = IMPLEMENTED
                if stub:
                    type = STUB
                elif "pytest.skip" in lines[i+1]:
                    type = SKIPPED
                test_name = match.group(1)
                test_file.add_test_method(match.group(1), type)
                stub = False

        tests.append(test_file)


# And finally generate a report and print what needs to be printed
report = Report(tests)
if args.summary:
    report.print_summary()
if args.stages:
    report.print_stages()
if args.categories:
    report.print_categories()
