import logging
from datetime import datetime, timezone

import pytest
import pytz

logger = logging.getLogger(__name__)


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item):
    # execute all other hooks to obtain the report object
    outcome = yield
    rep = outcome.get_result()

    # set a report attribute for each phase of a call, which can
    # be "setup", "call", "teardown"

    setattr(item, f'rep_{rep.when}', rep)


@pytest.fixture(autouse=True)
def sdc_log_emitter(request, sdc_executor):
    if sdc_executor.dump_log_on_error:
        sdc_time = datetime.now(timezone.utc)
        yield
        reason = None
        # request.node is an "item" because we use the default
        # "function" scope
        if request.node.rep_setup.failed:
            reason = 'setting up'
        elif request.node.rep_setup.passed:
            if request.node.rep_call.failed:
                reason = 'executing'
        if reason is not None:
            logs_timezone = sdc_executor.sdc_configuration['ui.server.timezone']
            sdc_time = sdc_time.astimezone(pytz.timezone(logs_timezone)).strftime('%Y-%m-%d %H:%M:%S,%f')
            sdc_log = sdc_executor.get_logs().after_time(sdc_time)

            if sdc_log:
                logger.error('Error while %s the test %s. SDC log printed', reason, request.node.nodeid)
                print('------------------------- SDC log - Begins -----------------------')
                print(sdc_log)
                print('------------------------- SDC log - Ends -------------------------')
            else:
                logger.error('Error while %s the test %s. SDC log is empty', reason, request.node.nodeid)
    else:
        yield
