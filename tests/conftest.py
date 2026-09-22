"""pytest 公共夹具与时间线日志工具。"""

import logging

import pytest

from pipeline_obs import ObservabilityService, RetryPolicy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
timeline_log = logging.getLogger("timeline")


@pytest.fixture
def service():
    return ObservabilityService(retry_policy=RetryPolicy(max_attempts=3, base_delay_seconds=0.01))
