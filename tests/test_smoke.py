"""冒烟测试：骨架可导入、可实例化。"""
from pipeline_obs import ObservabilityService


def test_service_can_be_constructed():
    svc = ObservabilityService()
    assert svc is not None
