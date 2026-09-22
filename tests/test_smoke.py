"""冒烟测试：仅验证骨架可导入、服务可实例化。"""


def test_skeleton_boots(service):
    assert service is not None
