import sml2mqtt._signals
from sml2mqtt._signals import shutdown_with_exception
from sml2mqtt.errors import DeviceSetupFailed


def test_rest_code(caplog, monkeypatch):
    monkeypatch.setattr(sml2mqtt._signals, 'return_code', None)
    monkeypatch.setattr(sml2mqtt._signals, 'create_task', lambda x: x)

    shutdown_with_exception(DeviceSetupFailed)
    assert caplog.record_tuples == []
    assert sml2mqtt._signals.return_code == 10
