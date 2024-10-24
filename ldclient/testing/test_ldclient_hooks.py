from typing import Callable

from ldclient import Config, Context, LDClient
from ldclient.evaluation import EvaluationDetail
from ldclient.hook import EvaluationSeriesContext, Hook, Metadata
from ldclient.integrations.test_data import TestData
from ldclient.migrations import Stage


def record(label, log):
    def inner(*args, **kwargs):
        log.append(label)

    return inner


class MockHook(Hook):
    def __init__(self, before_evaluation: Callable[[EvaluationSeriesContext, dict], dict], after_evaluation: Callable[[EvaluationSeriesContext, dict, EvaluationDetail], dict]):
        self.__before_evaluation = before_evaluation
        self.__after_evaluation = after_evaluation

    @property
    def metadata(self) -> Metadata:
        return Metadata(name='test-hook')

    def before_evaluation(self, series_context: EvaluationSeriesContext, data: dict) -> dict:
        return self.__before_evaluation(series_context, data)

    def after_evaluation(self, series_context: EvaluationSeriesContext, data: dict, detail: EvaluationDetail) -> dict:
        return self.__after_evaluation(series_context, data, detail)


user = Context.from_dict({'key': 'userkey', 'kind': 'user'})


def test_verify_hook_execution_order():
    calls = []
    configHook1 = MockHook(before_evaluation=record('configHook1::before', calls), after_evaluation=record('configHook1::after', calls))
    configHook2 = MockHook(before_evaluation=record('configHook2::before', calls), after_evaluation=record('configHook2::after', calls))

    clientHook1 = MockHook(before_evaluation=record('clientHook1::before', calls), after_evaluation=record('clientHook1::after', calls))
    clientHook2 = MockHook(before_evaluation=record('clientHook2::before', calls), after_evaluation=record('clientHook2::after', calls))

    config = Config('SDK_KEY', update_processor_class=TestData.data_source(), send_events=False, hooks=[configHook1, configHook2])
    client = LDClient(config=config)
    client.add_hook(clientHook1)
    client.add_hook(clientHook2)

    client.variation('invalid', user, False)

    assert calls == ['configHook1::before', 'configHook2::before', 'clientHook1::before', 'clientHook2::before', 'clientHook2::after', 'clientHook1::after', 'configHook2::after', 'configHook1::after']


def test_ignores_invalid_hooks():
    calls = []
    hook = MockHook(before_evaluation=record('before', calls), after_evaluation=record('after', calls))

    config = Config('SDK_KEY', update_processor_class=TestData.data_source(), send_events=False, hooks=[True, hook, 42])
    client = LDClient(config=config)
    client.add_hook("Hook, Hook, give us the Hook!")
    client.add_hook(hook)
    client.add_hook(None)

    client.variation('invalid', user, False)

    assert calls == ['before', 'before', 'after', 'after']


def test_after_evaluation_receives_evaluation_detail():
    details = []
    hook = MockHook(before_evaluation=record('before', []), after_evaluation=lambda series_context, data, detail: details.append(detail))

    td = TestData.data_source()
    td.update(td.flag('flag-key').variation_for_all(True))

    config = Config('SDK_KEY', update_processor_class=td, send_events=False, hooks=[hook])
    client = LDClient(config=config)
    client.variation('flag-key', user, False)

    assert len(details) == 1
    assert details[0].value is True
    assert details[0].variation_index == 0


def test_passing_data_from_before_to_after():
    calls = []
    hook = MockHook(before_evaluation=lambda series_context, data: "from before", after_evaluation=lambda series_context, data, detail: calls.append(data))

    config = Config('SDK_KEY', update_processor_class=TestData.data_source(), send_events=False, hooks=[hook])
    client = LDClient(config=config)
    client.variation('flag-key', user, False)

    assert len(calls) == 1
    assert calls[0] == "from before"


def test_exception_in_before_passes_empty_dict():
    def raise_exception(series_context, data):
        raise Exception("error")

    calls = []
    hook = MockHook(before_evaluation=raise_exception, after_evaluation=lambda series_context, data, detail: calls.append(data))

    config = Config('SDK_KEY', update_processor_class=TestData.data_source(), send_events=False, hooks=[hook])
    client = LDClient(config=config)
    client.variation('flag-key', user, False)

    assert len(calls) == 1
    assert calls[0] == {}


def test_exceptions_do_not_affect_data_passing_order():
    def raise_exception(series_context, data):
        raise Exception("error")

    calls = []
    hook1 = MockHook(before_evaluation=lambda series_context, data: "first hook", after_evaluation=lambda series_context, data, detail: calls.append(data))
    hook2 = MockHook(before_evaluation=raise_exception, after_evaluation=lambda series_context, data, detail: calls.append(data))
    hook3 = MockHook(before_evaluation=lambda series_context, data: "third hook", after_evaluation=lambda series_context, data, detail: calls.append(data))

    config = Config('SDK_KEY', update_processor_class=TestData.data_source(), send_events=False, hooks=[hook1, hook2, hook3])
    client = LDClient(config=config)
    client.variation('flag-key', user, False)

    assert len(calls) == 3
    # NOTE: These are reversed since the push happens in the after_evaluation
    # (when hooks are reversed)
    assert calls[0] == "third hook"
    assert calls[1] == {}
    assert calls[2] == "first hook"


def test_migration_evaluation_detail_contains_stage_value():
    details = []
    hook = MockHook(before_evaluation=record('before', []), after_evaluation=lambda series_context, data, detail: details.append(detail))

    td = TestData.data_source()
    td.update(td.flag('flag-key').variations("off").variation_for_all(0))

    config = Config('SDK_KEY', update_processor_class=td, send_events=False, hooks=[hook])
    client = LDClient(config=config)
    client.migration_variation('flag-key', user, Stage.LIVE)

    assert len(details) == 1
    assert details[0].value == Stage.OFF.value
    assert details[0].variation_index == 0


def test_migration_evaluation_detail_gets_default_if_flag_isnt_migration_flag():
    details = []
    hook = MockHook(before_evaluation=record('before', []), after_evaluation=lambda series_context, data, detail: details.append(detail))

    td = TestData.data_source()
    td.update(td.flag('flag-key').variations("nonstage").variation_for_all(0))

    config = Config('SDK_KEY', update_processor_class=td, send_events=False, hooks=[hook])
    client = LDClient(config=config)
    client.migration_variation('flag-key', user, Stage.LIVE)

    assert len(details) == 1
    assert details[0].value == Stage.LIVE.value
    assert details[0].variation_index is None


def test_migration_evaluation_detail_default_converts_to_off_if_invalid():
    details = []
    hook = MockHook(before_evaluation=record('before', []), after_evaluation=lambda series_context, data, detail: details.append(detail))

    td = TestData.data_source()
    td.update(td.flag('flag-key').variations("nonstage").variation_for_all(0))

    config = Config('SDK_KEY', update_processor_class=td, send_events=False, hooks=[hook])
    client = LDClient(config=config)
    client.migration_variation('flag-key', user, "invalid")

    assert len(details) == 1
    assert details[0].value == Stage.OFF.value
    assert details[0].variation_index is None
