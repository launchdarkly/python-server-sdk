import json
import time
import uuid
from datetime import timedelta
from threading import Thread
from typing import Dict, Set

import pytest

from ldclient.config import Config
from ldclient.context import Context
from ldclient.evaluation import EvaluationDetail
from ldclient.impl.events.diagnostics import (_DiagnosticAccumulator,
                                              create_diagnostic_id)
from ldclient.impl.events.event_context_formatter import EventContextFormatter
from ldclient.impl.events.event_processor import DefaultEventProcessor
from ldclient.impl.events.types import (EventInput, EventInputCustom,
                                        EventInputEvaluation,
                                        EventInputIdentify)
from ldclient.impl.util import timedelta_millis
from ldclient.migrations.tracker import MigrationOpEvent
from ldclient.migrations.types import Operation, Origin, Stage
from ldclient.testing.builders import *
from ldclient.testing.proxy_test_util import do_proxy_tests
from ldclient.testing.stub_util import MockHttp

default_config = Config("fake_sdk_key")
context = Context.builder('userkey').name('Red').build()
flag = FlagBuilder('flagkey').version(2).build()
flag_with_0_sampling_ratio = FlagBuilder('flagkey').version(3).sampling_ratio(0).build()
flag_excluded_from_summaries = FlagBuilder('flagkey').version(4).exclude_from_summaries(True).build()
timestamp = 10000

ep = None
mock_http = None


def setup_function():
    global mock_http
    mock_http = MockHttp()


def teardown_function():
    if ep is not None:
        ep.stop()


def make_context_keys(context: Context) -> dict:
    ret = {}  # type: Dict[str, str]
    for i in range(context.individual_context_count):
        c = context.get_individual_context(i)
        if c is not None:
            ret[c.kind] = c.key
    return ret


class DefaultTestProcessor(DefaultEventProcessor):
    def __init__(self, **kwargs):
        if 'diagnostic_opt_out' not in kwargs:
            kwargs['diagnostic_opt_out'] = True
        if 'sdk_key' not in kwargs:
            kwargs['sdk_key'] = 'SDK_KEY'
        config = Config(**kwargs)
        diagnostic_accumulator = _DiagnosticAccumulator(create_diagnostic_id(config))
        DefaultEventProcessor.__init__(self, config, mock_http, diagnostic_accumulator=diagnostic_accumulator)


@pytest.mark.parametrize(
    "operation,default_stage",
    [
        pytest.param(Operation.READ, Stage.OFF, id="read off"),
        pytest.param(Operation.READ, Stage.DUALWRITE, id="read dualwrite"),
        pytest.param(Operation.READ, Stage.SHADOW, id="read shadow"),
        pytest.param(Operation.READ, Stage.LIVE, id="read live"),
        pytest.param(Operation.READ, Stage.RAMPDOWN, id="read rampdown"),
        pytest.param(Operation.READ, Stage.COMPLETE, id="read complete"),
        pytest.param(Operation.WRITE, Stage.OFF, id="write off"),
        pytest.param(Operation.WRITE, Stage.DUALWRITE, id="write dualwrite"),
        pytest.param(Operation.WRITE, Stage.SHADOW, id="write shadow"),
        pytest.param(Operation.WRITE, Stage.LIVE, id="write live"),
        pytest.param(Operation.WRITE, Stage.RAMPDOWN, id="write rampdown"),
        pytest.param(Operation.WRITE, Stage.COMPLETE, id="write complete"),
    ],
)
def test_migration_op_event_is_queued_without_flag(operation: Operation, default_stage: Stage):
    with DefaultTestProcessor() as ep:
        e = MigrationOpEvent(timestamp, context, "key", None, operation, default_stage, EvaluationDetail('off', 0, {'kind': 'FALLTHROUGH'}), {Origin.OLD}, None, None, set(), {})
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 1
        check_migration_op_event(output[0], e)


@pytest.mark.parametrize(
    "operation,default_stage,invoked",
    [
        pytest.param(Operation.READ, Stage.OFF, {Origin.OLD}, id="read off"),
        pytest.param(Operation.READ, Stage.DUALWRITE, {Origin.OLD}, id="read dualwrite"),
        pytest.param(Operation.READ, Stage.SHADOW, {Origin.OLD, Origin.NEW}, id="read shadow"),
        pytest.param(Operation.READ, Stage.LIVE, {Origin.OLD, Origin.NEW}, id="read live"),
        pytest.param(Operation.READ, Stage.RAMPDOWN, {Origin.NEW}, id="read rampdown"),
        pytest.param(Operation.READ, Stage.COMPLETE, {Origin.NEW}, id="read complete"),
        pytest.param(Operation.WRITE, Stage.OFF, {Origin.OLD}, id="write off"),
        pytest.param(Operation.WRITE, Stage.DUALWRITE, {Origin.OLD, Origin.NEW}, id="write dualwrite"),
        pytest.param(Operation.WRITE, Stage.SHADOW, {Origin.OLD, Origin.NEW}, id="write shadow"),
        pytest.param(Operation.WRITE, Stage.LIVE, {Origin.OLD, Origin.NEW}, id="write live"),
        pytest.param(Operation.WRITE, Stage.RAMPDOWN, {Origin.OLD, Origin.NEW}, id="write rampdown"),
        pytest.param(Operation.WRITE, Stage.COMPLETE, {Origin.OLD, Origin.NEW}, id="write complete"),
    ],
)
def test_migration_op_event_is_queued_with_invoked(operation: Operation, default_stage: Stage, invoked: Set[Origin]):
    with DefaultTestProcessor() as ep:
        e = MigrationOpEvent(timestamp, context, flag.key, flag, operation, default_stage, EvaluationDetail('off', 0, {'kind': 'FALLTHROUGH'}), invoked, None, None, set(), {})
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 1
        check_migration_op_event(output[0], e)


@pytest.mark.parametrize(
    "operation,default_stage,errors",
    [
        pytest.param(Operation.READ, Stage.OFF, {Origin.OLD}, id="read off"),
        pytest.param(Operation.READ, Stage.DUALWRITE, {Origin.OLD}, id="read dualwrite"),
        pytest.param(Operation.READ, Stage.SHADOW, {Origin.OLD, Origin.NEW}, id="read shadow"),
        pytest.param(Operation.READ, Stage.LIVE, {Origin.OLD, Origin.NEW}, id="read live"),
        pytest.param(Operation.READ, Stage.RAMPDOWN, {Origin.NEW}, id="read rampdown"),
        pytest.param(Operation.READ, Stage.COMPLETE, {Origin.NEW}, id="read complete"),
        pytest.param(Operation.WRITE, Stage.OFF, {Origin.OLD}, id="write off"),
        pytest.param(Operation.WRITE, Stage.DUALWRITE, {Origin.OLD}, id="write dualwrite"),
        pytest.param(Operation.WRITE, Stage.SHADOW, {Origin.OLD}, id="write shadow"),
        pytest.param(Operation.WRITE, Stage.LIVE, {Origin.NEW}, id="write live"),
        pytest.param(Operation.WRITE, Stage.RAMPDOWN, {Origin.NEW}, id="write rampdown"),
        pytest.param(Operation.WRITE, Stage.COMPLETE, {Origin.NEW}, id="write complete"),
    ],
)
def test_migration_op_event_is_queued_with_errors(operation: Operation, default_stage: Stage, errors: Set[Origin]):
    with DefaultTestProcessor() as ep:
        e = MigrationOpEvent(timestamp, context, flag.key, flag, operation, default_stage, EvaluationDetail('off', 0, {'kind': 'FALLTHROUGH'}), {Origin.OLD, Origin.NEW}, None, None, errors, {})
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 1
        check_migration_op_event(output[0], e)


@pytest.mark.parametrize(
    "operation,default_stage,latencies",
    [
        pytest.param(Operation.READ, Stage.OFF, {Origin.OLD: 100}, id="read off"),
        pytest.param(Operation.READ, Stage.DUALWRITE, {Origin.OLD: 100}, id="read dualwrite"),
        pytest.param(Operation.READ, Stage.SHADOW, {Origin.OLD: 100, Origin.NEW: 100}, id="read shadow"),
        pytest.param(Operation.READ, Stage.LIVE, {Origin.OLD: 100, Origin.NEW: 100}, id="read live"),
        pytest.param(Operation.READ, Stage.RAMPDOWN, {Origin.NEW: 100}, id="read rampdown"),
        pytest.param(Operation.READ, Stage.COMPLETE, {Origin.NEW: 100}, id="read complete"),
        pytest.param(Operation.WRITE, Stage.OFF, {Origin.OLD: 100}, id="write off"),
        pytest.param(Operation.WRITE, Stage.DUALWRITE, {Origin.OLD: 100, Origin.NEW: 100}, id="write dualwrite"),
        pytest.param(Operation.WRITE, Stage.SHADOW, {Origin.OLD: 100, Origin.NEW: 100}, id="write shadow"),
        pytest.param(Operation.WRITE, Stage.LIVE, {Origin.OLD: 100, Origin.NEW: 100}, id="write live"),
        pytest.param(Operation.WRITE, Stage.RAMPDOWN, {Origin.OLD: 100, Origin.NEW: 100}, id="write rampdown"),
        pytest.param(Operation.WRITE, Stage.COMPLETE, {Origin.NEW: 100}, id="write complete"),
    ],
)
def test_migration_op_event_is_queued_with_latencies(operation: Operation, default_stage: Stage, latencies: Dict[Origin, float]):
    with DefaultTestProcessor() as ep:
        delta_latencies = {origin: timedelta(milliseconds=ms) for origin, ms in latencies.items()}
        e = MigrationOpEvent(
            timestamp, context, flag.key, flag, operation, default_stage, EvaluationDetail('off', 0, {'kind': 'FALLTHROUGH'}), {Origin.OLD, Origin.NEW}, None, None, set(), delta_latencies
        )
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 1
        check_migration_op_event(output[0], e)


def test_migration_op_event_is_disabled_with_sampling_ratio():
    with DefaultTestProcessor() as ep:
        e = MigrationOpEvent(
            timestamp,
            context,
            flag_with_0_sampling_ratio.key,
            flag_with_0_sampling_ratio,
            Operation.READ,
            Stage.OFF,
            EvaluationDetail('off', 0, {'kind': 'FALLTHROUGH'}),
            {Origin.OLD},
            None,
            None,
            set(),
            {},
        )
        ep.send_event(e)

        # NOTE: Have to send an identify event; otherwise, we will timeout waiting on no events.
        identify_event = EventInputIdentify(timestamp, context)
        ep.send_event(identify_event)

        output = flush_and_get_events(ep)
        assert len(output) == 1  # Got the identify but not the migration op
        check_identify_event(output[0], identify_event)


@pytest.mark.parametrize(
    "operation,default_stage",
    [
        pytest.param(Operation.READ, Stage.OFF, id="read off"),
        pytest.param(Operation.READ, Stage.DUALWRITE, id="read dualwrite"),
        pytest.param(Operation.READ, Stage.SHADOW, id="read shadow"),
        pytest.param(Operation.READ, Stage.LIVE, id="read live"),
        pytest.param(Operation.READ, Stage.RAMPDOWN, id="read rampdown"),
        pytest.param(Operation.READ, Stage.COMPLETE, id="read complete"),
        pytest.param(Operation.WRITE, Stage.OFF, id="write off"),
        pytest.param(Operation.WRITE, Stage.DUALWRITE, id="write dualwrite"),
        pytest.param(Operation.WRITE, Stage.SHADOW, id="write shadow"),
        pytest.param(Operation.WRITE, Stage.LIVE, id="write live"),
        pytest.param(Operation.WRITE, Stage.RAMPDOWN, id="write rampdown"),
        pytest.param(Operation.WRITE, Stage.COMPLETE, id="write complete"),
    ],
)
def test_migration_op_event_is_queued_with_consistency(operation: Operation, default_stage: Stage):
    for value in [True, False, None]:
        with DefaultTestProcessor() as ep:
            e = MigrationOpEvent(timestamp, context, flag.key, flag, operation, default_stage, EvaluationDetail('off', 0, {'kind': 'FALLTHROUGH'}), {Origin.OLD, Origin.NEW}, value, None, set(), {})
            ep.send_event(e)

            output = flush_and_get_events(ep)
            assert len(output) == 1
            check_migration_op_event(output[0], e)


def test_identify_event_is_queued():
    with DefaultTestProcessor() as ep:
        e = EventInputIdentify(timestamp, context)
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 1
        check_identify_event(output[0], e)


def test_context_is_filtered_in_identify_event():
    with DefaultTestProcessor(all_attributes_private=True) as ep:
        formatter = EventContextFormatter(True, [])
        e = EventInputIdentify(timestamp, context)
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 1
        check_identify_event(output[0], e, formatter.format_context(context))


def test_omit_anonymous_contexts_suppresses_identify_event():
    with DefaultTestProcessor(omit_anonymous_contexts=True) as ep:
        anon_context = Context.builder('userkey').name('Red').anonymous(True).build()
        e = EventInputIdentify(timestamp, anon_context)
        ep.send_event(e)

        try:
            flush_and_get_events(ep)
            pytest.fail("Expected no events")
        except AssertionError:
            pass


def test_omit_anonymous_contexts_strips_anonymous_contexts_correctly():
    with DefaultTestProcessor(omit_anonymous_contexts=True) as ep:
        a = Context.builder('a').kind('a').anonymous(True).build()
        b = Context.builder('b').kind('b').anonymous(True).build()
        c = Context.builder('c').kind('c').anonymous(False).build()
        mc = Context.multi_builder().add(a).add(b).add(c).build()

        e = EventInputIdentify(timestamp, mc)
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 1

        formatter = EventContextFormatter(True, [])
        check_identify_event(output[0], e, formatter.format_context(c))


def test_individual_feature_event_is_queued_with_index_event():
    with DefaultTestProcessor() as ep:
        e = EventInputEvaluation(timestamp, context, flag.key, flag, 1, 'value', None, 'default', None, True)
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 3
        check_index_event(output[0], e)
        check_feature_event(output[1], e)
        check_summary_event(output[2])


def test_omit_anonymous_context_emits_feature_event_without_index():
    with DefaultTestProcessor(omit_anonymous_contexts=True) as ep:
        anon = Context.builder('a').anonymous(True).build()
        e = EventInputEvaluation(timestamp, anon, flag.key, flag, 1, 'value', None, 'default', None, True)
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 2
        check_feature_event(output[0], e)
        check_summary_event(output[1])


def test_omit_anonymous_context_strips_anonymous_from_index_event():
    with DefaultTestProcessor(omit_anonymous_contexts=True) as ep:
        a = Context.builder('a').kind('a').anonymous(True).build()
        b = Context.builder('b').kind('b').anonymous(True).build()
        c = Context.builder('c').kind('c').anonymous(False).build()
        mc = Context.multi_builder().add(a).add(b).add(c).build()
        e = EventInputEvaluation(timestamp, mc, flag.key, flag, 1, 'value', None, 'default', None, True)
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 3
        check_index_event(output[0], e, c.to_dict())  # Should only contain non-anon context
        check_feature_event(output[1], e)
        check_summary_event(output[2])


def test_individual_feature_event_is_ignored_for_0_sampling_ratio():
    with DefaultTestProcessor() as ep:
        e = EventInputEvaluation(timestamp, context, flag_with_0_sampling_ratio.key, flag_with_0_sampling_ratio, 1, 'value', None, 'default', None, True)
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 2
        check_index_event(output[0], e)
        check_summary_event(output[1])


def test_exclude_can_keep_feature_event_from_summary():
    with DefaultTestProcessor() as ep:
        e = EventInputEvaluation(timestamp, context, flag_excluded_from_summaries.key, flag_excluded_from_summaries, 1, 'value', None, 'default', None, True)
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 2
        check_index_event(output[0], e)
        check_feature_event(output[1], e)


def test_context_is_filtered_in_index_event():
    with DefaultTestProcessor(all_attributes_private=True) as ep:
        formatter = EventContextFormatter(True, [])
        e = EventInputEvaluation(timestamp, context, flag.key, flag, 1, 'value', None, 'default', None, True)
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 3
        check_index_event(output[0], e, formatter.format_context(context))
        check_feature_event(output[1], e, formatter.format_context(context))
        check_summary_event(output[2])


def test_two_events_for_same_context_only_produce_one_index_event():
    with DefaultTestProcessor(context_keys_flush_interval=300) as ep:
        e0 = EventInputEvaluation(timestamp, context, flag.key, flag, 1, 'value1', None, 'default', None, True)
        e1 = EventInputEvaluation(timestamp, context, flag.key, flag, 2, 'value2', None, 'default', None, True)
        ep.send_event(e0)
        ep.send_event(e1)

        output = flush_and_get_events(ep)
        assert len(output) == 4
        check_index_event(output[0], e0)
        check_feature_event(output[1], e0)
        check_feature_event(output[2], e1)
        check_summary_event(output[3])


def test_new_index_event_is_added_if_context_cache_has_been_cleared():
    with DefaultTestProcessor(context_keys_flush_interval=0.1) as ep:
        e0 = EventInputEvaluation(timestamp, context, flag.key, flag, 1, 'value1', None, 'default', None, True)
        e1 = EventInputEvaluation(timestamp, context, flag.key, flag, 2, 'value2', None, 'default', None, True)
        ep.send_event(e0)
        time.sleep(0.2)
        ep.send_event(e1)

        output = flush_and_get_events(ep)
        assert len(output) == 5
        check_index_event(output[0], e0)
        check_feature_event(output[1], e0)
        check_index_event(output[2], e1)
        check_feature_event(output[3], e1)
        check_summary_event(output[4])


def test_event_kind_is_debug_if_flag_is_temporarily_in_debug_mode():
    with DefaultTestProcessor() as ep:
        future_time = now() + 100000
        debugged_flag = FlagBuilder(flag.key).version(flag.version).debug_events_until_date(future_time).build()
        e = EventInputEvaluation(timestamp, context, debugged_flag.key, debugged_flag, 1, 'value', None, 'default', None, False)
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 3
        check_index_event(output[0], e)
        check_debug_event(output[1], e)
        check_summary_event(output[2])


def test_event_can_be_both_tracked_and_debugged():
    with DefaultTestProcessor() as ep:
        future_time = now() + 100000
        debugged_flag = FlagBuilder(flag.key).version(flag.version).debug_events_until_date(future_time).build()
        e = EventInputEvaluation(timestamp, context, debugged_flag.key, debugged_flag, 1, 'value', None, 'default', None, True)
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 4
        check_index_event(output[0], e)
        check_feature_event(output[1], e)
        check_debug_event(output[2], e)
        check_summary_event(output[3])


def test_debug_event_can_be_disabled_with_sampling_ratio():
    with DefaultTestProcessor() as ep:
        future_time = now() + 100000
        debugged_flag = FlagBuilder(flag.key).version(flag.version).debug_events_until_date(future_time).sampling_ratio(0).build()
        e = EventInputEvaluation(timestamp, context, debugged_flag.key, debugged_flag, 1, 'value', None, 'default', None, True)
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 2
        check_index_event(output[0], e)
        check_summary_event(output[1])


def test_debug_mode_does_not_expire_if_both_client_time_and_server_time_are_before_expiration_time():
    with DefaultTestProcessor() as ep:
        # Pick a server time that slightly different from client time
        server_time = now() + 1000

        # Send and flush an event we don't care about, just to set the last server time
        mock_http.set_server_time(server_time)
        ep.send_event(EventInputIdentify(timestamp, Context.create('otherUser')))
        flush_and_get_events(ep)

        # Now send an event with debug mode on, with a "debug until" time that is further in
        # the future than both the client time and the server time
        debug_until = server_time + 10000
        debugged_flag = FlagBuilder(flag.key).version(flag.version).debug_events_until_date(debug_until).build()
        e = EventInputEvaluation(timestamp, context, debugged_flag.key, debugged_flag, 1, 'value', None, 'default', None, False)
        ep.send_event(e)

        # Should get a summary event only, not a full feature event
        output = flush_and_get_events(ep)
        assert len(output) == 3
        check_index_event(output[0], e)
        check_debug_event(output[1], e)
        check_summary_event(output[2])


def test_debug_mode_expires_based_on_client_time_if_client_time_is_later_than_server_time():
    with DefaultTestProcessor() as ep:
        # Pick a server time that is somewhat behind the client time
        server_time = now() - 20000

        # Send and flush an event we don't care about, just to set the last server time
        mock_http.set_server_time(server_time)
        ep.send_event(EventInputIdentify(timestamp, Context.create('otherUser')))
        flush_and_get_events(ep)

        # Now send an event with debug mode on, with a "debug until" time that is further in
        # the future than the server time, but in the past compared to the client.
        debug_until = server_time + 1000
        debugged_flag = FlagBuilder(flag.key).version(flag.version).debug_events_until_date(debug_until).build()
        e = EventInputEvaluation(timestamp, context, debugged_flag.key, debugged_flag, 1, 'value', None, 'default', None, False)
        ep.send_event(e)

        # Should get a summary event only, not a full feature event
        output = flush_and_get_events(ep)
        assert len(output) == 2
        check_index_event(output[0], e)
        check_summary_event(output[1])


def test_debug_mode_expires_based_on_server_time_if_server_time_is_later_than_client_time():
    with DefaultTestProcessor() as ep:
        # Pick a server time that is somewhat ahead of the client time
        server_time = now() + 20000

        # Send and flush an event we don't care about, just to set the last server time
        mock_http.set_server_time(server_time)
        ep.send_event(EventInputIdentify(timestamp, Context.create('otherUser')))
        flush_and_get_events(ep)

        # Now send an event with debug mode on, with a "debug until" time that is further in
        # the future than the client time, but in the past compared to the server.
        debug_until = server_time - 1000
        debugged_flag = FlagBuilder(flag.key).version(flag.version).debug_events_until_date(debug_until).build()
        e = EventInputEvaluation(timestamp, context, debugged_flag.key, debugged_flag, 1, 'value', None, 'default', None, False)
        ep.send_event(e)

        # Should get a summary event only, not a full feature event
        output = flush_and_get_events(ep)
        assert len(output) == 2
        check_index_event(output[0], e)
        check_summary_event(output[1])


def test_nontracked_events_are_summarized():
    with DefaultTestProcessor() as ep:
        flag1 = FlagBuilder('flagkey1').version(11).build()
        flag2 = FlagBuilder('flagkey2').version(22).build()
        earlier_time, later_time = 1111111, 2222222
        e1 = EventInputEvaluation(later_time, context, flag1.key, flag1, 1, 'value1', None, 'default1', None, False)
        e2 = EventInputEvaluation(earlier_time, context, flag2.key, flag2, 2, 'value2', None, 'default2', None, False)
        ep.send_event(e1)
        ep.send_event(e2)

        output = flush_and_get_events(ep)
        assert len(output) == 2
        check_index_event(output[0], e1)
        se = output[1]
        assert se['kind'] == 'summary'
        assert se['startDate'] == earlier_time
        assert se['endDate'] == later_time
        assert se['features'] == {
            'flagkey1': {'contextKinds': ['user'], 'default': 'default1', 'counters': [{'version': 11, 'variation': 1, 'value': 'value1', 'count': 1}]},
            'flagkey2': {'contextKinds': ['user'], 'default': 'default2', 'counters': [{'version': 22, 'variation': 2, 'value': 'value2', 'count': 1}]},
        }


def test_custom_event_is_queued_with_user():
    with DefaultTestProcessor() as ep:
        e = EventInputCustom(timestamp, context, 'eventkey', {'thing': 'stuff '}, 1.5)
        ep.send_event(e)

        output = flush_and_get_events(ep)
        assert len(output) == 2
        check_index_event(output[0], e)
        check_custom_event(output[1], e)


def test_nothing_is_sent_if_there_are_no_events():
    with DefaultTestProcessor() as ep:
        ep.flush()
        ep._wait_until_inactive()
        assert mock_http.request_data is None


def test_sdk_key_is_sent():
    with DefaultTestProcessor(sdk_key='SDK_KEY') as ep:
        ep.send_event(EventInputIdentify(timestamp, context))
        ep.flush()
        ep._wait_until_inactive()

        assert mock_http.request_headers.get('Authorization') == 'SDK_KEY'


def test_wrapper_header_not_sent_when_not_set():
    with DefaultTestProcessor() as ep:
        ep.send_event(EventInputIdentify(timestamp, context))
        ep.flush()
        ep._wait_until_inactive()

        assert mock_http.request_headers.get('X-LaunchDarkly-Wrapper') is None


def test_wrapper_header_sent_when_set():
    with DefaultTestProcessor(wrapper_name="Flask", wrapper_version="0.0.1") as ep:
        ep.send_event(EventInputIdentify(timestamp, context))
        ep.flush()
        ep._wait_until_inactive()

        assert mock_http.request_headers.get('X-LaunchDarkly-Wrapper') == "Flask/0.0.1"


def test_wrapper_header_sent_without_version():
    with DefaultTestProcessor(wrapper_name="Flask") as ep:
        ep.send_event(EventInputIdentify(timestamp, context))
        ep.flush()
        ep._wait_until_inactive()

        assert mock_http.request_headers.get('X-LaunchDarkly-Wrapper') == "Flask"


def test_event_schema_set_on_event_send():
    with DefaultTestProcessor() as ep:
        ep.send_event(EventInputIdentify(timestamp, context))
        ep.flush()
        ep._wait_until_inactive()

        assert mock_http.request_headers.get('X-LaunchDarkly-Event-Schema') == "4"


def test_sdk_key_is_sent_on_diagnostic_request():
    with DefaultTestProcessor(sdk_key='SDK_KEY', diagnostic_opt_out=False) as ep:
        ep._wait_until_inactive()
        assert mock_http.request_headers.get('Authorization') == 'SDK_KEY'


def test_event_schema_not_set_on_diagnostic_send():
    with DefaultTestProcessor(diagnostic_opt_out=False) as ep:
        ep._wait_until_inactive()
        assert mock_http.request_headers.get('X-LaunchDarkly-Event-Schema') is None


def test_init_diagnostic_event_sent():
    with DefaultTestProcessor(diagnostic_opt_out=False) as ep:
        diag_init = flush_and_get_events(ep)
        # Fields are tested in test_diagnostics.py
        assert len(diag_init) == 6
        assert diag_init['kind'] == 'diagnostic-init'


def test_periodic_diagnostic_includes_events_in_batch():
    with DefaultTestProcessor(diagnostic_opt_out=False) as ep:
        # Ignore init event
        flush_and_get_events(ep)
        # Send a payload with a single event
        ep.send_event(EventInputIdentify(timestamp, context))
        flush_and_get_events(ep)

        ep._send_diagnostic()
        diag_event = flush_and_get_events(ep)
        assert len(diag_event) == 8
        assert diag_event['kind'] == 'diagnostic'
        assert diag_event['eventsInLastBatch'] == 1
        assert diag_event['deduplicatedUsers'] == 0


def test_periodic_diagnostic_includes_deduplicated_users():
    with DefaultTestProcessor(diagnostic_opt_out=False) as ep:
        # Ignore init event
        flush_and_get_events(ep)
        # Send two custom events with the same user to cause a user deduplication
        e0 = EventInputCustom(timestamp, context, 'event1', None, None)
        e1 = EventInputCustom(timestamp, context, 'event2', None, None)
        ep.send_event(e0)
        ep.send_event(e1)
        flush_and_get_events(ep)

        ep._send_diagnostic()
        diag_event = flush_and_get_events(ep)
        assert len(diag_event) == 8
        assert diag_event['kind'] == 'diagnostic'
        assert diag_event['eventsInLastBatch'] == 3
        assert diag_event['deduplicatedUsers'] == 1


def test_no_more_payloads_are_sent_after_401_error():
    verify_unrecoverable_http_error(401)


def test_no_more_payloads_are_sent_after_403_error():
    verify_unrecoverable_http_error(403)


def test_will_still_send_after_408_error():
    verify_recoverable_http_error(408)


def test_will_still_send_after_429_error():
    verify_recoverable_http_error(429)


def test_will_still_send_after_500_error():
    verify_recoverable_http_error(500)


def test_does_not_block_on_full_inbox():
    config = Config("fake_sdk_key", events_max_pending=1)  # this sets the size of both the inbox and the outbox to 1
    ep_inbox_holder = [None]
    ep_inbox = None

    def dispatcher_factory(inbox, config, http, diag):
        ep_inbox_holder[0] = inbox  # it's an array because otherwise it's hard for a closure to modify a variable
        return None  # the dispatcher object itself doesn't matter, we only manipulate the inbox

    def event_consumer():
        while True:
            message = ep_inbox.get(block=True)
            if message.type == 'stop':
                message.param.set()
                return

    def start_consuming_events():
        Thread(target=event_consumer, name="ldclient.testing.events.consumer").start()

    with DefaultEventProcessor(config, mock_http, dispatcher_factory) as ep:
        ep_inbox = ep_inbox_holder[0]
        event1 = EventInputCustom(timestamp, context, 'event1')
        event2 = EventInputCustom(timestamp, context, 'event2')
        ep.send_event(event1)
        ep.send_event(event2)  # this event should be dropped - inbox is full
        message1 = ep_inbox.get(block=False)
        had_no_more = ep_inbox.empty()
        start_consuming_events()
        assert message1.param == event1
        assert had_no_more


def test_http_proxy(monkeypatch):
    def _event_processor_proxy_test(server, config, secure):
        with DefaultEventProcessor(config) as ep:
            ep.send_event(EventInputIdentify(timestamp, context))
            ep.flush()
            ep._wait_until_inactive()

    do_proxy_tests(_event_processor_proxy_test, 'POST', monkeypatch)


def verify_unrecoverable_http_error(status):
    with DefaultTestProcessor(sdk_key='SDK_KEY') as ep:
        mock_http.set_response_status(status)
        ep.send_event(EventInputIdentify(timestamp, context))
        ep.flush()
        ep._wait_until_inactive()
        mock_http.reset()

        ep.send_event(EventInputIdentify(timestamp, context))
        ep.flush()
        ep._wait_until_inactive()
        assert mock_http.request_data is None


def verify_recoverable_http_error(status):
    with DefaultTestProcessor(sdk_key='SDK_KEY') as ep:
        mock_http.set_response_status(status)
        ep.send_event(EventInputIdentify(timestamp, context))
        ep.flush()
        ep._wait_until_inactive()
        mock_http.reset()

        ep.send_event(EventInputIdentify(timestamp, context))
        ep.flush()
        ep._wait_until_inactive()
        assert mock_http.request_data is not None


def test_event_payload_id_is_sent():
    with DefaultEventProcessor(Config(sdk_key='SDK_KEY'), mock_http) as ep:
        ep.send_event(EventInputIdentify(timestamp, context))
        ep.flush()
        ep._wait_until_inactive()

        headerVal = mock_http.request_headers.get('X-LaunchDarkly-Payload-ID')
        assert headerVal is not None
        # Throws on invalid UUID
        uuid.UUID(headerVal)


def test_event_payload_id_changes_between_requests():
    with DefaultEventProcessor(Config(sdk_key='SDK_KEY'), mock_http) as ep:
        ep.send_event(EventInputIdentify(timestamp, context))
        ep.flush()
        ep._wait_until_inactive()

        ep.send_event(EventInputIdentify(timestamp, context))
        ep.flush()
        ep._wait_until_inactive()

        firstPayloadId = mock_http.recorded_requests[0][0].get('X-LaunchDarkly-Payload-ID')
        secondPayloadId = mock_http.recorded_requests[1][0].get('X-LaunchDarkly-Payload-ID')
        assert firstPayloadId != secondPayloadId


def flush_and_get_events(ep):
    ep.flush()
    ep._wait_until_inactive()
    if mock_http.request_data is None:
        raise AssertionError('Expected to get an HTTP request but did not get one')
    else:
        return json.loads(mock_http.request_data)


def check_identify_event(data, source: EventInput, context_json: Optional[dict] = None):
    assert data['kind'] == 'identify'
    assert data['creationDate'] == source.timestamp
    assert data['context'] == (source.context.to_dict() if context_json is None else context_json)


def check_index_event(data, source: EventInput, context_json: Optional[dict] = None):
    assert data['kind'] == 'index'
    assert data['creationDate'] == source.timestamp
    assert data['context'] == (source.context.to_dict() if context_json is None else context_json)


def check_feature_event(data, source: EventInputEvaluation, context_json: Optional[dict] = None):
    assert data['kind'] == 'feature'
    assert data['creationDate'] == source.timestamp
    assert data['key'] == source.key
    assert data.get('version') is None if source.flag is None else source.flag.version
    assert data.get('variation') == source.variation
    assert data.get('value') == source.value
    assert data.get('default') == source.default_value
    assert data['context'] == (source.context.to_dict() if context_json is None else context_json)
    assert data.get('prereq_of') is None if source.prereq_of is None else source.prereq_of.key


def check_migration_op_event(data, source: MigrationOpEvent):
    assert data['kind'] == 'migration_op'
    assert data['creationDate'] == source.timestamp
    assert data['contextKeys'] == make_context_keys(source.context)
    assert data['evaluation']['key'] == source.key
    assert data['evaluation']['value'] == source.detail.value

    if source.flag is not None:
        assert data['evaluation']['version'] == source.flag.version

    if source.default_stage is not None:
        assert data['evaluation']['default'] == source.default_stage.value

    if source.detail.variation_index is not None:
        assert data['evaluation']['variation'] == source.detail.variation_index

    if source.detail.reason is not None:
        assert data['evaluation']['reason'] == source.detail.reason

    if source.flag is not None and source.flag.sampling_ratio is not None and source.flag.sampling_ratio != 1:
        assert data['samplingRatio'] == source.flag.sampling_ratio

    index = 0
    if len(source.invoked):
        assert data['measurements'][index]['key'] == 'invoked'
        assert data['measurements'][index]['values'] == {origin.value: True for origin in source.invoked}
        index += 1

    if source.consistent is not None:
        assert data['measurements'][index]['key'] == 'consistent'
        assert data['measurements'][index]['value'] == source.consistent

        if source.flag is not None and source.flag.migrations is not None:
            check_ratio = source.flag.migrations.check_ratio
            if check_ratio is not None and check_ratio != 1:
                assert data['measurements'][index]['samplingRatio'] == check_ratio

        index += 1

    if len(source.latencies):
        assert data['measurements'][index]['key'] == 'latency_ms'
        assert data['measurements'][index]['values'] == {o.value: timedelta_millis(d) for o, d in source.latencies.items()}
        index += 1

    if len(source.errors):
        assert data['measurements'][index]['key'] == 'error'
        assert data['measurements'][index]['values'] == {origin.value: True for origin in source.errors}


def check_debug_event(data, source: EventInputEvaluation, context_json: Optional[dict] = None):
    assert data['kind'] == 'debug'
    assert data['creationDate'] == source.timestamp
    assert data['key'] == source.key
    assert data.get('version') is None if source.flag is None else source.flag.version
    assert data.get('variation') == source.variation
    assert data.get('value') == source.value
    assert data.get('default') == source.default_value
    assert data['context'] == (source.context.to_dict() if context_json is None else context_json)
    assert data.get('prereq_of') is None if source.prereq_of is None else source.prereq_of.key


def check_custom_event(data, source: EventInputCustom):
    assert data['kind'] == 'custom'
    assert data['creationDate'] == source.timestamp
    assert data['key'] == source.key
    assert data['data'] == source.data
    assert data['contextKeys'] == make_context_keys(source.context)
    assert data.get('metricValue') == source.metric_value


def check_summary_event(data):
    assert data['kind'] == 'summary'


def now():
    return int(time.time() * 1000)
