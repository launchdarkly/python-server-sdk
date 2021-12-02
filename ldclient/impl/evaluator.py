from ldclient import operators
from ldclient.evaluation import BigSegmentsStatus, EvaluationDetail
from ldclient.impl.event_factory import _EventFactory
from ldclient.util import stringify_attrs

from collections import namedtuple
import hashlib
import logging
from typing import Callable, Optional, Tuple

# For consistency with past logging behavior, we are pretending that the evaluation logic still lives in
# the ldclient.flag module.
log = logging.getLogger('ldclient.flag')

__LONG_SCALE__ = float(0xFFFFFFFFFFFFFFF)

__BUILTINS__ = ["key", "ip", "country", "email",
                "firstName", "lastName", "avatar", "name", "anonymous"]

__USER_ATTRS_TO_STRINGIFY_FOR_EVALUATION__ = [ "key", "secondary" ]
# Currently we are not stringifying the rest of the built-in attributes prior to evaluation, only for events.
# This is because it could affect evaluation results for existing users (ch35206).


# EvalResult is used internally to hold the EvaluationDetail result of an evaluation along with
# other side effects that are not exposed to the application, such as events generated by
# prerequisite evaluations, and the cached state of any Big Segments query that we may have
# ended up having to do for the user.
class EvalResult:
    def __init__(self):
        self.detail = None
        self.events = None
        self.big_segments_status = None
        self.big_segments_membership = None

    def add_event(self, event):
        if self.events is None:
            self.events = []
        self.events.append(event)


class Evaluator:
    """
    Encapsulates the feature flag evaluation logic. The Evaluator has no knowledge of the rest of the SDK environment;
    if it needs to retrieve flags or segments that are referenced by a flag, it does so through a read-only interface
    that is provided in the constructor. It also produces feature events as appropriate for any referenced prerequisite
    flags, but does not send them.
    """
    def __init__(
        self,
        get_flag: Callable[[str], Optional[dict]],
        get_segment: Callable[[str], Optional[dict]],
        get_big_segments_membership: Callable[[str], Optional[Tuple[dict, BigSegmentsStatus]]]
    ):
        self.__get_flag = get_flag
        self.__get_segment = get_segment
        self.__get_big_segments_membership = get_big_segments_membership

    def evaluate(self, flag: dict, user: dict, event_factory: _EventFactory) -> EvalResult:
        sanitized_user = stringify_attrs(user, __USER_ATTRS_TO_STRINGIFY_FOR_EVALUATION__)
        state = EvalResult()
        state.detail = self._evaluate(flag, sanitized_user, state, event_factory)
        if state.big_segments_status is not None:
            state.detail.reason['bigSegmentsStatus'] = state.big_segments_status
        return state

    def _evaluate(self, flag: dict, user: dict, state: EvalResult, event_factory: _EventFactory):
        if not flag.get('on', False):
            return _get_off_value(flag, {'kind': 'OFF'})

        prereq_failure_reason = self._check_prerequisites(flag, user, state, event_factory)
        if prereq_failure_reason is not None:
            return _get_off_value(flag, prereq_failure_reason)

        # Check to see if any user targets match:
        for target in flag.get('targets') or []:
            for value in target.get('values') or []:
                if value == user['key']:
                    return _get_variation(flag, target.get('variation'), {'kind': 'TARGET_MATCH'})

        # Now walk through the rules to see if any match
        for index, rule in enumerate(flag.get('rules') or []):
            if self._rule_matches_user(rule, user, state):
                return _get_value_for_variation_or_rollout(flag, rule, user,
                    {'kind': 'RULE_MATCH', 'ruleIndex': index, 'ruleId': rule.get('id')})

        # Walk through fallthrough and see if it matches
        if flag.get('fallthrough') is not None:
            return _get_value_for_variation_or_rollout(flag, flag['fallthrough'], user, {'kind': 'FALLTHROUGH'})

    def _check_prerequisites(self, flag: dict, user: dict, state: EvalResult, event_factory: _EventFactory):
        failed_prereq = None
        prereq_res = None
        for prereq in flag.get('prerequisites') or []:
            prereq_flag = self.__get_flag(prereq.get('key'))
            if prereq_flag is None:
                log.warning("Missing prereq flag: " + prereq.get('key'))
                failed_prereq = prereq
            else:
                prereq_res = self._evaluate(prereq_flag, user, state, event_factory)
                # Note that if the prerequisite flag is off, we don't consider it a match no matter what its
                # off variation was. But we still need to evaluate it in order to generate an event.
                if (not prereq_flag.get('on', False)) or prereq_res.variation_index != prereq.get('variation'):
                    failed_prereq = prereq
                event = event_factory.new_eval_event(prereq_flag, user, prereq_res, None, flag)
                state.add_event(event)
            if failed_prereq:
                return {'kind': 'PREREQUISITE_FAILED', 'prerequisiteKey': failed_prereq.get('key')}
        return None

    def _rule_matches_user(self, rule: dict, user: dict, state: EvalResult):
        for clause in rule.get('clauses') or []:
            if clause.get('attribute') is not None:
                if not self._clause_matches_user(clause, user, state):
                    return False
        return True

    def _clause_matches_user(self, clause: dict, user: dict, state: EvalResult):
        if clause.get('op') == 'segmentMatch':
            for seg_key in clause.get('values') or []:
                segment = self.__get_segment(seg_key)
                if segment is not None and self._segment_matches_user(segment, user, state):
                    return _maybe_negate(clause, True)
            return _maybe_negate(clause, False)
        else:
            return _clause_matches_user_no_segments(clause, user)

    def _segment_matches_user(self, segment: dict, user: dict, state: EvalResult):
        if segment.get('unbounded', False):
            return self._big_segment_match_user(segment, user, state)
        return _simple_segment_match_user(segment, user, True)

    def _big_segment_match_user(self, segment: dict, user: dict, state: EvalResult):
        generation = segment.get('generation', None)
        if generation is None:
            # Big segment queries can only be done if the generation is known. If it's unset,
            # that probably means the data store was populated by an older SDK that doesn't know
            # about the generation property and therefore dropped it from the JSON data. We'll treat
            # that as a "not configured" condition.
            state.big_segments_status = BigSegmentsStatus.NOT_CONFIGURED
            return False
        if state.big_segments_status is None:
            user_key = str(user.get('key'))
            result = self.__get_big_segments_membership(user_key)
            if result:
                state.big_segments_membership, state.big_segments_status = result
            else:
                state.big_segments_membership = None
                state.big_segments_status = BigSegmentsStatus.NOT_CONFIGURED
        segment_ref = _make_big_segment_ref(segment)
        membership = state.big_segments_membership
        included = None if membership is None else membership.get(segment_ref, None)
        if included is not None:
            return included
        return _simple_segment_match_user(segment, user, False)


# The following functions are declared outside Evaluator because they do not depend on any
# of Evaluator's state.

def _get_variation(flag, variation, reason):
    vars = flag.get('variations') or []
    if variation < 0 or variation >= len(vars):
        return EvaluationDetail(None, None, error_reason('MALFORMED_FLAG'))
    return EvaluationDetail(vars[variation], variation, reason)

def _get_off_value(flag, reason):
    off_var = flag.get('offVariation')
    if off_var is None:
        return EvaluationDetail(None, None, reason)
    return _get_variation(flag, off_var, reason)

def _get_value_for_variation_or_rollout(flag, vr, user, reason):
    index, inExperiment = _variation_index_for_user(flag, vr, user)
    if index is None:
        return EvaluationDetail(None, None, error_reason('MALFORMED_FLAG'))
    if inExperiment:
        reason['inExperiment'] = inExperiment
    return _get_variation(flag, index, reason)

def _get_user_attribute(user, attr):
    if attr == 'secondary':
        return None, True
    if attr in __BUILTINS__:
        return user.get(attr), False
    else:  # custom attribute
        if user.get('custom') is None or user['custom'].get(attr) is None:
            return None, True
        return user['custom'][attr], False

def _variation_index_for_user(feature, rule, user):
    if rule.get('variation') is not None:
        return (rule['variation'], False)

    rollout = rule.get('rollout')
    if rollout is None:
        return (None, False)
    variations = rollout.get('variations')
    seed = rollout.get('seed')
    if variations is not None and len(variations) > 0:
        bucket_by = 'key'
        if rollout.get('bucketBy') is not None:
            bucket_by = rollout['bucketBy']
        bucket = _bucket_user(seed, user, feature['key'], feature['salt'], bucket_by)
        is_experiment = rollout.get('kind') == 'experiment'
        sum = 0.0
        for wv in variations:
            sum += wv.get('weight', 0.0) / 100000.0
            if bucket < sum:
                is_experiment_partition = is_experiment and not wv.get('untracked')
                return (wv.get('variation'), is_experiment_partition)

        # The user's bucket value was greater than or equal to the end of the last bucket. This could happen due
        # to a rounding error, or due to the fact that we are scaling to 100000 rather than 99999, or the flag
        # data could contain buckets that don't actually add up to 100000. Rather than returning an error in
        # this case (or changing the scaling, which would potentially change the results for *all* users), we
        # will simply put the user in the last bucket.
        is_experiment_partition = is_experiment and not variations[-1].get('untracked')
        return (variations[-1].get('variation'), is_experiment_partition)

    return (None, False)

def _bucket_user(seed, user, key, salt, bucket_by):
    u_value, should_pass = _get_user_attribute(user, bucket_by)
    bucket_by_value = _bucketable_string_value(u_value)

    if should_pass or bucket_by_value is None:
        return 0.0

    id_hash = u_value
    if user.get('secondary') is not None:
        id_hash = id_hash + '.' + user['secondary']

    if seed is not None:
        prefix = str(seed)
    else:
        prefix = '%s.%s' % (key, salt)

    hash_key = '%s.%s' % (prefix, id_hash)
    hash_val = int(hashlib.sha1(hash_key.encode('utf-8')).hexdigest()[:15], 16)
    result = hash_val / __LONG_SCALE__
    return result

def _bucketable_string_value(u_value):
    return str(u_value) if isinstance(u_value, (str, int)) else None

def _clause_matches_user_no_segments(clause, user):
    u_value, should_pass = _get_user_attribute(user, clause.get('attribute'))
    if should_pass is True:
        return False
    if u_value is None:
        return None
    # is the attr an array?
    op_fn = operators.ops[clause['op']]
    if isinstance(u_value, (list, tuple)):
        for u in u_value:
            if _match_any(op_fn, u, clause.get('values') or []):
                return _maybe_negate(clause, True)
        return _maybe_negate(clause, False)
    else:
        return _maybe_negate(clause, _match_any(op_fn, u_value, clause.get('values') or []))

def _simple_segment_match_user(segment, user, use_includes_and_excludes):
    key = user.get('key')
    if key is not None:
        if use_includes_and_excludes:
            if key in segment.get('included', []):
                return True
            if key in segment.get('excluded', []):
                return False
        for rule in segment.get('rules', []):
            if _segment_rule_matches_user(rule, user, segment.get('key'), segment.get('salt')):
                return True
    return False

def _segment_rule_matches_user(rule, user, segment_key, salt):
    for clause in rule.get('clauses') or []:
        if not _clause_matches_user_no_segments(clause, user):
            return False

    # If the weight is absent, this rule matches
    if 'weight' not in rule or rule['weight'] is None:
        return True

    # All of the clauses are met. See if the user buckets in
    bucket_by = 'key' if rule.get('bucketBy') is None else rule['bucketBy']
    bucket = _bucket_user(None, user, segment_key, salt, bucket_by)
    weight = rule['weight'] / 100000.0
    return bucket < weight

def _match_any(op_fn, u, vals):
    for v in vals:
        if op_fn(u, v):
            return True
    return False

def _maybe_negate(clause, val):
    if clause.get('negate', False) is True:
        return not val
    return val

def _make_big_segment_ref(segment: dict) -> str:
    # The format of Big Segment references is independent of what store implementation is being
    # used; the store implementation receives only this string and does not know the details of
    # the data model. The Relay Proxy will use the same format when writing to the store.
    return "%s:%d" % (segment.get('key', ''), segment.get('generation', 0))

def error_reason(error_kind: str) -> dict:
    return {'kind': 'ERROR', 'errorKind': error_kind}
