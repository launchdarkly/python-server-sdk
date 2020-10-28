"""
This submodule contains a helper class for feature flag evaluation, as well as some implementation details.
"""

from collections import namedtuple
import hashlib
import logging

from typing import Optional, List, Any
import sys

from ldclient import operators
from ldclient.util import stringify_attrs
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

__LONG_SCALE__ = float(0xFFFFFFFFFFFFFFF)

__BUILTINS__ = ["key", "ip", "country", "email",
                "firstName", "lastName", "avatar", "name", "anonymous"]

__USER_ATTRS_TO_STRINGIFY_FOR_EVALUATION__ = [ "key", "secondary" ]
# Currently we are not stringifying the rest of the built-in attributes prior to evaluation, only for events.
# This is because it could affect evaluation results for existing users (ch35206).

log = logging.getLogger(sys.modules[__name__].__name__)


class EvaluationDetail:
    """
    The return type of :func:`ldclient.client.LDClient.variation_detail()`, combining the result of a
    flag evaluation with information about how it was calculated.
    """
    def __init__(self, value: object, variation_index: Optional[int], reason: dict):
        """Constructs an instance.
        """
        self.__value = value
        self.__variation_index = variation_index
        self.__reason = reason

    @property
    def value(self) -> object:
        """The result of the flag evaluation. This will be either one of the flag's
        variations or the default value that was passed to the
        :func:`ldclient.client.LDClient.variation_detail()` method.
        """
        return self.__value

    @property
    def variation_index(self) -> Optional[int]:
        """The index of the returned value within the flag's list of variations, e.g.
        0 for the first variation -- or None if the default value was returned.
        """
        return self.__variation_index

    @property
    def reason(self) -> dict:
        """A dictionary describing the main factor that influenced the flag evaluation value.
        It contains the following properties:

        * ``kind``: The general category of reason, as follows:

          * ``"OFF"``: the flag was off
          * ``"FALLTHROUGH"`` -- the flag was on but the user did not match any targets or rules
          * ``"TARGET_MATCH"`` -- the user was specifically targeted for this flag
          * ``"RULE_MATCH"`` -- the user matched one of the flag's rules
          * ``"PREREQUISITE_FAILED"`` -- the flag was considered off because it had at least one
            prerequisite flag that did not return the desired variation
          * ``"ERROR"`` - the flag could not be evaluated due to an unexpected error.

        * ``ruleIndex``, ``ruleId``: The positional index and unique identifier of the matched
          rule, if the kind was ``RULE_MATCH``

        * ``prerequisiteKey``: The flag key of the prerequisite that failed, if the kind was
          ``PREREQUISITE_FAILED``

        * ``errorKind``: further describes the nature of the error if the kind was ``ERROR``,
          e.g. ``"FLAG_NOT_FOUND"``
        """
        return self.__reason

    def is_default_value(self) -> bool:

        """Returns True if the flag evaluated to the default value rather than one of its
        variations.
        """
        return self.__variation_index is None

    def __eq__(self, other) -> bool:
        return self.value == other.value and self.variation_index == other.variation_index and self.reason == other.reason

    def __ne__(self, other) -> bool:
        return not self.__eq__(other)

    def __str__(self) -> str:
        return "(value=%s, variation_index=%s, reason=%s)" % (self.value, self.variation_index, self.reason)

    def __repr__(self) -> str:
        return self.__str__()


EvalResult = namedtuple('EvalResult', ['detail', 'events'])


def error_reason(error_kind: str) -> dict:
    return {'kind': 'ERROR', 'errorKind': error_kind}


def evaluate(flag, user, store, event_factory) -> EvalResult:
    sanitized_user = stringify_attrs(user, __USER_ATTRS_TO_STRINGIFY_FOR_EVALUATION__)
    prereq_events = [] # type: List[Any]
    detail = _evaluate(flag, sanitized_user, store, prereq_events, event_factory)
    return EvalResult(detail = detail, events = prereq_events)

def _evaluate(flag, user, store, prereq_events, event_factory):
    if not flag.get('on', False):
        return _get_off_value(flag, {'kind': 'OFF'})

    prereq_failure_reason = _check_prerequisites(flag, user, store, prereq_events, event_factory)
    if prereq_failure_reason is not None:
        return _get_off_value(flag, prereq_failure_reason)

    # Check to see if any user targets match:
    for target in flag.get('targets') or []:
        for value in target.get('values') or []:
            if value == user['key']:
                return _get_variation(flag, target.get('variation'), {'kind': 'TARGET_MATCH'})

    # Now walk through the rules to see if any match
    for index, rule in enumerate(flag.get('rules') or []):
        if _rule_matches_user(rule, user, store):
            return _get_value_for_variation_or_rollout(flag, rule, user,
                {'kind': 'RULE_MATCH', 'ruleIndex': index, 'ruleId': rule.get('id')})

    # Walk through fallthrough and see if it matches
    if flag.get('fallthrough') is not None:
        return _get_value_for_variation_or_rollout(flag, flag['fallthrough'], user, {'kind': 'FALLTHROUGH'})


def _check_prerequisites(flag, user, store, events, event_factory):
    failed_prereq = None
    prereq_res = None
    for prereq in flag.get('prerequisites') or []:
        prereq_flag = store.get(FEATURES, prereq.get('key'), lambda x: x)
        if prereq_flag is None:
            log.warning("Missing prereq flag: " + prereq.get('key'))
            failed_prereq = prereq
        else:
            prereq_res = _evaluate(prereq_flag, user, store, events, event_factory)
            # Note that if the prerequisite flag is off, we don't consider it a match no matter what its
            # off variation was. But we still need to evaluate it in order to generate an event.
            if (not prereq_flag.get('on', False)) or prereq_res.variation_index != prereq.get('variation'):
                failed_prereq = prereq
            event = event_factory.new_eval_event(prereq_flag, user, prereq_res, None, flag)
            events.append(event)
        if failed_prereq:
            return {'kind': 'PREREQUISITE_FAILED', 'prerequisiteKey': failed_prereq.get('key')}
    return None


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
    index = _variation_index_for_user(flag, vr, user)
    if index is None:
        return EvaluationDetail(None, None, error_reason('MALFORMED_FLAG'))
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
        return rule['variation']

    rollout = rule.get('rollout')
    if rollout is None:
        return None
    variations = rollout.get('variations')
    if variations is not None and len(variations) > 0:
        bucket_by = 'key'
        if rollout.get('bucketBy') is not None:
            bucket_by = rollout['bucketBy']
        bucket = _bucket_user(user, feature['key'], feature['salt'], bucket_by)
        sum = 0.0
        for wv in variations:
            sum += wv.get('weight', 0.0) / 100000.0
            if bucket < sum:
                return wv.get('variation')

        # The user's bucket value was greater than or equal to the end of the last bucket. This could happen due
        # to a rounding error, or due to the fact that we are scaling to 100000 rather than 99999, or the flag
        # data could contain buckets that don't actually add up to 100000. Rather than returning an error in
        # this case (or changing the scaling, which would potentially change the results for *all* users), we
        # will simply put the user in the last bucket.
        return variations[-1].get('variation')

    return None


def _bucket_user(user, key, salt, bucket_by):
    u_value, should_pass = _get_user_attribute(user, bucket_by)
    bucket_by_value = _bucketable_string_value(u_value)

    if should_pass or bucket_by_value is None:
        return 0.0

    id_hash = u_value
    if user.get('secondary') is not None:
        id_hash = id_hash + '.' + user['secondary']
    hash_key = '%s.%s.%s' % (key, salt, id_hash)
    hash_val = int(hashlib.sha1(hash_key.encode('utf-8')).hexdigest()[:15], 16)
    result = hash_val / __LONG_SCALE__
    return result


def _bucketable_string_value(u_value):
    return str(u_value) if isinstance(u_value, (str, int)) else None

def _rule_matches_user(rule, user, store):
    for clause in rule.get('clauses') or []:
        if clause.get('attribute') is not None:
            if not _clause_matches_user(clause, user, store):
                return False
    return True


def _clause_matches_user(clause, user, store):
    if clause.get('op') == 'segmentMatch':
        for seg_key in clause.get('values') or []:
            segment = store.get(SEGMENTS, seg_key, lambda x: x)
            if segment is not None and _segment_matches_user(segment, user):
                return _maybe_negate(clause, True)
        return _maybe_negate(clause, False)
    else:
        return _clause_matches_user_no_segments(clause, user)

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

def _segment_matches_user(segment, user):
    key = user.get('key')
    if key is not None:
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
    bucket = _bucket_user(user, segment_key, salt, bucket_by)
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
