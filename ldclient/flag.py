from collections import namedtuple
import hashlib
import logging

import six
import sys

from ldclient import operators
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

__LONG_SCALE__ = float(0xFFFFFFFFFFFFFFF)

__BUILTINS__ = ["key", "ip", "country", "email",
                "firstName", "lastName", "avatar", "name", "anonymous"]

log = logging.getLogger(sys.modules[__name__].__name__)


EvalResult = namedtuple('EvalResult', ['variation', 'value', 'events'])


def evaluate(flag, user, store):
    prereq_events = []
    if flag.get('on', False):
        variation, value, prereq_events = _evaluate(flag, user, store)
        if value is not None:
            return EvalResult(variation = variation, value = value, events = prereq_events)

    off_var = flag.get('offVariation')
    off_value = None if off_var is None else _get_variation(flag, off_var)
    return EvalResult(variation = off_var, value = off_value, events = prereq_events)


def _evaluate(flag, user, store, prereq_events=None):
    events = prereq_events or []
    failed_prereq = None
    prereq_var = None
    prereq_value = None
    for prereq in flag.get('prerequisites') or []:
        prereq_flag = store.get(FEATURES, prereq.get('key'), lambda x: x)
        if prereq_flag is None:
            log.warn("Missing prereq flag: " + prereq.get('key'))
            failed_prereq = prereq
            break
        if prereq_flag.get('on', False) is True:
            prereq_var, prereq_value, events = _evaluate(prereq_flag, user, store, events)
            if prereq_var is None or not prereq_var == prereq.get('variation'):
                failed_prereq = prereq
        else:
            failed_prereq = prereq

        event = {'kind': 'feature', 'key': prereq.get('key'), 'user': user, 'variation': prereq_var,
                 'value': prereq_value, 'version': prereq_flag.get('version'), 'prereqOf': flag.get('key'),
                 'trackEvents': prereq_flag.get('trackEvents'),
                 'debugEventsUntilDate': prereq_flag.get('debugEventsUntilDate')}
        events.append(event)

    if failed_prereq is not None:
        return None, None, events

    index = _evaluate_index(flag, user, store)
    return index, _get_variation(flag, index), events


def _evaluate_index(feature, user, store):
    # Check to see if any user targets match:
    for target in feature.get('targets') or []:
        for value in target.get('values') or []:
            if value == user['key']:
                return target.get('variation')

    # Now walk through the rules to see if any match
    for rule in feature.get('rules') or []:
        if _rule_matches_user(rule, user, store):
            return _variation_index_for_user(feature, rule, user)

    # Walk through fallthrough and see if it matches
    if feature.get('fallthrough') is not None:
        return _variation_index_for_user(feature, feature['fallthrough'], user)

    return None


def _get_variation(feature, index):
    if index is not None and index < len(feature['variations']):
        return feature['variations'][index]
    return None


def _get_off_variation(feature):
    if feature.get('offVariation') is not None:
        return _get_variation(feature, feature.get('offVariation'))
    return None


def _get_user_attribute(user, attr):
    if attr is 'secondary':
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

    if rule.get('rollout') is not None:
        bucket_by = 'key'
        if rule['rollout'].get('bucketBy') is not None:
            bucket_by = rule['rollout']['bucketBy']
        bucket = _bucket_user(user, feature['key'], feature['salt'], bucket_by)
        sum = 0.0
        for wv in rule['rollout'].get('variations') or []:
            sum += wv.get('weight', 0.0) / 100000.0
            if bucket < sum:
                return wv.get('variation')

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
    if isinstance(u_value, six.string_types):
        return u_value
    if isinstance(u_value, six.integer_types):
        return str(u_value)
    return None


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
