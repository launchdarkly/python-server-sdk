import hashlib
import logging

import six
import sys

from ldclient import operators

__LONG_SCALE__ = float(0xFFFFFFFFFFFFFFF)

__BUILTINS__ = ["key", "ip", "country", "email",
                "firstName", "lastName", "avatar", "name", "anonymous"]

log = logging.getLogger(sys.modules[__name__].__name__)


def evaluate(flag, user, store):
    prereq_events = []
    if flag.get('on', False):
        value, prereq_events = _evaluate(flag, user, store)
        if value is not None:
            return value, prereq_events

    return _get_off_variation(flag), prereq_events


def _evaluate(flag, user, store, prereq_events=None):
    events = prereq_events or []
    failed_prereq = None
    prereq_value = None
    for prereq in flag.get('prerequisites') or []:
        prereq_flag = store.get(prereq.get('key'), lambda x: x)
        if prereq_flag is None:
            log.warn("Missing prereq flag: " + prereq.get('key'))
            failed_prereq = prereq
            break
        if prereq_flag.get('on', False) is True:
            prereq_value, events = _evaluate(prereq_flag, user, store, events)
            variation = _get_variation(prereq_flag, prereq.get('variation'))
            if prereq_value is None or not prereq_value == variation:
                failed_prereq = prereq
        else:
            failed_prereq = prereq

        event = {'kind': 'feature', 'key': prereq.get('key'), 'user': user,
                 'value': prereq_value, 'version': prereq_flag.get('version'), 'prereqOf': prereq.get('key')}
        events.append(event)

    if failed_prereq is not None:
        return None, events

    index = _evaluate_index(flag, user)
    return _get_variation(flag, index), events


def _evaluate_index(feature, user):
    # Check to see if any user targets match:
    for target in feature.get('targets') or []:
        for value in target.get('values') or []:
            if value == user['key']:
                return target.get('variation')

    # Now walk through the rules to see if any match
    for rule in feature.get('rules') or []:
        if _rule_matches_user(rule, user):
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
        bucket = _bucket_user(user, feature, bucket_by)
        sum = 0.0
        for wv in rule['rollout'].get('variations') or []:
            sum += wv.get('weight', 0.0) / 100000.0
            if bucket < sum:
                return wv.get('variation')

    return None


def _bucket_user(user, feature, bucket_by):
    u_value, should_pass = _get_user_attribute(user, bucket_by)
    if should_pass is True or not isinstance(u_value, six.string_types):
        return 0.0

    id_hash = u_value
    if user.get('secondary') is not None:
        id_hash = id_hash + '.' + user['secondary']
    hash_key = '%s.%s.%s' % (feature['key'], feature['salt'], id_hash)
    hash_val = int(hashlib.sha1(hash_key.encode('utf-8')).hexdigest()[:15], 16)
    result = hash_val / __LONG_SCALE__
    return result


def _rule_matches_user(rule, user):
    for clause in rule.get('clauses') or []:
        if clause.get('attribute') is not None:
            if not _clause_matches_user(clause, user):
                return False
    return True


def _clause_matches_user(clause, user):
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


def _match_any(op_fn, u, vals):
    for v in vals:
        if op_fn(u, v):
            return True
    return False


def _maybe_negate(clause, val):
    if clause.get('negate', False) is True:
        return not val
    return val
