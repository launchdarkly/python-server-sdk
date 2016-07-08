import hashlib
import logging

import six
import sys

from ldclient import operators

__LONG_SCALE__ = float(0xFFFFFFFFFFFFFFF)

__BUILTINS__ = ["key", "ip", "country", "email",
                "firstName", "lastName", "avatar", "name", "anonymous"]

log = logging.getLogger(sys.modules[__name__].__name__)


def _evaluate(feature, user):
    if feature is None:
        return None
    if feature.get('on', False):
        #TODO: prereqs
        index = _evaluate_index(feature, user)
        log.debug("Got index: " + str(index))
        return _get_variation(feature, index)
    else:
        if 'offVariation' in feature and feature['offVariation']:
            return _get_variation(feature, feature['offVariation'])

    return None


def _evaluate_index(feature, user):
    # Check to see if any user targets match:
    for target in feature.get('targets', []):
        for value in target.get('values', []):
            if value == user['key']:
                return target.get('variation')

    # Now walk through the rules to see if any match
    for rule in feature.get('rules', []):
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
        bucket_by = rule['rollout'].get('bucketBy') or 'key'
        bucket = _bucket_user(user, feature, bucket_by)
        sum = 0.0
        for wv in rule['rollout'].get('variations', []):
            sum += wv.get('weight', 0.0) / 100000.0
            if bucket < sum:
                return wv.get('variation')

    return None


def _bucket_user(user, feature, bucket_by):
    u_value = _get_user_attribute(user, bucket_by)
    if isinstance(u_value, six.string_types):
        id_hash = u_value
        if user.get('secondary') is not None:
            id_hash += "." + user['secondary']
        hash_key = '%s.%s.%s' % (feature['key'], feature['salt'], id_hash)
        hash_val = int(hashlib.sha1(hash_key.encode('utf-8')).hexdigest()[:15], 16)
        result = hash_val / __LONG_SCALE__
        return result

    return 0.0


def _rule_matches_user(rule, user):
    for clause in rule.get('clauses', []):
        if clause.get('attribute') is not None:
            if not _clause_matches_user(clause, user):
                return False
    return True


def _clause_matches_user(clause, user):
    u_value, should_pass = _get_user_attribute(user, clause.get('attribute'))
    log.debug("got user attr: " + str(clause.get('attribute')) + " value: " + str(u_value))
    if should_pass is True:
        return False
    if u_value is None:
        return None
    # is the attr an array?
    op_fn = operators.ops[clause['op']]
    if isinstance(u_value, (list, tuple)):
        log.debug("array..")
        for u in u_value:
            if _match_any(op_fn, u, clause.get('values', [])):
                return _maybe_negate(clause, True)
            return _maybe_negate(clause, True)
    else:
        return _maybe_negate(clause, _match_any(op_fn, u_value, clause.get('values', [])))


def _match_any(op_fn, u, vals):
    for v in vals:
        if op_fn(u, v):
            log.debug("Matched: u: " + str(u) + " with v: " + str(v))
            return True
    log.debug("Didn't match: u: " + str(u) + " with v: " + str(vals))
    return False


def _maybe_negate(clause, val):
    if clause.get('negate', False):
        return not val
    return val
