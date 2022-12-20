from ldclient.impl.model import *

from typing import Optional

# Event constructors are centralized here to avoid mistakes and repetitive logic.
# The LDClient owns two instances of _EventFactory: one that always embeds evaluation reasons
# in the events (for when variation_detail is called) and one that doesn't.
#
# Note that none of these methods fill in the "creationDate" property, because in the Python
# client, that is done by DefaultEventProcessor.send_event().

class _EventFactory:
    def __init__(self, with_reasons):
        self._with_reasons = with_reasons

    def new_eval_event(self, flag: FeatureFlag, user, detail, default_value, prereq_of_flag: Optional[FeatureFlag] = None) -> dict:
        add_experiment_data = self.is_experiment(flag, detail.reason)
        e = {
            'kind': 'feature',
            'key': flag.key,
            'user': user,
            'value': detail.value,
            'variation': detail.variation_index,
            'default': default_value,
            'version': flag.version
        }
        # the following properties are handled separately so we don't waste bandwidth on unused keys
        if add_experiment_data or flag.track_events:
            e['trackEvents'] = True
        if flag.debug_events_until_date:
            e['debugEventsUntilDate'] = flag.debug_events_until_date
        if prereq_of_flag is not None:
            e['prereqOf'] = prereq_of_flag.key
        if add_experiment_data or self._with_reasons:
            e['reason'] = detail.reason
        if user is not None and user.get('anonymous'):
            e['contextKind'] = self._user_to_context_kind(user)
        return e

    def new_default_event(self, flag: FeatureFlag, user, default_value, reason) -> dict:
        e = {
            'kind': 'feature',
            'key': flag.key,
            'user': user,
            'value': default_value,
            'default': default_value,
            'version': flag.version
        }
        # the following properties are handled separately so we don't waste bandwidth on unused keys
        if flag.track_events:
            e['trackEvents'] = True
        if flag.debug_events_until_date:
            e['debugEventsUntilDate'] = flag.debug_events_until_date
        if self._with_reasons:
            e['reason'] = reason
        if user is not None and user.get('anonymous'):
            e['contextKind'] = self._user_to_context_kind(user)
        return e

    def new_unknown_flag_event(self, key, user, default_value, reason):
        e = {
            'kind': 'feature',
            'key': key,
            'user': user,
            'value': default_value,
            'default': default_value
        }
        if self._with_reasons:
            e['reason'] = reason
        if user is not None and user.get('anonymous'):
            e['contextKind'] = self._user_to_context_kind(user)
        return e

    def new_identify_event(self, user):
        return {
            'kind': 'identify',
            'key': str(user.get('key')),
            'user': user
        }

    def new_custom_event(self, event_name, user, data, metric_value):
        e = {
            'kind': 'custom',
            'key': event_name,
            'user': user
        }
        if data is not None:
            e['data'] = data
        if metric_value is not None:
            e['metricValue'] = metric_value
        if user.get('anonymous'):
            e['contextKind'] = self._user_to_context_kind(user)
        return e

    def _user_to_context_kind(self, user):
        if user.get('anonymous'):
            return "anonymousUser"
        else:
            return "user"

    @staticmethod
    def is_experiment(flag: FeatureFlag, reason):
        if reason is not None:
            if reason.get('inExperiment'):
                return True
            kind = reason['kind']
            if kind == 'RULE_MATCH':
                index = reason['ruleIndex']
                rules = flag.rules
                return index >= 0 and index < len(rules) and rules[index].track_events
            elif kind == 'FALLTHROUGH':
                return flag.track_events_fallthrough
        return False
