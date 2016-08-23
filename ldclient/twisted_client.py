from functools import partial

from twisted.internet import defer
from twisted.internet.defer import DeferredList

from ldclient import LDClient
from ldclient import log
from ldclient.flag import _get_variation, _evaluate_index, _get_off_variation


class TwistedLDClient(LDClient):
    @defer.inlineCallbacks
    def _evaluate_and_send_events(self, flag, user, default):
        value = yield self._evaluate(flag, user)
        if value is None:
            value = default
        log.info("value: " + str(value))
        self._send_event({'kind': 'feature', 'key': flag.get('key'), 'user': user, 'value': value,
                          'default': default, 'version': flag.get('version')})
        defer.returnValue(value)

    def _evaluate(self, flag, user):
        if flag.get('on', False):
            def cb(result):
                if result is not None:
                    return result
                return _get_off_variation(flag)

            value = self._evaluate_internal(flag, user)
            value.addBoth(cb)
            return value

        return _get_off_variation(flag)

    def _evaluate_internal(self, flag, user):
        def check_prereq_results(result):
            prereq_ok = True
            for (success, prereq_ok) in result:
                if success is False or prereq_ok is False:
                    prereq_ok = False

            if prereq_ok is True:
                index = _evaluate_index(flag, user)
                variation = _get_variation(flag, index)
                return variation
            return None

        results = DeferredList(map(partial(self._evaluate_prereq, user), flag.get('prerequisites') or []))
        results.addBoth(check_prereq_results)
        return results

    # returns False if the prereq failed or there was an error evaluating it. Otherwise returns True
    def _evaluate_prereq(self, user, prereq):

        @defer.inlineCallbacks
        def eval_prereq(prereq_flag):
            if prereq_flag is None:
                log.warn("Missing prereq flag: " + prereq.get('key'))
                defer.returnValue(False)
            if prereq_flag.get('on', False) is True:
                prereq_value = yield self._evaluate_internal(prereq_flag, user)
                variation = _get_variation(prereq_flag, prereq.get('variation'))
                if prereq_value is None or not prereq_value == variation:
                    ok = False
                else:
                    ok = True
            else:
                ok = False
            defer.returnValue(ok)

        result = self._store.get(prereq.get('key'), eval_prereq)
        return result

    @defer.inlineCallbacks
    def _evaluate_multi(self, user, flags):
        results = {}
        for k, v in flags.items() or {}:
            r = yield self._evaluate(v, user)
            results[k] = r
        defer.returnValue(results)
