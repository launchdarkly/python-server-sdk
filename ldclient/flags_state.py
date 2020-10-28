"""
This submodule contains a helper class for feature flag evaluation.
"""
from typing import Optional, Dict, Any

import json
import time

class FeatureFlagsState:
    """
    A snapshot of the state of all feature flags with regard to a specific user, generated by
    calling the :func:`ldclient.client.LDClient.all_flags_state()` method. Serializing this
    object to JSON, using the :func:`to_json_dict` method or ``jsonpickle``, will produce the
    appropriate data structure for bootstrapping the LaunchDarkly JavaScript client. See the
    JavaScript SDK Reference Guide on `Bootstrapping <https://docs.launchdarkly.com/docs/js-sdk-reference#section-bootstrapping>`_.
    """
    def __init__(self, valid: bool):
        self.__flag_values = {} # type: Dict[str, Any]
        self.__flag_metadata = {} # type: Dict[str, Any]
        self.__valid = valid

    # Used internally to build the state map
    def add_flag(self, flag, value, variation, reason, details_only_if_tracked):
        key = flag['key']
        self.__flag_values[key] = value
        meta = {}
        with_details = (not details_only_if_tracked) or flag.get('trackEvents')
        if not with_details:
            if flag.get('debugEventsUntilDate'):
                now = int(time.time() * 1000)
                with_details = (flag.get('debugEventsUntilDate') > now)
        if with_details:
            meta['version'] = flag.get('version')
            if reason is not None:
                meta['reason'] = reason
        if variation is not None:
            meta['variation'] = variation
        if flag.get('trackEvents'):
            meta['trackEvents'] = True
        if flag.get('debugEventsUntilDate') is not None:
            meta['debugEventsUntilDate'] = flag.get('debugEventsUntilDate')
        self.__flag_metadata[key] = meta

    @property
    def valid(self) -> bool:
        """True if this object contains a valid snapshot of feature flag state, or False if the
        state could not be computed (for instance, because the client was offline or there was no user).
        """
        return self.__valid


    def get_flag_value(self, key: str) -> object:
        """Returns the value of an individual feature flag at the time the state was recorded.

        :param key: the feature flag key
        :return: the flag's value; None if the flag returned the default value, or if there was no such flag
        """
        return self.__flag_values.get(key)

    def get_flag_reason(self, key: str) -> Optional[dict]:
        """Returns the evaluation reason for an individual feature flag at the time the state was recorded.

        :param key: the feature flag key
        :return: a dictionary describing the reason; None if reasons were not recorded, or if there was no
          such flag
        """
        meta = self.__flag_metadata.get(key)
        return None if meta is None else meta.get('reason')

    def to_values_map(self) -> dict:
        """Returns a dictionary of flag keys to flag values. If the flag would have evaluated to the
        default value, its value will be None.

        Do not use this method if you are passing data to the front end to "bootstrap" the JavaScript client.
        Instead, use :func:`to_json_dict()`.
        """
        return self.__flag_values

    def to_json_dict(self) -> dict:
        """Returns a dictionary suitable for passing as JSON, in the format used by the LaunchDarkly
        JavaScript SDK. Use this method if you are passing data to the front end in order to
        "bootstrap" the JavaScript client.
        """
        ret = self.__flag_values.copy()
        ret['$flagsState'] = self.__flag_metadata
        ret['$valid'] = self.__valid
        return ret

    def to_json_string(self) -> str:
        """Same as to_json_dict, but serializes the JSON structure into a string.
        """
        return json.dumps(self.to_json_dict())

    def __getstate__(self) -> dict:
        """Equivalent to to_json_dict() - used if you are serializing the object with jsonpickle.
        """
        return self.to_json_dict()
