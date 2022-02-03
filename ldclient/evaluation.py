import json
import time
from typing import Any, Dict, Optional

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
          * ``"FALLTHROUGH"``: the flag was on but the user did not match any targets or rules
          * ``"TARGET_MATCH"``: the user was specifically targeted for this flag
          * ``"RULE_MATCH"``: the user matched one of the flag's rules
          * ``"PREREQUISITE_FAILED"``: the flag was considered off because it had at least one
            prerequisite flag that did not return the desired variation
          * ``"ERROR"``: the flag could not be evaluated due to an unexpected error.

        * ``ruleIndex``, ``ruleId``: The positional index and unique identifier of the matched
          rule, if the kind was ``RULE_MATCH``

        * ``prerequisiteKey``: The flag key of the prerequisite that failed, if the kind was
          ``PREREQUISITE_FAILED``

        * ``errorKind``: further describes the nature of the error if the kind was ``ERROR``,
          e.g. ``"FLAG_NOT_FOUND"``
        
        * ``bigSegmentsStatus``: describes the validity of Big Segment information, if and only if
          the flag evaluation required querying at least one Big Segment; otherwise it returns None.
          Allowable values are defined in :class:`BigSegmentsStatus`. For more information, read the
          LaunchDarkly documentation: https://docs.launchdarkly.com/home/users/big-segments
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


class BigSegmentsStatus:
    """
    Indicates that the Big Segment query involved in the flag evaluation was successful, and
    the segment state is considered up to date.
    """
    HEALTHY = "HEALTHY"

    """
    Indicates that the Big Segment query involved in the flag evaluation was successful, but
    segment state may not be up to date.
    """
    STALE = "STALE"

    """
    Indicates that Big Segments could not be queried for the flag evaluation because the SDK
    configuration did not include a Big Segment store.
    """
    NOT_CONFIGURED = "NOT_CONFIGURED"

    """
    Indicates that the Big Segment query involved in the flag evaluation failed, for
    instance due to a database error.
    """
    STORE_ERROR = "STORE_ERROR"


class FeatureFlagsState:
    """
    A snapshot of the state of all feature flags with regard to a specific user, generated by
    calling the :func:`ldclient.client.LDClient.all_flags_state()` method. Serializing this
    object to JSON, using the :func:`to_json_dict` method or ``jsonpickle``, will produce the
    appropriate data structure for bootstrapping the LaunchDarkly JavaScript client. See the
    JavaScript SDK Reference Guide on `Bootstrapping <https://docs.launchdarkly.com/sdk/features/bootstrapping#javascript>`_.
    """
    def __init__(self, valid: bool):
        self.__flag_values = {} # type: Dict[str, Any]
        self.__flag_metadata = {} # type: Dict[str, Any]
        self.__valid = valid

    # Used internally to build the state map
    def add_flag(self, flag_state, with_reasons, details_only_if_tracked):
        key = flag_state['key']
        self.__flag_values[key] = flag_state['value']
        meta = {}

        trackEvents = flag_state.get('trackEvents', False)
        trackReason = flag_state.get('trackReason', False)

        omit_details = False
        if details_only_if_tracked:
            now = int(time.time() * 1000)
            if not trackEvents and not trackReason and not (flag_state.get('debugEventsUntilDate') is not None and flag_state['debugEventsUntilDate'] > now):
                omit_details = True

        reason = None if not with_reasons and not trackReason else flag_state['reason']

        if reason is not None and not omit_details:
            meta['reason'] = reason

        if not omit_details:
            meta['version'] = flag_state['version']

        if flag_state['variation'] is not None:
            meta['variation'] = flag_state['variation']
        if trackEvents:
            meta['trackEvents'] = True
        if trackReason:
            meta['trackReason'] = True
        if flag_state.get('debugEventsUntilDate') is not None:
            meta['debugEventsUntilDate'] = flag_state.get('debugEventsUntilDate')
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
