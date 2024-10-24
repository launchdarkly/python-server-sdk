import json
from typing import Any, List, Optional, Union

# This file provides support for our data model classes.
#
# Top-level data model classes (FeatureFlag, Segment) should subclass ModelEntity. This
# provides a standard behavior where we decode the entity from a dict that corresponds to
# the JSON representation, and the constructor for each class does any necessary capturing
# and validation of individual properties, while the ModelEntity constructor also stores
# the original data as a dict so we can easily re-serialize it or inspect it as a dict.
#
# Lower-level classes such as Clause are not derived from ModelEntity because we don't
# need to serialize them outside of the enclosing FeatureFlag/Segment.
#
# All data model classes should use the opt_ and req_ functions so that any JSON values
# of invalid types will cause immediate rejection of the data set, rather than allowing
# invalid types to get into the evaluation/event logic where they would cause errors that
# are harder to diagnose.


def opt_type(data: dict, name: str, desired_type) -> Any:
    value = data.get(name)
    if value is not None and not isinstance(value, desired_type):
        raise ValueError('error in flag/segment data: property "%s" should be type %s but was %s"' % (name, desired_type, value.__class__))
    return value


def opt_bool(data: dict, name: str) -> bool:
    return opt_type(data, name, bool) is True


def opt_dict(data: dict, name: str) -> Optional[dict]:
    return opt_type(data, name, dict)


def opt_dict_list(data: dict, name: str) -> list:
    return validate_list_type(opt_list(data, name), name, dict)


def opt_int(data: dict, name: str) -> Optional[int]:
    return opt_type(data, name, int)


def opt_number(data: dict, name: str) -> Optional[Union[int, float]]:
    value = data.get(name)
    if value is not None and not isinstance(value, int) and not isinstance(value, float):
        raise ValueError('error in flag/segment data: property "%s" should be a number but was %s"' % (name, value.__class__))
    return value


def opt_list(data: dict, name: str) -> list:
    return opt_type(data, name, list) or []


def opt_str(data: dict, name: str) -> Optional[str]:
    return opt_type(data, name, str)


def opt_str_list(data: dict, name: str) -> List[str]:
    return validate_list_type(opt_list(data, name), name, str)


def req_type(data: dict, name: str, desired_type) -> Any:
    value = opt_type(data, name, desired_type)
    if value is None:
        raise ValueError('error in flag/segment data: required property "%s" is missing' % name)
    return value


def req_dict_list(data: dict, name: str) -> list:
    return validate_list_type(req_list(data, name), name, dict)


def req_int(data: dict, name: str) -> int:
    return req_type(data, name, int)


def req_list(data: dict, name: str) -> list:
    return req_type(data, name, list)


def req_str(data: dict, name: str) -> str:
    return req_type(data, name, str)


def req_str_list(data: dict, name: str) -> List[str]:
    return validate_list_type(req_list(data, name), name, str)


def validate_list_type(items: list, name: str, desired_type) -> list:
    for item in items:
        if not isinstance(item, desired_type):
            raise ValueError('error in flag/segment data: property %s should be an array of %s but an item was %s' % (name, desired_type, item.__class__))
    return items


class ModelEntity:
    def __init__(self, data: dict):
        self._data = data

    def to_json_dict(self):
        return self._data

    def get(self, attribute, default=None) -> Any:
        return self._data.get(attribute, default)

    def __getitem__(self, attribute) -> Any:
        return self._data[attribute]

    def __contains__(self, attribute) -> bool:
        return attribute in self._data

    def __eq__(self, other) -> bool:
        return self.__class__ == other.__class__ and self._data == other._data

    def __repr__(self) -> str:
        return json.dumps(self._data, separators=(',', ':'))
