import json

from ldclient.impl.model.entity import ModelEntity


class ModelEncoder(json.JSONEncoder):
    """
    A JSON encoder customized to serialize our data model types correctly. We should
    use this whenever we are writing flag data to a persistent store.
    """

    def __init__(self):
        super().__init__(separators=(',', ':'))

    def default(self, obj):
        if isinstance(obj, ModelEntity):
            return obj.to_json_dict()
        return json.JSONEncoder.default(self, obj)
