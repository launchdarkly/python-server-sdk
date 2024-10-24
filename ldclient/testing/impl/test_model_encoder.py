import json

from ldclient.impl.model import *


class MyTestEntity(ModelEntity):
    def __init__(self, value):
        self._value = value

    def to_json_dict(self) -> dict:
        return {'magicValue': self._value}


def test_model_encoder():
    data = [MyTestEntity(1), MyTestEntity('x')]
    output = ModelEncoder().encode(data)
    assert output == '[{"magicValue":1},{"magicValue":"x"}]'
