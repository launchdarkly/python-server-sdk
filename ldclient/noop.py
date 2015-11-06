from ldclient.interfaces import FeatureRequester


class NoOpFeatureRequester(FeatureRequester):

    def __init__(self, *_):
        pass

    def get(self, key, callback):
        return None
