from random import Random


class Sampler:
    def __init__(self, generator: Random):
        self.__generator = generator

    def sample(self, ratio: int):
        # Booleans are considered ints in python, so we have to check for them
        # as well here.
        if isinstance(ratio, bool) or not isinstance(ratio, int):
            return False
        if ratio <= 0:
            return False
        if ratio == 1:
            return True

        return self.__generator.random() < (1 / ratio)
