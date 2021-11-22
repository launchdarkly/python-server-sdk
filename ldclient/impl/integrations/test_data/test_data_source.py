
TRUE_VARIATION_INDEX = 0
FALSE_VARIATION_INDEX = 1

def variation_for_boolean(variation):
    if variation:
        return TRUE_VARIATION_INDEX
    else:
        return FALSE_VARIATION_INDEX

class _FlagBuilder():
    def __init__(self, key):
        self._key = key
        self._on = True
        # TODO set up deep copy
        self._variations = []

    def on(self, aBool):
        self._on = aBool
        return self

    def fallthrough_variation(self, variation):
        if isinstance(variation, bool):
            self._boolean_flag(self)._fallthrough_variation = variation
            return self
        else:
            self._fallthrough_variation = variation
            return self

    def off_variation(self, variation) :
        if isinstance(variation, bool):
            self._boolean_flag(self)._off_variation = variation
            return self
        else:
            self._off_variation = variation
            return self

    def boolean_flag(self):
        if self._is_boolean_flag():
            return self
        else:
            return (self.variations(True, False)
                .fallthrough_variation(TRUE_VARIATION_INDEX)
                .off_variation(FALSE_VARIATION_INDEX))

    def _is_boolean_flag(self):
        return (len(self._variations) == 2
            and self._variations[TRUE_VARIATION_INDEX] == True
            and self._variations[FALSE_VARIATION_INDEX] == False)

    def variations(self, *variations):
        self._variations = variations
        return self


    def variation_for_all_users(self, variation):
        if isinstance(variation, bool):
            return self.boolean_flag().variation_for_all_users(variation_for_boolean(variation))
        else:
            return self.on(True).fallthrough_variation(variation)
