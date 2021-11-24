import copy

TRUE_VARIATION_INDEX = 0
FALSE_VARIATION_INDEX = 1

def variation_for_boolean(variation):
    if variation:
        return TRUE_VARIATION_INDEX
    else:
        return FALSE_VARIATION_INDEX

class TestData():

    def flag(key):
        return _FlagBuilder(key)

class _FlagBuilder():
    def __init__(self, key):
        self._key = key
        self._on = True
        self._variations = []

    def copy(self):
        to = _FlagBuilder(self._key)

        to._on = self._on
        to._variations = copy.copy(self._variations)

        try:
            to._off_variation = self._off_variation
        except:
            pass

        try:
            to._fallthrough_variation = self._fallthrough_variation
        except:
            pass

        try:
            to._targets = copy.copy(self._targets)
        except:
            pass

        try:
            to._rules = copy.copy(self._rules)
        except:
            pass

        return to


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
        if self.is_boolean_flag():
            return self
        else:
            return (self.variations(True, False)
                .fallthrough_variation(TRUE_VARIATION_INDEX)
                .off_variation(FALSE_VARIATION_INDEX))

    def is_boolean_flag(self):
        return (len(self._variations) == 2
            and self._variations[TRUE_VARIATION_INDEX] == True
            and self._variations[FALSE_VARIATION_INDEX] == False)

    def variations(self, *variations):
        self._variations = list(variations)

        return self


    def variation_for_all_users(self, variation):
        if isinstance(variation, bool):
            return self.boolean_flag().variation_for_all_users(variation_for_boolean(variation))
        else:
            return self.on(True).fallthrough_variation(variation)

    def variation_for_user(self, user_key, variation):
        if isinstance(variation, bool):
            return self.boolean_flag().variation_for_user(user_key, variation_for_boolean(variation))
        else:
            # `variation` specifies the index of the variation to set
            targets = {}
            try:
                targets = self._targets
            except:
                self._targets = {}

            for idx, var in enumerate(self._variations):
                if (idx == variation):
                    # If there is no set at the current variation, set it to be empty
                    target_for_variation = []
                    if idx in targets:
                        target_for_variation = targets[idx]

                    # If user is not in the current variation set, add them
                    if user_key not in target_for_variation:
                        target_for_variation.append(user_key)

                    self._targets[idx] = target_for_variation

                else:
                    # Remove user from the other variation set if necessary
                    if idx in targets:
                        target_for_variation = targets[idx]
                        if user_key in target_for_variation:
                            user_key_idx = target_for_variation.index(user_key)
                            del target_for_variation[user_key_idx]

                        self._targets[idx] = target_for_variation

            return self

    def add_rule(self, flag_rule_builder):
        try:
            len(self._rules) >= 0
        except:
            self._rules = []

        self._rules.append(flag_rule_builder)

    def if_match(self, attribute, *values):
        flag_rule_builder = _FlagRuleBuilder(self)
        return flag_rule_builder.and_match(attribute, *values)

    def if_not_match(self, attribute, *values):
        flag_rule_builder = _FlagRuleBuilder(self)
        return flag_rule_builder.and_not_match(attribute, values)

    def clear_rules(self):
        del self._rules
        return self


    def build(self, version):
        base_flag_object = {
            'key': self._key,
            'version': version,
            'on': self._on,
            'variations': self._variations
        }

        try:
            base_flag_object['off_variation'] = self._off_variation
        except:
            pass

        try:
            base_flag_object['fallthrough_variation'] = self._fallthrough_variation
        except:
            pass

        try:
            targets = []
            for var_index, user_keys in self._targets.items():
                targets.append({
                    'variation': var_index,
                    'values': user_keys
                })
            base_flag_object['targets'] = targets
        except:
            pass

        try:
            base_flag_object['rules'] = []
            for idx, rule in enumerate(self._rules):
                base_flag_object['rules'].append(rule.build(idx))
        except:
            pass

        return base_flag_object


class _FlagRuleBuilder():
    def __init__(self, flag_builder):
        self._flag_builder = flag_builder
        self._clauses = []
        self._variation = None

    def and_match(self, attribute, *values):
        self._clauses.append({
                'attribute': attribute,
                'operator': 'in',
                'values': list(values),
                'negate': False
            })
        return self

    def and_not_match(self, attribute, *values):
        self._clauses.append({
                'attribute': attribute,
                'operator': 'in',
                'values': list(values),
                'negate': True
            })
        return self

    def then_return(self, variation):
        if isinstance(variation, bool):
            self._flag_builder.boolean_flag()
            return self.then_return(variation_for_boolean(variation))
        else:
            self._variation = variation
            self._flag_builder.add_rule(self)
            return self._flag_builder

    def build(self, id):
        return {
            'id': 'rule' + str(id),
            'variation': self._variation,
            'clauses': self._clauses
        }
