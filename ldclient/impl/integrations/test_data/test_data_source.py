import copy
from ldclient.versioned_data_kind import FEATURES
from ldclient.rwlock import ReadWriteLock

TRUE_VARIATION_INDEX = 0
FALSE_VARIATION_INDEX = 1

def variation_for_boolean(variation):
    if variation:
        return TRUE_VARIATION_INDEX
    else:
        return FALSE_VARIATION_INDEX

class TestData():
    def __init__(self):
        self._flag_builders = {}
        self._current_flags = {}
        self._lock = ReadWriteLock()
        self._instances = []

    def __call__(self, config, store, ready):
        data_source = _TestDataSource(store, self)
        try:
            self._lock.lock()
            self._instances.append(data_source)
        finally:
            self._lock.unlock()

        return data_source

    @staticmethod
    def data_source():
        return TestData()

    def flag(self, key):
        try:
            self._lock.rlock()
            if key in self._flag_builders and self._flag_builders[key]:
                return self._flag_builders[key].copy()
            else:
                return FlagBuilder(key).boolean_flag()
        finally:
            self._lock.runlock()

    def update(self, flag_builder):
        try:
            self._lock.lock()

            old_version = 0
            if flag_builder._key in self._current_flags:
                old_flag = self._current_flags[flag_builder._key]
                if old_flag:
                    old_version = old_flag.version

            new_flag = flag_builder.build(old_version + 1)

            self._current_flags[flag_builder._key] = new_flag
            self._flag_builders[flag_builder._key] = flag_builder.copy()
        finally:
            self._lock.unlock()

        for instance in self._instances:
            instance.upsert(new_flag)

        return self


    def make_init_data(self):
        return { FEATURES: copy.copy(self._current_flags) }

    def closed_instance(self, instance):
        try:
            self._lock.lock()
            self._instances.remove(instance)
        finally:
            self._lock.unlock()



class _TestDataSource():

    def __init__(self, feature_store, test_data):
        self._feature_store = feature_store
        self._test_data = test_data

    def start(self):
        self._feature_store.init(self._test_data.make_init_data())

    def stop(self):
        self._test_data.closed_instance(self)

    def initialized(self):
        return True

    def upsert(self, new_flag):
        self._feature_store.upsert(FEATURES, new_flag)



class FlagBuilder():
    def __init__(self, key):
        self._key = key
        self._on = True
        self._variations = []
        self._off_variation = None
        self._fallthrough_variation = None
        self._targets = {}
        self._rules = []


    def copy(self):
        to = FlagBuilder(self._key)

        to._on = self._on
        to._variations = copy.copy(self._variations)
        to._off_variation = self._off_variation
        to._fallthrough_variation = self._fallthrough_variation
        to._targets = copy.copy(self._targets)
        to._rules = copy.copy(self._rules)

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
            # `variation` is True/False value
            return self.boolean_flag().variation_for_user(user_key, variation_for_boolean(variation))
        else:
            # `variation` specifies the index of the variation to set
            targets = self._targets

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

        base_flag_object['off_variation'] = self._off_variation
        base_flag_object['fallthrough_variation'] = self._fallthrough_variation

        targets = []
        for var_index, user_keys in self._targets.items():
            targets.append({
                'variation': var_index,
                'values': user_keys
            })
        base_flag_object['targets'] = targets

        base_flag_object['rules'] = []
        for idx, rule in enumerate(self._rules):
            base_flag_object['rules'].append(rule.build(idx))

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
