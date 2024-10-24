import copy
from typing import Any, Dict, List, Optional, Set, Union

from ldclient.context import Context
from ldclient.impl.integrations.test_data.test_data_source import \
    _TestDataSource
from ldclient.impl.rwlock import ReadWriteLock
from ldclient.versioned_data_kind import FEATURES

TRUE_VARIATION_INDEX = 0
FALSE_VARIATION_INDEX = 1


def _variation_for_boolean(variation):
    if variation:
        return TRUE_VARIATION_INDEX
    else:
        return FALSE_VARIATION_INDEX


class TestData:
    """A mechanism for providing dynamically updatable feature flag state in a
    simplified form to an SDK client in test scenarios.

    Unlike ``Files``, this mechanism does not use any external resources. It provides only
    the data that the application has put into it using the ``update`` method.
    ::

        td = TestData.data_source()
        td.update(td.flag('flag-key-1').variation_for_all(True))

        client = LDClient(config=Config('SDK_KEY', update_processor_class = td))

        # flags can be updated at any time:
        td.update(td.flag('flag-key-1'). \\
            variation_for_user('some-user-key', True). \\
            fallthrough_variation(False))

    The above example uses a simple boolean flag, but more complex configurations are possible using
    the methods of the ``FlagBuilder`` that is returned by ``flag``. ``FlagBuilder``
    supports many of the ways a flag can be configured on the LaunchDarkly dashboard, but does not
    currently support 1. rule operators other than "in" and "not in", or 2. percentage rollouts.

    If the same ``TestData`` instance is used to configure multiple ``LDClient`` instances,
    any changes made to the data will propagate to all of the ``LDClient`` instances.
    """

    # Prevent pytest from treating this as a test class
    __test__ = False

    def __init__(self):
        self._flag_builders = {}
        self._current_flags = {}
        self._lock = ReadWriteLock()
        self._instances = []

    def __call__(self, config, store, ready):
        data_source = _TestDataSource(store, self, ready)
        try:
            self._lock.lock()
            self._instances.append(data_source)
        finally:
            self._lock.unlock()

        return data_source

    @staticmethod
    def data_source() -> 'TestData':
        """Creates a new instance of the test data source.

        :return: a new configurable test data source
        """
        return TestData()

    def flag(self, key: str) -> 'FlagBuilder':
        """Creates or copies a ``FlagBuilder`` for building a test flag configuration.

        If this flag key has already been defined in this ``TestData`` instance, then the builder
        starts with the same configuration that was last provided for this flag.

        Otherwise, it starts with a new default configuration in which the flag has ``True`` and
        ``False`` variations, is ``True`` for all users when targeting is turned on and
        ``False`` otherwise, and currently has targeting turned on. You can change any of those
        properties, and provide more complex behavior, using the ``FlagBuilder`` methods.

        Once you have set the desired configuration, pass the builder to ``update``.

        :param str key: the flag key
        :return: the flag configuration builder object
        """
        try:
            self._lock.rlock()
            if key in self._flag_builders and self._flag_builders[key]:
                return self._flag_builders[key]._copy()
            else:
                return FlagBuilder(key).boolean_flag()
        finally:
            self._lock.runlock()

    def update(self, flag_builder: 'FlagBuilder') -> 'TestData':
        """Updates the test data with the specified flag configuration.

        This has the same effect as if a flag were added or modified on the LaunchDarkly dashboard.
        It immediately propagates the flag change to any ``LDClient`` instance(s) that you have
        already configured to use this ``TestData``. If no ``LDClient`` has been started yet,
        it simply adds this flag to the test data which will be provided to any ``LDClient`` that
        you subsequently configure.

        Any subsequent changes to this ``FlagBuilder`` instance do not affect the test data,
        unless you call ``update`` again.

        :param flag_builder: a flag configuration builder
        :return: self (the TestData object)
        """
        try:
            self._lock.lock()

            old_version = 0
            if flag_builder._key in self._current_flags:
                old_flag = self._current_flags[flag_builder._key]
                if old_flag:
                    old_version = old_flag['version']

            new_flag = flag_builder._build(old_version + 1)

            self._current_flags[flag_builder._key] = new_flag
            self._flag_builders[flag_builder._key] = flag_builder._copy()
        finally:
            self._lock.unlock()

        for instance in self._instances:
            instance.upsert(new_flag)

        return self

    def _make_init_data(self) -> dict:
        return {FEATURES: copy.copy(self._current_flags)}

    def _closed_instance(self, instance):
        try:
            self._lock.lock()
            self._instances.remove(instance)
        finally:
            self._lock.unlock()


class FlagBuilder:
    """A builder for feature flag configurations to be used with :class:`ldclient.integrations.test_data.TestData`.

    :see: :meth:`ldclient.integrations.test_data.TestData.flag()`
    :see: :meth:`ldclient.integrations.test_data.TestData.update()`
    """

    def __init__(self, key: str):
        """:param str key: The name of the flag"""
        self._key = key
        self._on = True
        self._variations = []  # type: List[Any]
        self._off_variation = None  # type: Optional[int]
        self._fallthrough_variation = None  # type: Optional[int]
        self._targets = {}  # type: Dict[str, Dict[int, Set[str]]]
        self._rules = []  # type: List[FlagRuleBuilder]

    # Note that _copy is private by convention, because we don't want developers to
    # consider it part of the public API, but it is still called from TestData.
    def _copy(self) -> 'FlagBuilder':
        """Creates a deep copy of the flag builder. Subsequent updates to the
        original ``FlagBuilder`` object will not update the copy and vise versa.

        :return: a copy of the flag builder object
        """
        to = FlagBuilder(self._key)

        to._on = self._on
        to._variations = copy.copy(self._variations)
        to._off_variation = self._off_variation
        to._fallthrough_variation = self._fallthrough_variation
        to._targets = dict()
        for k, v in self._targets.items():
            to._targets[k] = copy.copy(v)
        to._rules = copy.copy(self._rules)

        return to

    def on(self, on: bool) -> 'FlagBuilder':
        """Sets targeting to be on or off for this flag.

        The effect of this depends on the rest of the flag configuration, just as it does on the
        real LaunchDarkly dashboard. In the default configuration that you get from calling
        :meth:`ldclient.integrations.test_data.TestData.flag()` with a new flag key,
        the flag will return ``False`` whenever targeting is off, and ``True`` when
        targeting is on.

        :param on: ``True`` if targeting should be on
        :return: the flag builder
        """
        self._on = on
        return self

    def fallthrough_variation(self, variation: Union[bool, int]) -> 'FlagBuilder':
        """Specifies the fallthrough variation. The fallthrough is the value
        that is returned if targeting is on and the user was not matched by a more specific
        target or rule.

        If the flag was previously configured with other variations and the variation
        specified is a boolean, this also changes it to a boolean flag.

        :param bool|int variation: ``True`` or ``False`` or the desired fallthrough variation index:
            ``0`` for the first, ``1`` for the second, etc.
        :return: the flag builder
        """
        if isinstance(variation, bool):
            self.boolean_flag()._fallthrough_variation = _variation_for_boolean(variation)
            return self
        else:
            self._fallthrough_variation = variation
            return self

    def off_variation(self, variation: Union[bool, int]) -> 'FlagBuilder':
        """Specifies the fallthrough variation. This is the variation that is returned
        whenever targeting is off.

        If the flag was previously configured with other variations and the variation
        specified is a boolean, this also changes it to a boolean flag.

        :param bool|int variation: ``True`` or ``False`` or the desired off variation index:
            ``0`` for the first, ``1`` for the second, etc.
        :return: the flag builder
        """
        if isinstance(variation, bool):
            self.boolean_flag()._off_variation = _variation_for_boolean(variation)
            return self
        else:
            self._off_variation = variation
            return self

    def boolean_flag(self) -> 'FlagBuilder':
        """A shortcut for setting the flag to use the standard boolean configuration.

        This is the default for all new flags created with
        :meth:`ldclient.integrations.test_data.TestData.flag()`.

        The flag will have two variations, ``True`` and ``False`` (in that order);
        it will return ``False`` whenever targeting is off, and ``True`` when targeting is on
        if no other settings specify otherwise.

        :return: the flag builder
        """
        if self._is_boolean_flag():
            return self
        else:
            return self.variations(True, False).fallthrough_variation(TRUE_VARIATION_INDEX).off_variation(FALSE_VARIATION_INDEX)

    def _is_boolean_flag(self):
        return len(self._variations) == 2 and self._variations[TRUE_VARIATION_INDEX] is True and self._variations[FALSE_VARIATION_INDEX] is False

    def variations(self, *variations) -> 'FlagBuilder':
        """Changes the allowable variation values for the flag.

        The value may be of any valid JSON type. For instance, a boolean flag
        normally has ``True, False``; a string-valued flag might have
        ``'red', 'green'``; etc.

        **Example:** A single variation
        ::

             td.flag('new-flag').variations(True)

        **Example:** Multiple variations
        ::

            td.flag('new-flag').variations('red', 'green', 'blue')

        :param variations: the the desired variations
        :return: the flag builder
        """
        self._variations = list(variations)

        return self

    def variation_for_all(self, variation: Union[bool, int]) -> 'FlagBuilder':
        """Sets the flag to always return the specified variation for all contexts.

        The variation is specified, targeting is switched on, and any existing targets or rules are removed.
        The fallthrough variation is set to the specified value. The off variation is left unchanged.

        If the flag was previously configured with other variations and the variation specified is a boolean,
        this also changes it to a boolean flag.

        :param bool|int variation: ``True`` or ``False`` or the desired variation index to return:
            ``0`` for the first, ``1`` for the second, etc.
        :return: the flag builder
        """
        if isinstance(variation, bool):
            return self.boolean_flag().variation_for_all(_variation_for_boolean(variation))
        else:
            return self.clear_rules().clear_targets().on(True).fallthrough_variation(variation)

    def value_for_all(self, value: Any) -> 'FlagBuilder':
        """
        Sets the flag to always return the specified variation value for all users.

        The value may be of any JSON type. This method changes the flag to have only
        a single variation, which is this value, and to return the same variation
        regardless of whether targeting is on or off. Any existing targets or rules
        are removed.

        :param value the desired value to be returned for all users
        :return the flag builder
        """
        return self.variations(value).variation_for_all(0)

    def variation_for_user(self, user_key: str, variation: Union[bool, int]) -> 'FlagBuilder':
        """Sets the flag to return the specified variation for a specific user key when targeting
        is on.

        This has no effect when targeting is turned off for the flag.

        If the flag was previously configured with other variations and the variation specified is a boolean,
        this also changes it to a boolean flag.

        :param user_key: a user key
        :param bool|int variation: ``True`` or ``False`` or the desired variation index to return:
            ``0`` for the first, ``1`` for the second, etc.
        :return: the flag builder
        """
        return self.variation_for_key(Context.DEFAULT_KIND, user_key, variation)

    def variation_for_key(self, context_kind: str, context_key: str, variation: Union[bool, int]) -> 'FlagBuilder':
        """Sets the flag to return the specified variation for a specific context, identified
        by context kind and key, when targeting is on.

        This has no effect when targeting is turned off for the flag.

        If the flag was previously configured with other variations and the variation specified is a boolean,
        this also changes it to a boolean flag.

        :param context_kind: the context kind
        :param context_key: the context key
        :param bool|int variation: ``True`` or ``False`` or the desired variation index to return:
            ``0`` for the first, ``1`` for the second, etc.
        :return: the flag builder
        """
        if isinstance(variation, bool):
            # `variation` is True/False value
            return self.boolean_flag().variation_for_key(context_kind, context_key, _variation_for_boolean(variation))

        # `variation` specifies the index of the variation to set
        targets = self._targets.get(context_kind)
        if targets is None:
            targets = {}
            self._targets[context_kind] = targets

        for idx, var in enumerate(self._variations):
            if idx == variation:
                # If there is no set at the current variation, set it to be empty
                target_for_variation = targets.get(idx)
                if target_for_variation is None:
                    target_for_variation = set()
                    targets[idx] = target_for_variation

                # If key is not in the current variation set, add it
                target_for_variation.add(context_key)

            else:
                # Remove key from the other variation set if necessary
                if idx in targets:
                    targets[idx].discard(context_key)

        return self

    def _add_rule(self, flag_rule_builder: 'FlagRuleBuilder'):
        self._rules.append(flag_rule_builder)

    def if_match(self, attribute: str, *values) -> 'FlagRuleBuilder':
        """Starts defining a flag rule, using the "is one of" operator.

        This is a shortcut for calling :meth:`ldclient.integrations.test_data.FlagBuilder.if_match_context()`
        with "user" as the context kind.

        **Example:** create a rule that returns ``True`` if the name is "Patsy" or "Edina"
        ::

            td.flag("flag") \\
                .if_match('name', 'Patsy', 'Edina') \\
                .then_return(True)

        :param attribute: the user attribute to match against
        :param values: values to compare to
        :return: the flag rule builder
        """
        return self.if_match_context(Context.DEFAULT_KIND, attribute, *values)

    def if_match_context(self, context_kind: str, attribute: str, *values) -> 'FlagRuleBuilder':
        """Starts defining a flag rule, using the "is one of" operator. This matching expression only
        applies to contexts of a specific kind.

        **Example:** create a rule that returns ``True`` if the name attribute for the
        company" context is "Ella" or "Monsoon":
        ::

            td.flag("flag") \\
                .if_match_context('company', 'name', 'Ella', 'Monsoon') \\
                .then_return(True)

        :param context_kind: the context kind
        :param attribute: the context attribute to match against
        :param values: values to compare to
        :return: the flag rule builder
        """
        flag_rule_builder = FlagRuleBuilder(self)
        return flag_rule_builder.and_match_context(context_kind, attribute, *values)

    def if_not_match(self, attribute: str, *values) -> 'FlagRuleBuilder':
        """Starts defining a flag rule, using the "is not one of" operator.

        This is a shortcut for calling :meth:`ldclient.integrations.test_data.FlagBuilder.if_not_match_context()`
        with "user" as the context kind.

        **Example:** create a rule that returns ``True`` if the name is neither "Saffron" nor "Bubble"
        ::

            td.flag("flag") \\
                .if_not_match('name', 'Saffron', 'Bubble') \\
                .then_return(True)

        :param attribute: the user attribute to match against
        :param values: values to compare to
        :return: the flag rule builder
        """
        return self.if_not_match_context(Context.DEFAULT_KIND, attribute, *values)

    def if_not_match_context(self, context_kind: str, attribute: str, *values) -> 'FlagRuleBuilder':
        """Starts defining a flag rule, using the "is not one of" operator. This matching expression only
        applies to contexts of a specific kind.

        **Example:** create a rule that returns ``True`` if the name attribute for the
        "company" context is neither "Pendant" nor "Sterling Cooper":
        ::

            td.flag("flag") \\
                .if_not_match('company', 'name', 'Pendant', 'Sterling Cooper') \\
                .then_return(True)

        :param context_kind: the context kind
        :param attribute: the context attribute to match against
        :param values: values to compare to
        :return: the flag rule builder
        """
        flag_rule_builder = FlagRuleBuilder(self)
        return flag_rule_builder.and_not_match_context(context_kind, attribute, *values)

    def clear_rules(self) -> 'FlagBuilder':
        """Removes any existing rules from the flag.
        This undoes the effect of methods like
        :meth:`ldclient.integrations.test_data.FlagBuilder.if_match()`.

        :return: the same flag builder
        """
        self._rules = []
        return self

    def clear_targets(self) -> 'FlagBuilder':
        """Removes any existing targets from the flag.
        This undoes the effect of methods like
        :meth:`ldclient.integrations.test_data.FlagBuilder.variation_for_user()`.

        :return: the same flag builder
        """
        self._targets = {}
        return self

    # Note that _build is private by convention, because we don't want developers to
    # consider it part of the public API, but it is still called from TestData.
    def _build(self, version: int) -> dict:
        """Creates a dictionary representation of the flag

        :param version: the version number of the rule
        :return: the dictionary representation of the flag
        """
        base_flag_object = {'key': self._key, 'version': version, 'on': self._on, 'variations': self._variations, 'prerequisites': [], 'salt': ''}

        base_flag_object['offVariation'] = self._off_variation
        base_flag_object['fallthrough'] = {'variation': self._fallthrough_variation}

        targets = []
        context_targets = []
        for target_context_kind, target_variations in self._targets.items():
            for var_index, target_keys in target_variations.items():
                if target_context_kind == Context.DEFAULT_KIND:
                    targets.append({'variation': var_index, 'values': sorted(list(target_keys))})  # sorting just for test determinacy
                    context_targets.append({'contextKind': target_context_kind, 'variation': var_index, 'values': []})
                else:
                    context_targets.append({'contextKind': target_context_kind, 'variation': var_index, 'values': sorted(list(target_keys))})  # sorting just for test determinacy
        base_flag_object['targets'] = targets
        base_flag_object['contextTargets'] = context_targets

        rules = []
        for idx, rule in enumerate(self._rules):
            rules.append(rule._build(str(idx)))
        base_flag_object['rules'] = rules

        return base_flag_object


class FlagRuleBuilder:
    """
    A builder for feature flag rules to be used with :class:`ldclient.integrations.test_data.FlagBuilder`.

    In the LaunchDarkly model, a flag can have any number of rules, and a rule can have any number of
    clauses. A clause is an individual test such as "name is 'X'". A rule matches a user if all of the
    rule's clauses match the user.

    To start defining a rule, use one of the flag builder's matching methods such as
    :meth:`ldclient.integrations.test_data.FlagBuilder.if_match()`.
    This defines the first clause for the rule.  Optionally, you may add more
    clauses with the rule builder's methods such as
    :meth:`ldclient.integrations.test_data.FlagRuleBuilder.and_match()` or
    :meth:`ldclient.integrations.test_data.FlagRuleBuilder.and_not_match()`.
    Finally, call :meth:`ldclient.integrations.test_data.FlagRuleBuilder.then_return()`
    to finish defining the rule.
    """

    def __init__(self, flag_builder: FlagBuilder):
        self._flag_builder = flag_builder
        self._clauses = []  # type: List[dict]
        self._variation = None  # type: Optional[int]

    def and_match(self, attribute: str, *values) -> 'FlagRuleBuilder':
        """Adds another clause, using the "is one of" operator.

        This is a shortcut for calling :meth:`ldclient.integrations.test_data.FlagRuleBuilder.and_match_context()`
        with "user" as the context kind.

        **Example:** create a rule that returns ``True`` if the name is "Patsy" and the country is "gb"
        ::

            td.flag('flag') \\
                .if_match('name', 'Patsy') \\
                .and_match('country', 'gb') \\
                .then_return(True)

        :param attribute: the user attribute to match against
        :param values: values to compare to
        :return: the flag rule builder
        """
        return self.and_match_context(Context.DEFAULT_KIND, attribute, *values)

    def and_match_context(self, context_kind: str, attribute: str, *values) -> 'FlagRuleBuilder':
        """Adds another clause, using the "is one of" operator. This matching expression only
        applies to contexts of a specific kind.

        **Example:** create a rule that returns ``True`` if the name attribute for the
        "company" context is "Ella", and the country attribute for the "company" context is "gb":
        ::

            td.flag('flag') \\
                .if_match_context('company', 'name', 'Ella') \\
                .and_match_context('company', 'country', 'gb') \\
                .then_return(True)

        :param context_kind: the context kind
        :param attribute: the context attribute to match against
        :param values: values to compare to
        :return: the flag rule builder
        """
        self._clauses.append({'contextKind': context_kind, 'attribute': attribute, 'op': 'in', 'values': list(values), 'negate': False})
        return self

    def and_not_match(self, attribute: str, *values) -> 'FlagRuleBuilder':
        """Adds another clause, using the "is not one of" operator.

        This is a shortcut for calling :meth:`ldclient.integrations.test_data.FlagRuleBuilder.and_not_match_context()`
        with "user" as the context kind.

        **Example:** create a rule that returns ``True`` if the name is "Patsy" and the country is not "gb"
        ::

            td.flag('flag') \\
                .if_match('name', 'Patsy') \\
                .and_not_match('country', 'gb') \\
                .then_return(True)

        :param attribute: the user attribute to match against
        :param values: values to compare to
        :return: the flag rule builder
        """
        return self.and_not_match_context(Context.DEFAULT_KIND, attribute, *values)

    def and_not_match_context(self, context_kind: str, attribute: str, *values) -> 'FlagRuleBuilder':
        """Adds another clause, using the "is not one of" operator. This matching expression only
        applies to contexts of a specific kind.

        **Example:** create a rule that returns ``True`` if the name attribute for the
        "company" context is "Ella", and the country attribute for the "company" context is not "gb":
        ::

            td.flag('flag') \\
                .if_match_context('company', 'name', 'Ella') \\
                .and_not_match_context('company', 'country', 'gb') \\
                .then_return(True)

        :param context_kind: the context kind
        :param attribute: the context attribute to match against
        :param values: values to compare to
        :return: the flag rule builder
        """
        self._clauses.append({'contextKind': context_kind, 'attribute': attribute, 'op': 'in', 'values': list(values), 'negate': True})
        return self

    def then_return(self, variation: Union[bool, int]) -> 'FlagBuilder':
        """Finishes defining the rule, specifying the result as either a boolean
        or a variation index.

        If the flag was previously configured with other variations and the variation specified is a boolean,
        this also changes it to a boolean flag.

        :param bool|int variation: ``True`` or ``False`` or the desired  variation index:
            ``0`` for the first, ``1`` for the second, etc.
        :return:  the flag builder with this rule added
        """
        if isinstance(variation, bool):
            self._flag_builder.boolean_flag()
            return self.then_return(_variation_for_boolean(variation))
        else:
            self._variation = variation
            self._flag_builder._add_rule(self)
            return self._flag_builder

    # Note that _build is private by convention, because we don't want developers to
    # consider it part of the public API, but it is still called from FlagBuilder.
    def _build(self, id: str) -> dict:
        """Creates a dictionary representation of the rule

        :param id: the rule id
        :return: the dictionary representation of the rule
        """
        return {'id': 'rule' + id, 'variation': self._variation, 'clauses': self._clauses}
