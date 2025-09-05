from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional, Set, Union

from ldclient.context import Context
from ldclient.impl.integrations.test_datav2.test_data_sourcev2 import (
    _TestDataSourceV2
)
from ldclient.impl.rwlock import ReadWriteLock

TRUE_VARIATION_INDEX = 0
FALSE_VARIATION_INDEX = 1


def _variation_for_boolean(variation):
    return TRUE_VARIATION_INDEX if variation else FALSE_VARIATION_INDEX


class FlagRuleBuilderV2:
    """
    A builder for feature flag rules to be used with :class:`ldclient.integrations.test_datav2.FlagBuilderV2`.

    In the LaunchDarkly model, a flag can have any number of rules, and a rule can have any number of
    clauses. A clause is an individual test such as \"name is 'X'\". A rule matches a user if all of the
    rule's clauses match the user.

    To start defining a rule, use one of the flag builder's matching methods such as
    :meth:`ldclient.integrations.test_datav2.FlagBuilderV2.if_match()`.
    This defines the first clause for the rule.  Optionally, you may add more
    clauses with the rule builder's methods such as
    :meth:`ldclient.integrations.test_datav2.FlagRuleBuilderV2.and_match()` or
    :meth:`ldclient.integrations.test_datav2.FlagRuleBuilderV2.and_not_match()`.
    Finally, call :meth:`ldclient.integrations.test_datav2.FlagRuleBuilderV2.then_return()`
    to finish defining the rule.
    """

    def __init__(self, flag_builder: FlagBuilderV2):
        self._flag_builder = flag_builder
        self._clauses: List[dict] = []
        self._variation: Optional[int] = None

    def and_match(self, attribute: str, *values) -> FlagRuleBuilderV2:
        """
        Adds another clause, using the \"is one of\" operator.

        This is a shortcut for calling :meth:`ldclient.integrations.test_datav2.FlagRuleBuilderV2.and_match_context()`
        with \"user\" as the context kind.

        **Example:** create a rule that returns ``True`` if the name is \"Patsy\" and the country is \"gb\"
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

    def and_match_context(self, context_kind: str, attribute: str, *values) -> FlagRuleBuilderV2:
        """
        Adds another clause, using the \"is one of\" operator. This matching expression only
        applies to contexts of a specific kind.

        **Example:** create a rule that returns ``True`` if the name attribute for the
        \"company\" context is \"Ella\", and the country attribute for the \"company\" context is \"gb\":
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

    def and_not_match(self, attribute: str, *values) -> FlagRuleBuilderV2:
        """
        Adds another clause, using the \"is not one of\" operator.

        This is a shortcut for calling :meth:`ldclient.integrations.test_datav2.FlagRuleBuilderV2.and_not_match_context()`
        with \"user\" as the context kind.

        **Example:** create a rule that returns ``True`` if the name is \"Patsy\" and the country is not \"gb\"
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

    def and_not_match_context(self, context_kind: str, attribute: str, *values) -> FlagRuleBuilderV2:
        """
        Adds another clause, using the \"is not one of\" operator. This matching expression only
        applies to contexts of a specific kind.

        **Example:** create a rule that returns ``True`` if the name attribute for the
        \"company\" context is \"Ella\", and the country attribute for the \"company\" context is not \"gb\":
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

    def then_return(self, variation: Union[bool, int]) -> FlagBuilderV2:
        """
        Finishes defining the rule, specifying the result as either a boolean
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

        self._variation = variation
        self._flag_builder._add_rule(self)
        return self._flag_builder

    # Note that _build is private by convention, because we don't want developers to
    # consider it part of the public API, but it is still called from FlagBuilderV2.
    def _build(self, id: str) -> dict:
        """
        Creates a dictionary representation of the rule

        :param id: the rule id
        :return: the dictionary representation of the rule
        """
        return {'id': 'rule' + id, 'variation': self._variation, 'clauses': self._clauses}


class FlagBuilderV2:
    """
    A builder for feature flag configurations to be used with :class:`ldclient.integrations.test_datav2.TestDataV2`.

    :see: :meth:`ldclient.integrations.test_datav2.TestDataV2.flag()`
    :see: :meth:`ldclient.integrations.test_datav2.TestDataV2.update()`
    """

    def __init__(self, key: str):
        """:param str key: The name of the flag"""
        self._key = key
        self._on = True
        self._variations: List[Any] = []
        self._off_variation: Optional[int] = None
        self._fallthrough_variation: Optional[int] = None
        self._targets: Dict[str, Dict[int, Set[str]]] = {}
        self._rules: List[FlagRuleBuilderV2] = []

    # Note that _copy is private by convention, because we don't want developers to
    # consider it part of the public API, but it is still called from TestDataV2.
    def _copy(self) -> FlagBuilderV2:
        """
        Creates a deep copy of the flag builder. Subsequent updates to the
        original ``FlagBuilderV2`` object will not update the copy and vise versa.

        :return: a copy of the flag builder object
        """
        to = FlagBuilderV2(self._key)

        to._on = self._on
        to._variations = copy.copy(self._variations)
        to._off_variation = self._off_variation
        to._fallthrough_variation = self._fallthrough_variation
        to._targets = dict()
        for k, v in self._targets.items():
            to._targets[k] = copy.copy(v)
        to._rules = copy.copy(self._rules)

        return to

    def on(self, on: bool) -> FlagBuilderV2:
        """
        Sets targeting to be on or off for this flag.

        The effect of this depends on the rest of the flag configuration, just as it does on the
        real LaunchDarkly dashboard. In the default configuration that you get from calling
        :meth:`ldclient.integrations.test_datav2.TestDataV2.flag()` with a new flag key,
        the flag will return ``False`` whenever targeting is off, and ``True`` when
        targeting is on.

        :param on: ``True`` if targeting should be on
        :return: the flag builder
        """
        self._on = on
        return self

    def fallthrough_variation(self, variation: Union[bool, int]) -> FlagBuilderV2:
        """
        Specifies the fallthrough variation. The fallthrough is the value
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

        self._fallthrough_variation = variation
        return self

    def off_variation(self, variation: Union[bool, int]) -> FlagBuilderV2:
        """
        Specifies the fallthrough variation. This is the variation that is returned
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

        self._off_variation = variation
        return self

    def boolean_flag(self) -> FlagBuilderV2:
        """
        A shortcut for setting the flag to use the standard boolean configuration.

        This is the default for all new flags created with
        :meth:`ldclient.integrations.test_datav2.TestDataV2.flag()`.

        The flag will have two variations, ``True`` and ``False`` (in that order);
        it will return ``False`` whenever targeting is off, and ``True`` when targeting is on
        if no other settings specify otherwise.

        :return: the flag builder
        """
        if self._is_boolean_flag():
            return self

        return self.variations(True, False).fallthrough_variation(TRUE_VARIATION_INDEX).off_variation(FALSE_VARIATION_INDEX)

    def _is_boolean_flag(self):
        return len(self._variations) == 2 and self._variations[TRUE_VARIATION_INDEX] is True and self._variations[FALSE_VARIATION_INDEX] is False

    def variations(self, *variations) -> FlagBuilderV2:
        """
        Changes the allowable variation values for the flag.

        The value may be of any valid JSON type. For instance, a boolean flag
        normally has ``True, False``; a string-valued flag might have
        ``'red', 'green'``; etc.

        **Example:** A single variation
        ::

             td.flag('new-flag').variations(True)

        **Example:** Multiple variations
        ::

            td.flag('new-flag').variations('red', 'green', 'blue')

        :param variations: the desired variations
        :return: the flag builder
        """
        self._variations = list(variations)

        return self

    def variation_for_all(self, variation: Union[bool, int]) -> FlagBuilderV2:
        """
        Sets the flag to always return the specified variation for all contexts.

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

        return self.clear_rules().clear_targets().on(True).fallthrough_variation(variation)

    def value_for_all(self, value: Any) -> FlagBuilderV2:
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

    def variation_for_user(self, user_key: str, variation: Union[bool, int]) -> FlagBuilderV2:
        """
        Sets the flag to return the specified variation for a specific user key when targeting
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

    def variation_for_key(self, context_kind: str, context_key: str, variation: Union[bool, int]) -> FlagBuilderV2:
        """
        Sets the flag to return the specified variation for a specific context, identified
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

    def _add_rule(self, flag_rule_builder: FlagRuleBuilderV2):
        self._rules.append(flag_rule_builder)

    def if_match(self, attribute: str, *values) -> FlagRuleBuilderV2:
        """
        Starts defining a flag rule, using the \"is one of\" operator.

        This is a shortcut for calling :meth:`ldclient.integrations.test_datav2.FlagBuilderV2.if_match_context()`
        with \"user\" as the context kind.

        **Example:** create a rule that returns ``True`` if the name is \"Patsy\" or \"Edina\"
        ::

            td.flag(\"flag\") \\
                .if_match('name', 'Patsy', 'Edina') \\
                .then_return(True)

        :param attribute: the user attribute to match against
        :param values: values to compare to
        :return: the flag rule builder
        """
        return self.if_match_context(Context.DEFAULT_KIND, attribute, *values)

    def if_match_context(self, context_kind: str, attribute: str, *values) -> FlagRuleBuilderV2:
        """
        Starts defining a flag rule, using the \"is one of\" operator. This matching expression only
        applies to contexts of a specific kind.

        **Example:** create a rule that returns ``True`` if the name attribute for the
        company\" context is \"Ella\" or \"Monsoon\":
        ::

            td.flag(\"flag\") \\
                .if_match_context('company', 'name', 'Ella', 'Monsoon') \\
                .then_return(True)

        :param context_kind: the context kind
        :param attribute: the context attribute to match against
        :param values: values to compare to
        :return: the flag rule builder
        """
        flag_rule_builder = FlagRuleBuilderV2(self)
        return flag_rule_builder.and_match_context(context_kind, attribute, *values)

    def if_not_match(self, attribute: str, *values) -> FlagRuleBuilderV2:
        """
        Starts defining a flag rule, using the \"is not one of\" operator.

        This is a shortcut for calling :meth:`ldclient.integrations.test_datav2.FlagBuilderV2.if_not_match_context()`
        with \"user\" as the context kind.

        **Example:** create a rule that returns ``True`` if the name is neither \"Saffron\" nor \"Bubble\"
        ::

            td.flag(\"flag\") \\
                .if_not_match('name', 'Saffron', 'Bubble') \\
                .then_return(True)

        :param attribute: the user attribute to match against
        :param values: values to compare to
        :return: the flag rule builder
        """
        return self.if_not_match_context(Context.DEFAULT_KIND, attribute, *values)

    def if_not_match_context(self, context_kind: str, attribute: str, *values) -> FlagRuleBuilderV2:
        """
        Starts defining a flag rule, using the \"is not one of\" operator. This matching expression only
        applies to contexts of a specific kind.

        **Example:** create a rule that returns ``True`` if the name attribute for the
        \"company\" context is neither \"Pendant\" nor \"Sterling Cooper\":
        ::

            td.flag(\"flag\") \\
                .if_not_match('company', 'name', 'Pendant', 'Sterling Cooper') \\
                .then_return(True)

        :param context_kind: the context kind
        :param attribute: the context attribute to match against
        :param values: values to compare to
        :return: the flag rule builder
        """
        flag_rule_builder = FlagRuleBuilderV2(self)
        return flag_rule_builder.and_not_match_context(context_kind, attribute, *values)

    def clear_rules(self) -> FlagBuilderV2:
        """
        Removes any existing rules from the flag.
        This undoes the effect of methods like
        :meth:`ldclient.integrations.test_datav2.FlagBuilderV2.if_match()`.

        :return: the same flag builder
        """
        self._rules = []
        return self

    def clear_targets(self) -> FlagBuilderV2:
        """
        Removes any existing targets from the flag.
        This undoes the effect of methods like
        :meth:`ldclient.integrations.test_datav2.FlagBuilderV2.variation_for_user()`.

        :return: the same flag builder
        """
        self._targets = {}
        return self

    # Note that _build is private by convention, because we don't want developers to
    # consider it part of the public API, but it is still called from TestDataV2.
    def _build(self, version: int) -> dict:
        """
        Creates a dictionary representation of the flag

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


class TestDataV2:
    """
    A mechanism for providing dynamically updatable feature flag state in a
    simplified form to an SDK client in test scenarios using the FDv2 protocol.

    This type is not stable, and not subject to any backwards
    compatibility guarantees or semantic versioning. It is not suitable for production usage.

    Do not use it.
    You have been warned.

    Unlike ``Files``, this mechanism does not use any external resources. It provides only
    the data that the application has put into it using the ``update`` method.
    ::

        from ldclient.impl.datasystem import config as datasystem_config

        td = TestDataV2.data_source()
        td.update(td.flag('flag-key-1').variation_for_all(True))

        # Configure the data system with TestDataV2 as both initializer and synchronizer
        data_config = datasystem_config.custom()
        data_config.initializers([lambda: td.build_initializer()])
        data_config.synchronizers(lambda: td.build_synchronizer())

        # TODO(fdv2): This will be integrated with the main Config in a future version
        # For now, TestDataV2 is primarily intended for unit testing scenarios

        # flags can be updated at any time:
        td.update(td.flag('flag-key-1').
            variation_for_user('some-user-key', True).
            fallthrough_variation(False))

    The above example uses a simple boolean flag, but more complex configurations are possible using
    the methods of the ``FlagBuilderV2`` that is returned by ``flag``. ``FlagBuilderV2``
    supports many of the ways a flag can be configured on the LaunchDarkly dashboard, but does not
    currently support 1. rule operators other than "in" and "not in", or 2. percentage rollouts.

    If the same ``TestDataV2`` instance is used to configure multiple ``LDClient`` instances,
    any changes made to the data will propagate to all of the ``LDClient`` instances.
    """

    # Prevent pytest from treating this as a test class
    __test__ = False

    def __init__(self):
        self._flag_builders = {}
        self._current_flags = {}
        self._lock = ReadWriteLock()
        self._instances = []
        self._version = 0

    @staticmethod
    def data_source() -> TestDataV2:
        """
        Creates a new instance of the test data source.

        :return: a new configurable test data source
        """
        return TestDataV2()

    def flag(self, key: str) -> FlagBuilderV2:
        """
        Creates or copies a ``FlagBuilderV2`` for building a test flag configuration.

        If this flag key has already been defined in this ``TestDataV2`` instance, then the builder
        starts with the same configuration that was last provided for this flag.

        Otherwise, it starts with a new default configuration in which the flag has ``True`` and
        ``False`` variations, is ``True`` for all users when targeting is turned on and
        ``False`` otherwise, and currently has targeting turned on. You can change any of those
        properties, and provide more complex behavior, using the ``FlagBuilderV2`` methods.

        Once you have set the desired configuration, pass the builder to ``update``.

        :param str key: the flag key
        :return: the flag configuration builder object
        """
        try:
            self._lock.rlock()
            if key in self._flag_builders and self._flag_builders[key]:
                return self._flag_builders[key]._copy()

            return FlagBuilderV2(key).boolean_flag()
        finally:
            self._lock.runlock()

    def update(self, flag_builder: FlagBuilderV2) -> TestDataV2:
        """
        Updates the test data with the specified flag configuration.

        This has the same effect as if a flag were added or modified on the LaunchDarkly dashboard.
        It immediately propagates the flag change to any ``LDClient`` instance(s) that you have
        already configured to use this ``TestDataV2``. If no ``LDClient`` has been started yet,
        it simply adds this flag to the test data which will be provided to any ``LDClient`` that
        you subsequently configure.

        Any subsequent changes to this ``FlagBuilderV2`` instance do not affect the test data,
        unless you call ``update`` again.

        :param flag_builder: a flag configuration builder
        :return: self (the TestDataV2 object)
        """
        instances_copy = []
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

            # Create a copy of instances while holding the lock to avoid race conditions
            instances_copy = list(self._instances)
        finally:
            self._lock.unlock()

        for instance in instances_copy:
            instance.upsert_flag(new_flag)

        return self

    def _make_init_data(self) -> Dict[str, Any]:
        try:
            self._lock.rlock()
            return copy.copy(self._current_flags)
        finally:
            self._lock.runlock()

    def _get_version(self) -> int:
        try:
            self._lock.lock()
            version = self._version
            self._version += 1
            return version
        finally:
            self._lock.unlock()

    def _closed_instance(self, instance):
        try:
            self._lock.lock()
            if instance in self._instances:
                self._instances.remove(instance)
        finally:
            self._lock.unlock()

    def _add_instance(self, instance):
        try:
            self._lock.lock()
            self._instances.append(instance)
        finally:
            self._lock.unlock()

    def build_initializer(self) -> _TestDataSourceV2:
        """
        Creates an initializer that can be used with the FDv2 data system.

        :return: a test data initializer
        """
        return _TestDataSourceV2(self)

    def build_synchronizer(self) -> _TestDataSourceV2:
        """
        Creates a synchronizer that can be used with the FDv2 data system.

        :return: a test data synchronizer
        """
        return _TestDataSourceV2(self)
