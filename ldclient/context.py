"""
This submodule implements the SDK's evaluation context model.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from typing import Any, Dict, Optional, Union

_INVALID_KIND_REGEX = re.compile('[^-a-zA-Z0-9._]')
_USER_STRING_ATTRS = {'name', 'firstName', 'lastName', 'email', 'country', 'avatar', 'ip'}


def _escape_key_for_fully_qualified_key(key: str) -> str:
    # When building a fully-qualified key, ':' and '%' are percent-escaped; we do not use a full
    # URL-encoding function because implementations of this are inconsistent across platforms.
    return key.replace('%', '%25').replace(':', '%3A')


def _validate_kind(kind: str) -> Optional[str]:
    if kind == '':
        return 'context kind must not be empty'
    if kind == 'kind':
        return '"kind" is not a valid context kind'
    if kind == 'multi':
        return 'context of kind "multi" must be created with create_multi or multi_builder'
    if _INVALID_KIND_REGEX.search(kind):
        return 'context kind contains disallowed characters'
    return None


class Context:
    """
    A collection of attributes that can be referenced in flag evaluations and analytics events.
    This entity is also called an "evaluation context."

    To create a Context of a single kind, such as a user, you may use :func:`create()` when only the
    key and the kind are relevant; or, to specify other attributes, use :func:`builder()`.

    To create a Context with multiple kinds (a multi-context), use :func:`create_multi()` or
    :func:`multi_builder()`.

    A Context can be in an error state if it was built with invalid attributes. See :attr:`valid`
    and :attr:`error`.

    A Context is immutable once created.
    """

    DEFAULT_KIND = 'user'
    """A constant for the default context kind of "user"."""

    MULTI_KIND = 'multi'
    """A constant for the kind that all multi-contexts have."""

    def __init__(
        self,
        kind: Optional[str],
        key: str,
        name: Optional[str] = None,
        anonymous: bool = False,
        attributes: Optional[dict] = None,
        private_attributes: Optional[list[str]] = None,
        multi_contexts: Optional[list[Context]] = None,
        allow_empty_key: bool = False,
        error: Optional[str] = None,
    ):
        """
        Constructs an instance, setting all properties. Avoid using this constructor directly.

        Applications should not normally use this constructor; the intended pattern is to use
        factory methods or builders. Calling this constructor directly may result in some context
        validation being skipped.
        """
        if error is not None:
            self.__make_invalid(error)
            return
        if multi_contexts is not None:
            if len(multi_contexts) == 0:
                self.__make_invalid('multi-context must contain at least one kind')
                return
            # Sort them by kind; they need to be sorted for computing a fully-qualified key, but even
            # if fully_qualified_key is never used, this is helpful for __eq__ and determinacy.
            multi_contexts = sorted(multi_contexts, key=lambda c: c.kind)
            last_kind = None
            errors = None  # type: Optional[list[str]]
            full_key = ''
            for c in multi_contexts:
                if c.error is not None:
                    if errors is None:
                        errors = []
                    errors.append(c.error)
                    continue
                if c.kind == last_kind:
                    self.__make_invalid('multi-kind context cannot have same kind more than once')
                    return
                last_kind = c.kind
                if full_key != '':
                    full_key += ':'
                full_key += c.kind + ':' + _escape_key_for_fully_qualified_key(c.key)
            if errors:
                self.__make_invalid(', '.join(errors))
                return
            self.__kind = 'multi'
            self.__multi = multi_contexts  # type: Optional[list[Context]]
            self.__key = ''
            self.__name = None
            self.__anonymous = False
            self.__attributes = None
            self.__private = None
            self.__full_key = full_key
            self.__error = None  # type: Optional[str]
            return
        if kind is None:
            kind = Context.DEFAULT_KIND
        kind_error = _validate_kind(kind)
        if kind_error:
            self.__make_invalid(kind_error)
            return
        if key == '' and not allow_empty_key:
            self.__make_invalid('context key must not be None or empty')
            return
        self.__key = key
        self.__kind = kind
        self.__name = name
        self.__anonymous = anonymous
        self.__attributes = attributes
        self.__private = private_attributes
        self.__multi = None
        self.__full_key = key if kind == Context.DEFAULT_KIND else '%s:%s' % (kind, _escape_key_for_fully_qualified_key(key))
        self.__error = None

    @classmethod
    def create(cls, key: str, kind: Optional[str] = None) -> Context:
        """
        Creates a single-kind Context with only the key and the kind specified.

        If you omit the kind, it defaults to "user" (:const:`DEFAULT_KIND`).

        :param key: the context key
        :param kind: the context kind; if omitted, it is :const:`DEFAULT_KIND` ("user")
        :return: a context

        :see: :func:`builder()`
        :see: :func:`create_multi()`
        """
        return Context(kind, key, None, False, None, None, None, False)

    @classmethod
    def create_multi(cls, *contexts: Context) -> Context:
        """
        Creates a multi-context out of the specified single-kind Contexts.

        To create a Context for a single context kind, use :func:`create()` or
        :func:`builder()`.

        You may use :func:`multi_builder()` instead if you want to add contexts one at a time
        using a builder pattern.

        For the returned Context to be valid, the contexts list must not be empty, and all of its
        elements must be valid Contexts. Otherwise, the returned Context will be invalid as
        reported by :func:`error()`.

        If only one context parameter is given, the method returns that same context.

        If a nested context is a multi-context, this is exactly equivalent to adding each of the
        individual kinds from it separately. See :func:`ldclient.ContextMultiBuilder.add()`.

        :param contexts: the individual contexts
        :return: a multi-context

        :see: :func:`create()`
        :see: :func:`multi_builder()`
        """
        # implementing this via multi_builder gives us the flattening behavior for free
        builder = ContextMultiBuilder()
        for c in contexts:
            builder.add(c)
        return builder.build()

    @classmethod
    def from_dict(cls, props: dict) -> Context:
        """
        Creates a Context from properties in a dictionary, corresponding to the JSON
        representation of a context.

        :param props: the context properties
        :return: a context
        """
        if props is None:
            return Context.__create_with_error('Cannot use None as a context')
        kind = props.get('kind')
        if not isinstance(kind, str):
            return Context.__create_with_schema_type_error('kind')
        if kind == 'multi':
            b = ContextMultiBuilder()
            for k, v in props.items():
                if k != 'kind':
                    if not isinstance(v, dict):
                        return Context.__create_with_schema_type_error(k)
                    c = Context.__from_dict_single(v, k)
                    b.add(c)
            return b.build()
        return Context.__from_dict_single(props, props['kind'])

    @classmethod
    def builder(cls, key: str) -> ContextBuilder:
        """
        Creates a builder for building a Context.

        You may use :class:`ldclient.ContextBuilder` methods to set additional attributes and/or
        change the context kind before calling :func:`ldclient.ContextBuilder.build()`. If you
        do not change any values, the defaults for the Context are that its ``kind`` is :const:`DEFAULT_KIND`,
        its :attr:`key` is set to the key parameter specified here, :attr:`anonymous` is False, and it has no values for
        any other attributes.

        This method is for building a Context that has only a single kind. To define a multi-context,
        use :func:`create_multi()` or :func:`multi_builder()`.

        :param key: the context key
        :return: a new builder

        :see: :func:`create()`
        :see: :func:`create_multi()`

        """
        return ContextBuilder(key)

    @classmethod
    def builder_from_context(cls, context: Context) -> ContextBuilder:
        """
        Creates a builder whose properties are the same as an existing single-kind Context.

        You may then change the builder's state in any way and call :func:`ldclient.ContextBuilder.build()`
        to create a new independent Context.

        :param context: the context to copy from
        :return: a new builder
        """
        return ContextBuilder(context.key, context)

    @classmethod
    def multi_builder(cls) -> ContextMultiBuilder:
        """
        Creates a builder for building a multi-context.

        This method is for building a Context that contains multiple contexts, each for a different
        context kind. To define a single context, use :func:`create()` or :func:`builder()` instead.

        The difference between this method and :func:`create_multi()` is simply that the builder
        allows you to add contexts one at a time, if that is more convenient for your logic.

        :return: a new builder

        :see: :func:`builder()`
        :see: :func:`create_multi()`
        """
        return ContextMultiBuilder()

    @property
    def valid(self) -> bool:
        """
        True for a valid Context, or False for an invalid one.

        A valid context is one that can be used in SDK operations. An invalid context is one that
        is missing necessary attributes or has invalid attributes, indicating an incorrect usage
        of the SDK API. The only ways for a context to be invalid are:

        * The :attr:`kind` property had a disallowed value. See :func:`ldclient.ContextBuilder.kind()`.
        * For a single context, the :attr:`key` property was None or empty.
        * You tried to create a multi-context without specifying any contexts.
        * You tried to create a multi-context using the same context kind more than once.
        * You tried to create a multi-context where at least one of the individual Contexts was invalid.

        In any of these cases, :attr:`valid` will be False, and :attr:`error` will return a
        description of the error.

        Since in normal usage it is easy for applications to be sure they are using context kinds
        correctly, and because throwing an exception is undesirable in application code that uses
        LaunchDarkly, the SDK stores the error state in the Context itself and checks for such
        errors at the time the Context is used, such as in a flag evaluation. At that point, if
        the context is invalid, the operation will fail in some well-defined way as described in
        the documentation for that method, and the SDK will generally log a warning as well. But
        in any situation where you are not sure if you have a valid Context, you can check
        :attr:`valid` or :attr:`error`.
        """
        return self.__error is None

    @property
    def error(self) -> Optional[str]:
        """
        Returns None for a valid Context, or an error message for an invalid one.

        If this is None, then :attr:`valid` is True. If it is not None, then :attr:`valid` is
        False.
        """
        return self.__error

    @property
    def multiple(self) -> bool:
        """
        True if this is a multi-context.

        If this value is True, then :attr:`kind` is guaranteed to be :const:`MULTI_KIND`, and
        you can inspect the individual context for each kind with :func:`get_individual_context()`.

        If this value is False, then :attr:`kind` is guaranteed to return a value that is not
        :const:`MULTI_KIND`.

        :see: :func:`create_multi()`
        """
        return self.__multi is not None

    @property
    def kind(self) -> str:
        """
        Returns the context's ``kind`` attribute.

        Every valid context has a non-empty kind. For multi-contexts, this value is
        :const:`MULTI_KIND` and the kinds within the context can be inspected with
        :func:`get_individual_context()`.

        :see: :func:`ldclient.ContextBuilder.kind()`
        :see: :func:`create()`
        """
        return self.__kind

    @property
    def key(self) -> str:
        """
        Returns the context's ``key`` attribute.

        For a single context, this value is set by :func:`create`, or :func:`ldclient.ContextBuilder.key()`.

        For a multi-context, there is no single value and :attr:`key` returns an empty string. Use
        :func:`get_individual_context()` to get the Context for a particular kind, then get the
        :attr:`key` of that Context.

        :see: :func:`ldclient.ContextBuilder.key()`
        :see: :func:`create()`
        """
        return self.__key

    @property
    def name(self) -> Optional[str]:
        """
        Returns the context's ``name`` attribute.

        For a single context, this value is set by :func:`ldclient.ContextBuilder.name()`. It is
        None if no value was set.

        For a multi-context, there is no single value and :attr:`name` returns None. Use
        :func:`get_individual_context()` to get the Context for a particular kind, then get the
        :attr:`name` of that Context.

        :see: :func:`ldclient.ContextBuilder.name()`
        """
        return self.__name

    @property
    def anonymous(self) -> bool:
        """
        Returns True if this context is only intended for flag evaluations and will not be
        indexed by LaunchDarkly.

        The default value is False. False means that this Context represents an entity such as a
        user that you want to be able to see on the LaunchDarkly dashboard.

        Setting ``anonymous`` to True excludes this context from the database that is
        used by the dashboard. It does not exclude it from analytics event data, so it is
        not the same as making attributes private; all non-private attributes will still be
        included in events and data export. There is no limitation on what other attributes
        may be included (so, for instance, ``anonymous`` does not mean there is no :attr:`name`),
        and the context will still have whatever :attr:`key` you have given it.

        This value is also addressable in evaluations as the attribute name "anonymous". It
        is always treated as a boolean true or false in evaluations.

        :see: :func:`ldclient.ContextBuilder.anonymous()`
        """
        return self.__anonymous

    def without_anonymous_contexts(self) -> Context:
        """
        For a multi-kind context:

        A multi-kind context is made up of two or more single-kind contexts.
        This method will first discard any single-kind contexts which are
        anonymous. It will then create a new multi-kind context from the
        remaining single-kind contexts. This may result in an invalid context
        (e.g. all single-kind contexts are anonymous).

        For a single-kind context:

        If the context is not anonymous, this method will return the current
        context as is and unmodified.

        If the context is anonymous, this method will return an invalid context.
        """
        contexts = self.__multi if self.__multi is not None else [self]
        contexts = [c for c in contexts if not c.anonymous]

        return Context.create_multi(*contexts)

    def get(self, attribute: str) -> Any:
        """
        Looks up the value of any attribute of the context by name.

        For a single-kind context, the attribute name can be any custom attribute that was set
        by :func:`ldclient.ContextBuilder.set()`. It can also be one of the built-in ones
        like "kind", "key", or "name"; in such cases, it is equivalent to :attr:`kind`,
        :attr:`key`, or :attr:`name`.

        For a multi-context, the only supported attribute name is "kind". Use
        :func:`get_individual_context()` to get the context for a particular kind and then get
        its attributes.

        If the value is found, the return value is the attribute value. If there is no such
        attribute, the return value is None. An attribute that actually exists cannot have a
        value of None.

        Context has a ``__getitem__`` magic method equivalent to ``get``, so ``context['attr']``
        behaves the same as ``context.get('attr')``.

        :param attribute: the desired attribute name
        :return: the attribute value, or None if there is no such attribute

        :see: :func:`ldclient.ContextBuilder.set()`
        """
        if attribute == 'key':
            return self.__key
        if attribute == 'kind':
            return self.__kind
        if attribute == 'name':
            return self.__name
        if attribute == 'anonymous':
            return self.__anonymous
        if self.__attributes is None:
            return None
        return self.__attributes.get(attribute)

    @property
    def individual_context_count(self) -> int:
        """
        Returns the number of context kinds in this context.

        For a valid individual context, this returns 1. For a multi-context, it returns the number
        of context kinds. For an invalid context, it returns zero.

        :return: the number of context kinds

        :see: :func:`get_individual_context()`
        """
        if self.__error is not None:
            return 0
        if self.__multi is None:
            return 1
        return len(self.__multi)

    def get_individual_context(self, kind: Union[int, str]) -> Optional[Context]:
        """
        Returns the single-kind Context corresponding to one of the kinds in this context.

        The ``kind`` parameter can be either a number representing a zero-based index, or a string
        representing a context kind.

        If this method is called on a single-kind Context, then the only allowable value for
        ``kind`` is either zero or the same value as the Context's :attr:`kind`, and the return
        value on success is the same Context.

        If the method is called on a multi-context, and ``kind`` is a number, it must be a
        non-negative index that is less than the number of kinds (that is, less than the value
        of :attr:`individual_context_count`), and the return value on success is one of the
        individual Contexts within. Or, if ``kind`` is a string, it must match the context
        kind of one of the individual contexts.

        If there is no context corresponding to ``kind``, the method returns None.

        :param kind: the index or string value of a context kind
        :return: the context corresponding to that index or kind, or None

        :see: :attr:`individual_context_count`
        """
        if self.__error is not None:
            return None
        if isinstance(kind, str):
            if self.__multi is None:
                return self if kind == self.__kind else None
            for c in self.__multi:
                if c.kind == kind:
                    return c
            return None
        if self.__multi is None:
            return self if kind == 0 else None
        if kind < 0 or kind >= len(self.__multi):
            return None
        return self.__multi[kind]

    @property
    def custom_attributes(self) -> Iterable[str]:
        """
        Gets the names of all non-built-in attributes that have been set in this context.

        For a single-kind context, this includes all the names that were passed to
        :func:`ldclient.ContextBuilder.set()` as long as the values were not None (since a
        value of None in LaunchDarkly is equivalent to the attribute not being set).

        For a multi-context, there are no such names.

        :return: an iterable
        """
        return () if self.__attributes is None else self.__attributes

    @property
    def _attributes(self) -> Optional[dict[str, Any]]:
        # for internal use by ContextBuilder - we don't want to expose the original dict
        # since that would break immutability
        return self.__attributes

    @property
    def private_attributes(self) -> Iterable[str]:
        """
        Gets the list of all attribute references marked as private for this specific Context.

        This includes all attribute names/paths that were specified with
        :func:`ldclient.ContextBuilder.private()`.

        :return: an iterable
        """
        return () if self.__private is None else self.__private

    @property
    def _private_attributes(self) -> Optional[list[str]]:
        # for internal use by ContextBuilder - we don't want to expose the original list otherwise
        # since that would break immutability
        return self.__private

    @property
    def fully_qualified_key(self) -> str:
        """
        A string that describes the Context uniquely based on ``kind`` and ``key`` values.

        This value is used whenever LaunchDarkly needs a string identifier based on all of the
        :attr:`kind` and :attr:`key` values in the context. Applications typically do not need to use it.
        """
        return self.__full_key

    def to_dict(self) -> dict[str, Any]:
        """
        Returns a dictionary of properties corresponding to the JSON representation of the
        context (as an associative array), in the standard format used by LaunchDarkly SDKs.

        Use this method if you are passing context data to the front end for use with the
        LaunchDarkly JavaScript SDK.

        :return: a dictionary corresponding to the JSON representation
        """
        if not self.valid:
            return {}
        if self.__multi is not None:
            ret = {"kind": "multi"}  # type: dict[str, Any]
            for c in self.__multi:
                ret[c.kind] = c.__to_dict_single(False)
            return ret
        return self.__to_dict_single(True)

    def to_json_string(self) -> str:
        """
        Returns the JSON representation of the context as a string, in the standard format
        used by LaunchDarkly SDKs.

        This is equivalent to calling :func:`to_dict()` and then ``json.dumps()``.

        :return: the JSON representation as a string
        """
        return json.dumps(self.to_dict(), separators=(',', ':'))

    def __to_dict_single(self, with_kind: bool) -> dict[str, Any]:
        ret = {"key": self.__key}  # type: Dict[str, Any]
        if with_kind:
            ret["kind"] = self.__kind
        if self.__name is not None:
            ret["name"] = self.__name
        if self.__anonymous:
            ret["anonymous"] = True
        if self.__attributes is not None:
            for k, v in self.__attributes.items():
                ret[k] = v
        if self.__private is not None:
            ret["_meta"] = {"privateAttributes": self.__private}
        return ret

    @classmethod
    def __from_dict_single(self, props: dict, kind: Optional[str]) -> Context:
        b = ContextBuilder('')
        if kind is not None:
            b.kind(kind)
        for k, v in props.items():
            if k == '_meta':
                if v is None:
                    continue
                if not isinstance(v, dict):
                    return Context.__create_with_schema_type_error(k)
                p = v.get("privateAttributes")
                if p is not None:
                    if not isinstance(p, list):
                        return Context.__create_with_schema_type_error("privateAttributes")
                    for pa in p:
                        if not isinstance(pa, str):
                            return Context.__create_with_schema_type_error("privateAttributes")
                        b.private(pa)
            else:
                if not b.try_set(k, v):
                    return Context.__create_with_schema_type_error(k)
        return b.build()

    def __getitem__(self, attribute) -> Any:
        return self.get(attribute) if isinstance(attribute, str) else None

    def __repr__(self) -> str:
        """
        Returns a standard string representation of a context.

        For a valid Context, this is currently defined as being the same as the JSON representation,
        since that is the simplest way to represent all of the Context properties. However, application
        code should not rely on ``__repr__`` always being the same as the JSON representation. If you
        specifically want the latter, use :func:`to_json_string()`. For an invalid Context, ``__repr__``
        returns a description of why it is invalid.

        :return: a string representation
        """
        if not self.valid:
            return "[invalid context: %s]" % self.__error
        return self.to_json_string()

    def __eq__(self, other) -> bool:
        """
        Compares contexts for deep equality of their attributes.

        :return: true if the Contexts are equal
        """
        if not isinstance(other, Context):
            return False
        if (
            self.__kind != other.__kind
            or self.__key != other.__key
            or self.__name != other.__name
            or self.__anonymous != other.__anonymous
            or self.__attributes != other.__attributes
            or self.__private != other.__private
            or self.__error != other.__error
        ):
            return False
        # Note that it's OK to compare __attributes because Python does a deep-equality check for dicts,
        # and it's OK to compare __private_attributes because we have canonicalized them by sorting.
        if self.__multi is None:
            return True  # we already know the other context isn't a multi-context due to checking kind
        if other.__multi is None or len(other.__multi) != len(self.__multi):
            return False
        for i in range(len(self.__multi)):
            if other.__multi[i] != self.__multi[i]:
                return False
        return True

    def __ne__(self, other) -> bool:
        return not self.__eq__(other)

    def __make_invalid(self, error: str):
        self.__error = error
        self.__kind = ''
        self.__key = ''
        self.__name = None
        self.__anonymous = False
        self.__attributes = None
        self.__private = None
        self.__multi = None
        self.__full_key = ''

    @classmethod
    def __create_with_error(cls, error: str) -> Context:
        return Context('', '', None, False, None, None, None, False, error)

    @classmethod
    def __create_with_schema_type_error(cls, propname: str) -> Context:
        return Context.__create_with_error('invalid data type for "%s"' % propname)


class ContextBuilder:
    """
    A mutable object that uses the builder pattern to specify properties for :class:`ldclient.Context`.

    Use this type if you need to construct a context that has only a single kind. To define a
    multi-context, use :func:`ldclient.Context.create_multi()` or :func:`ldclient.Context.multi_builder()`.

    Obtain an instance of ContextBuilder by calling :func:`ldclient.Context.builder()`. Then, call
    setter methods such as :func:`name()` or :func:`set()` to specify any additional attributes. Then,
    call :func:`build()` to create the context. ContextBuilder setters return a reference to the same
    builder, so calls can be chained:
    ::

        context = Context.builder('user-key') \
            .name('my-name') \
            .set('country', 'us') \
            .build

    :param key: the context key
    """

    def __init__(self, key: str, copy_from: Optional[Context] = None):
        self.__key = key
        if copy_from is None:
            self.__kind = Context.DEFAULT_KIND
            self.__name = None  # type: Optional[str]
            self.__anonymous = False
            self.__attributes = None  # type: Optional[Dict[str, Any]]
            self.__private = None  # type: Optional[list[str]]
            self.__copy_on_write_attrs = False
            self.__copy_on_write_private = False
        else:
            self.__kind = copy_from.kind
            self.__name = copy_from.name
            self.__anonymous = copy_from.anonymous
            self.__attributes = copy_from._attributes
            self.__private = copy_from._private_attributes
            self.__copy_on_write_attrs = self.__attributes is not None
            self.__copy_on_write_private = self.__private is not None
        self.__allow_empty_key = False

    def build(self) -> Context:
        """
        Creates a Context from the current builder properties.

        The Context is immutable and will not be affected by any subsequent actions on the builder.

        It is possible to specify invalid attributes for a ContextBuilder, such as an empty key.
        Instead of throwing an exception, the ContextBuilder always returns an Context and you can
        check :attr:`ldclient.Context.valid` or :attr:`ldclient.Context.error` to see if it has
        an error. See :attr:`ldclient.Context.valid` for more information about invalid conditions.
        If you pass an invalid Context to an SDK method, the SDK will detect this and will log a
        description of the error.

        :return: a new :class:`ldclient.Context`
        """
        self.__copy_on_write_attrs = self.__attributes is not None
        self.__copy_on_write_private = self.__private is not None
        return Context(self.__kind, self.__key, self.__name, self.__anonymous, self.__attributes, self.__private, None, self.__allow_empty_key)

    def key(self, key: str) -> ContextBuilder:
        """
        Sets the context's key attribute.

        Every context has a key, which is always a string. It cannot be an empty string, but
        there are no other restrictions on its value.

        The key attribute can be referenced by flag rules, flag target lists, and segments.

        :param key: the context key
        :return: the builder
        """
        self.__key = key
        return self

    def kind(self, kind: str) -> ContextBuilder:
        """
        Sets the context's kind attribute.

        Every context has a kind. Setting it to an empty string or None is equivalent to
        :const:`ldclient.Context.DEFAULT_KIND` ("user"). This value is case-sensitive.

        The meaning of the context kind is completely up to the application. Validation rules are
        as follows:

        * It may only contain letters, numbers, and the characters ``.``, ``_``, and ``-``.
        * It cannot equal the literal string "kind".
        * For a single context, it cannot equal "multi".

        :param kind: the context kind
        :return: the builder
        """
        self.__kind = kind
        return self

    def name(self, name: Optional[str]) -> ContextBuilder:
        """
        Sets the context's name attribute.

        This attribute is optional. It has the following special rules:

        * Unlike most other attributes, it is always a string if it is specified.
        * The LaunchDarkly dashboard treats this attribute as the preferred display name for
          contexts.

        :param name: the context name (None to unset the attribute)
        :return: the builder
        """
        self.__name = name
        return self

    def anonymous(self, anonymous: bool) -> ContextBuilder:
        """
        Sets whether the context is only intended for flag evaluations and should not be
        indexed by LaunchDarkly.

        The default value is False. False means that this Context represents an entity
        such as a user that you want to be able to see on the LaunchDarkly dashboard.

        Setting ``anonymous`` to True excludes this context from the database that is
        used by the dashboard. It does not exclude it from analytics event data, so it is
        not the same as making attributes private; all non-private attributes will still be
        included in events and data export. There is no limitation on what other attributes
        may be included (so, for instance, ``anonymous`` does not mean there is no ``name``),
        and the context will still have whatever ``key`` you have given it.

        This value is also addressable in evaluations as the attribute name "anonymous". It
        is always treated as a boolean true or false in evaluations.

        :param anonymous: true if the context should be excluded from the LaunchDarkly database
        :return: the builder

        :see: :attr:`ldclient.Context.anonymous`
        """
        self.__anonymous = anonymous
        return self

    def set(self, attribute: str, value: Any) -> ContextBuilder:
        """
        Sets the value of any attribute for the context.

        This includes only attributes that are addressable in evaluations-- not metadata such
        as :func:`private()`. If ``attributeName`` is ``"private"``, you will be setting an attribute
        with that name which you can use in evaluations or to record data for your own purposes,
        but it will be unrelated to :func:`private()`.

        The allowable types for context attributes are equivalent to JSON types: boolean, number,
        string, array (list), or object (dictionary). For all attribute names that do not have
        special meaning to LaunchDarkly, you may use any of those types. Values of different JSON
        types are always treated as different values: for instance, the number 1 is not the same
        as the string "1".

        The following attribute names have special restrictions on their value types, and
        any value of an unsupported type will be ignored (leaving the attribute unchanged):

        * ``"kind"``, ``"key"``: Must be a string. See :func:`kind()` and :func:`key()`.
        * ``"name"``: Must be a string or None. See :func:`name()`.
        * ``"anonymous"``: Must be a boolean. See :func:`anonymous()`.

        The attribute name ``"_meta"`` is not allowed, because it has special meaning in the
        JSON schema for contexts; any attempt to set an attribute with this name has no
        effect.

        Values that are JSON arrays or objects have special behavior when referenced in
        flag/segment rules.

        A value of None is equivalent to removing any current non-default value of the
        attribute. Null/None is not a valid attribute value in the LaunchDarkly model; any
        expressions in feature flags that reference an attribute with a null value will
        behave as if the attribute did not exist.

        :param attribute: the attribute name to set
        :param value: the value to set
        :return: the builder
        """
        self.try_set(attribute, value)
        return self

    def try_set(self, attribute: str, value: Any) -> bool:
        """
        Same as :func:`set()`, but returns a boolean indicating whether the attribute was
        successfully set.

        :param attribute: the attribute name to set
        :param value: the value to set
        :return: True if successful; False if the name was invalid or the value was not an
          allowed type for that attribute
        """
        if attribute == '' or attribute == '_meta':
            return False
        if attribute == 'key':
            if isinstance(value, str):
                self.__key = value
                return True
            return False
        if attribute == 'kind':
            if isinstance(value, str):
                self.__kind = value
                return True
            return False
        if attribute == 'name':
            if value is None or isinstance(value, str):
                self.__name = value
                return True
            return False
        if attribute == 'anonymous':
            if isinstance(value, bool):
                self.__anonymous = value
                return True
            return False
        if self.__copy_on_write_attrs:
            self.__copy_on_write_attrs = False
            self.__attributes = self.__attributes and self.__attributes.copy()
        if self.__attributes is None:
            self.__attributes = {}
        if value is None:
            self.__attributes.pop(attribute, None)
        else:
            self.__attributes[attribute] = value
        return True

    def private(self, *attributes: str) -> ContextBuilder:
        """
        Designates any number of Context attributes, or properties within them, as private: that is,
        their values will not be sent to LaunchDarkly.

        Each parameter can be either a simple attribute name, or a slash-delimited path referring to
        a JSON object property within an attribute.

        :param attributes: attribute names or references to mark as private
        :return: the builder
        """
        if len(attributes) != 0:
            if self.__copy_on_write_private:
                self.__copy_on_write_private = False
                self.__private = self.__private and self.__private.copy()
            if self.__private is None:
                self.__private = []
            self.__private.extend(attributes)
        return self

    def _allow_empty_key(self, allow: bool):
        # This is used internally in Context.__from_dict_old_user to support old-style users with an
        # empty key, which was allowed in the user model.
        self.__allow_empty_key = allow


class ContextMultiBuilder:
    """
    A mutable object that uses the builder pattern to specify properties for a multi-context.

    Use this builder if you need to construct a :class:`ldclient.Context` that contains multiple contexts,
    each for a different context kind. To define a regular context for a single kind, use
    :func:`ldclient.Context.create()` or :func:`ldclient.Context.builder()`.

    Obtain an instance of ContextMultiBuilder by calling :func:`ldclient.Context.multi_builder()`;
    then, call :func:`add()` to specify the individual context for each kind. The method returns a
    reference to the same builder, so calls can be chained:
    ::

        context = Context.multi_builder() \
            .add(Context.new("my-user-key")) \
            .add(Context.new("my-org-key", "organization")) \
            .build
    """

    def __init__(self):
        self.__contexts = []  # type: list[Context]
        self.__copy_on_write = False

    def build(self) -> Context:
        """
        Creates a Context from the current builder properties.

        The Context is immutable and will not be affected by any subsequent actions on the builder.

        It is possible for a ContextMultiBuilder to represent an invalid state. Instead of throwing
        an exception, the ContextMultiBuilder always returns a Context, and you can check
        :attr:`ldclient.Context.valid` or :attr:`ldclient.Context.error` to see if it has an
        error. See :attr:`ldclient.Context.valid` for more information about invalid context
        conditions. If you pass an invalid context to an SDK method, the SDK will detect this and
        will log a description of the error.

        If only one context was added to the builder, this method returns that context rather
        than a multi-context.

        :return: a new Context
        """
        if len(self.__contexts) == 1:
            return self.__contexts[0]  # multi-context with only one context is the same as just that context
        self.__copy_on_write = True
        # Context constructor will handle validation
        return Context(None, '', None, False, None, None, self.__contexts)

    def add(self, context: Context) -> ContextMultiBuilder:
        """
        Adds an individual Context for a specific kind to the builer.

        It is invalid to add more than one Context for the same kind, or to add an LContext
        that is itself invalid. This error is detected when you call :func:`build()`.

        If the nested context is a multi-context, this is exactly equivalent to adding each of the
        individual contexts from it separately. For instance, in the following example, ``multi1`` and
        ``multi2`` end up being exactly the same:
        ::

            c1 = Context.new("key1", "kind1")
            c2 = Context.new("key2", "kind2")
            c3 = Context.new("key3", "kind3")

            multi1 = Context.multi_builder().add(c1).add(c2).add(c3).build()

            c1plus2 = Context.multi_builder.add(c1).add(c2).build()
            multi2 = Context.multi_builder().add(c1plus2).add(c3).build()

        :param context: the context to add
        :return: the builder
        """
        if context.multiple:
            for i in range(context.individual_context_count):
                c = context.get_individual_context(i)
                if c is not None:
                    self.add(c)
        else:
            if self.__copy_on_write:
                self.__copy_on_write = False
                self.__contexts = self.__contexts.copy()
            self.__contexts.append(context)
        return self


__all__ = ['Context', 'ContextBuilder', 'ContextMultiBuilder']
