# Contributing to the LaunchDarkly Server-side SDK for Python

LaunchDarkly has published an [SDK contributor's guide](https://docs.launchdarkly.com/sdk/concepts/contributors-guide) that provides a detailed explanation of how our SDKs work. See below for additional information on how to contribute to this SDK.

## Submitting bug reports and feature requests
 
The LaunchDarkly SDK team monitors the [issue tracker](https://github.com/launchdarkly/python-server-sdk/issues) in the SDK repository. Bug reports and feature requests specific to this SDK should be filed in this issue tracker. The SDK team will respond to all newly filed issues within two business days.

## Submitting pull requests
 
We encourage pull requests and other contributions from the community. Before submitting pull requests, ensure that all temporary or unintended code is removed. Don't worry about adding reviewers to the pull request; the LaunchDarkly SDK team will add themselves. The SDK team will acknowledge all pull requests within two business days.

## Build instructions

### Setup

This project is built using [poetry](https://python-poetry.org/). To learn more about the basics of working with this tool, read [Poetry's basic usage guide](https://python-poetry.org/docs/basic-usage/).

To begin development, active the poetry shell and ensure your dependencies are installed.

```
poetry shell
poetry install
```

This library defines several extra dependencies to optionally enhance the SDK's capabilities. Use the following commands to install one or more of the available extras.

```
poetry install --extras "redis consul dynamodb test-filesource"
poetry install -E redis -E consul -E dynamodb -E test-filesource
poetry install --all-extras
```

### Testing

To run all unit tests except for the database integrations:

```shell
make test
```

To run all unit tests including the database integrations (this requires you to have instances of Consul, DynamoDB, and Redis running locally):

```shell
make test-all
```

There are also integration tests that can be run against the LaunchDarkly service. To enable them, set the environment variable `LD_SDK_KEY` to a valid production SDK Key.

It is preferable to run tests against all supported minor versions of Python (as described in `README.md` under Requirements), or at least the lowest and highest versions, prior to submitting a pull request. However, LaunchDarkly's CI tests will run automatically against all supported versions.

### Building documentation

See "Documenting types and methods" below. To build the documentation locally, so you can see the effects of any changes before a release:

```shell
make docs
```

The output will appear in `docs/build/html`. Its formatting will be somewhat different since it does not have the same stylesheets used on readthedocs.io.

### Running the linter

The `mypy` tool is used in CI to verify type hints and warn of potential code problems. To run it locally:

```shell
make lint
```

## Code organization

The SDK's module structure is as follows:

* `ldclient`: This module exports the most commonly used classes and methods in the SDK, such as `LDClient`. The implementations may live in other modules, but applications should not need to import a more specific module such as `ldclient.client` to get those symbols.
* `ldclient.integrations`: This module contains entry points for optional features that are related to how the SDK communicates with other systems, such as `Redis`.
* `ldclient.interfaces`: This namespace contains types that do not do anything by themselves, but may need to be referenced if you are using optional features or implementing a custom component.

A special case is the module `ldclient.impl`, and any modules within it. Everything under `impl` is considered a private implementation detail: all files there are excluded from the generated documentation, and are considered subject to change at any time and not supported for direct use by application developers. Alternately, class names can be prefixed with an underscore to be "private by convention"; that will at least prevent them from being included in wildcard imports like `from ldclient import *`, but it is still preferable to avoid a proliferation of implementation-only modules within the main `ldclient` module, since developers may wrongly decide to reference such modules in imports.

So, if there is a class whose existence is entirely an implementation detail, it should be in `impl`. Similarly, classes that are _not_ in `impl` must not expose any public members (i.e. symbols that do not have an underscore prefix) that are not meant to be part of the supported public API. This is important because of our guarantee of backward compatibility for all public APIs within a major version: we want to be able to change our implementation details to suit the needs of the code, without worrying about breaking a customer's code. Due to how the language works, we can't actually prevent an application developer from referencing those classes in their code, but this convention makes it clear that such use is discouraged and unsupported.

### Type hints

Python does not require the use of type hints, but they can be extremely helpful for spotting mistakes and for improving the IDE experience, so we should always use them in the SDK. Every method in the public API is expected to have type hints for all non-`self` parameters, and for its return value if any.

It's also desirable to use type hints for private attributes, to catch possible mistakes in their use. Until all versions of Python that we support allow the PEP 526 syntax for doing this, we must do it via a comment in the format that `mypy` understands, for instance:

```python
    self._some_attribute = None  # type: Optional[int]
```

## Documenting types and methods

All classes and public methods outside of `ldclient.impl` should have docstrings in Sphinx format. These are used to build the documentation that is published on [readthedocs.io](https://launchdarkly-python-sdk.readthedocs.io/). See the [Sphinx documentation](https://www.sphinx-doc.org/en/master/) for details of the docstring format.

Please try to make the style and terminology in documentation comments consistent with other documentation comments in the SDK. Also, if a class or method is being added that has an equivalent in other SDKs, and if we have described it in a consistent away in those other SDKs, please reuse the text whenever possible (with adjustments for anything language-specific) rather than writing new text.
