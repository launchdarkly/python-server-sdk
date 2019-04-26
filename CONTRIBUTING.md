# Contributing to the LaunchDarkly Server-side SDK for Python

LaunchDarkly has published an [SDK contributor's guide](https://docs.launchdarkly.com/docs/sdk-contributors-guide) that provides a detailed explanation of how our SDKs work. See below for additional information on how to contribute to this SDK.

## Submitting bug reports and feature requests
 
The LaunchDarkly SDK team monitors the issue tracker associated with the `launchdarkly/python-server-sdk` SDK repository. Bug reports and feature requests specific to this SDK should be filed in this issue tracker. The SDK team will respond to all newly filed issues within two business days.

## Submitting pull requests
 
We encourage pull requests and other contributions from the community. Before submitting pull requests, ensure that all temporary or unintended code is removed. Don't worry about adding reviewers to the pull request; the LaunchDarkly SDK team will add themselves. The SDK team will acknowledge all pull requests within two business days.

## Build instructions

### Setup

It's advisable to use `virtualenv` to create a development environment within the project directory:

```
mkvirtualenv python-client
source ./python-client/bin/activate
```

To install the runtime and test requirements:

```
pip install -r requirements.txt
pip install -r test-requirements.txt
```

The additional requirements files `consul-requirements.txt`, `dynamodb-requirements.txt`, `redis-requirements.txt`, and `test-filesource-optional-requirements.txt` can also be installed if you need to test the corresponding features.

### Testing

To run all unit tests:

```
pytest
```

There are also integration tests that can be run against the LaunchDarkly service. To enable them, set the environment variable `LD_SDK_KEY` to a valid production SDK Key.

### Portability

Most portability issues are addressed by using the `six` package. We are avoiding the use of `__future__` imports, since they can easily be omitted by mistake causing code in one file to behave differently from another; instead, whenever possible, use an explicit approach that makes it clear what the desired behavior is in all Python versions (e.g. if you want to do floor division, use `//`; if you want to divide as floats, explicitly cast to floats).

It is preferable to run tests against all supported minor versions of Python (as described in `README.md` under Requirements), or at least the lowest and highest versions, prior to submitting a pull request. However, LaunchDarkly's CI tests will run automatically against all supported versions.
