# Change log

All notable changes to the LaunchDarkly Python SDK will be documented in this file. This project adheres to [Semantic Versioning](http://semver.org).

## [4.2.0] - 2018-01-12
## Changed
- Will use feature store if already initialized even if connection to service could not be established.  This is useful when flags have been initialized in redis.

## [4.1.0] - 2017-12-21

### Added
- Allow user to stop user attributes from being sent in analytics events back to LaunchDarkly.  Set `private_attribute_names` on each
  request and/or on `Config` to a list of strings matching the names of the attributes you wish to exclude.  Set
  `all_attributes_private` on the `Config` object to hide all attributes. 

### Changed
- Stop reattempting connections when receiving a 401 (unauthorized) response from LaunchDarkly.  This should only be caused by invalid SDK key so retrying is pointless.

### Deprecated 
- `events_enabled` is deprecated and `send_events` should be used instead.  `events_enabled` may be removed in a future minor revision.


## [4.0.6] - 2017-06-09
### Changed
- Improved error handling when processing stream events
- Replaced 3rd party rfc3339 library for license compliance
- No longer caching `get_one()` responses


## [4.0.5] - 2017-04-25
### Fixed
- [#70](https://github.com/launchdarkly/python-client/issues/70) Regex `matches` targeting rules now include the user if
a match is found anywhere in the attribute.  Before fixing this bug, the beginning of the attribute needed to match the pattern.
### Changed
- [#43](https://github.com/launchdarkly/python-client/issues/43) Started publishing code coverage metrics to Code Climate. 
Bear in mind that the Code Climate coverage report only shows the unit test coverage, while the bulk of our SDK test coverage comes
from a [separate integration test suite](https://github.com/launchdarkly/python-client#testing).

## [4.0.4] - 2017-04-18
### Fixed
- [#65](https://github.com/launchdarkly/python-client/issues/65) Ensure that no warning is logged about a missing SDK key when the `ldclient` package is imported.

## [4.0.3] - 2017-03-14
### Changed
- Fixed missing python2.6-requirements.txt in manifest

## [4.0.2] - 2017-03-13
### Added
- Support for Python 2.6.

## [4.0.1] - 2017-01-10
### Changed
- RedisFeatureStore now returns default when Redis errors occur
- Better detection of stream connection issues.

## [4.0.0] - 2016-11-18
### Changed
- Changing the config and SDK key is now supported after initialization. The client will be restarted with the new configuration
- Breaking api change: `ldclient.sdk_key = <KEY>` replaced with: `ldclient.set_sdk_key('<KEY>')`
- Breaking api change: `ldclient.config = config` replaced with: `ldclient.set_config(config)`
- No longer depend on sseclient library, instead include our own sse client

## [3.0.3] - 2016-11-03
### Changed
- Add backoff when retrying stream connection.
- More correct initialized state.

## [3.0.2] - 2016-10-26
### Changed
- Better error handling when sending events.

## [3.0.1] - 2016-10-21
### Changed
- Now using jsonpickle to serialize analytics events. Addresses https://github.com/launchdarkly/python-client/issues/57
- Better handling of indirect/put and indirect/patch messages in streaming connection.

## [3.0.0] - 2016-08-22
### Added
- Twisted support for LDD mode only.

### Changed
- FeatureStore interface get() and all() methods now take an additional callback parameter.

## [2.0.0] - 2016-08-10
### Added
- Support for multivariate feature flags. `variation` replaces `toggle` and can return a string, number, dict, or boolean value depending on how the flag is defined.
- New `all_flags` method returns all flag values for a specified user.
- New `secure_mode_hash` function computes a hash suitable for the new LaunchDarkly [JavaScript client's secure mode feature](https://github.com/launchdarkly/js-client#secure-mode).

### Deprecated
- The `toggle` call has been deprecated in favor of `variation`.
 
### Removed
- Twisted support has temporarily been removed.

