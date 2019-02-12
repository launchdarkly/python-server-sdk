# Change log

All notable changes to the LaunchDarkly Python SDK will be documented in this file. This project adheres to [Semantic Versioning](http://semver.org).

## [6.8.3] - 2019-02-12

Note that starting with this release, generated API documentation is available online at [https://launchdarkly-python-sdk.readthedocs.io](https://launchdarkly-python-sdk.readthedocs.io). This is published automatically from the documentation comments in the code.

### Changed:
- The SDK no longer uses `jsonpickle`.
- The CI test suite for all supported Python versions, which previously only ran in Linux, has been extended to include Python 3.6 in Windows.

### Fixed:
- Corrected and expanded many documentation comments. All public API methods (not including internal implementation details) are now documented.

## [6.8.2] - 2019-01-31
### Fixed:
- Fixed a _different_ packaging error that was still present in the 6.8.1 release, which made the package installable but caused imports to fail. The 6.8.1 release has been pulled from PyPI. We apologize for these recent errors, which were not detected prior to release because our prerelease testing was using the source code directly rather than installing it with `pip`. Our CI tests have been updated and should prevent this in the future.


## [6.8.1] - 2019-01-31
### Fixed:
- Fixed a packaging error that made the 6.8.0 release not installable. There are no other changes. The 6.8.0 release has been pulled from PyPI.


## [6.8.0] - 2019-01-31
### Added:
- It is now possible to use Consul as a persistent feature store, similar to the existing Redis and DynamoDB integrations. See `Consul` in `ldclient.integrations`, and the reference guide for ["Using a persistent feature store"](https://docs.launchdarkly.com/v2.0/docs/using-a-persistent-feature-store).

## [6.7.0] - 2019-01-15
### Added:
- It is now possible to use DynamoDB as a persistent feature store, similar to the existing Redis integration. See `DynamoDB` in `ldclient.integrations`, and the reference guide to ["Using a persistent feature store"](https://docs.launchdarkly.com/v2.0/docs/using-a-persistent-feature-store).
- The new class `CacheConfig` (in `ldclient.feature_store`) encapsulates all the parameters that control local caching in database feature stores. This takes the place of the `expiration` and `capacity` parameters that are in the deprecated `RedisFeatureStore` constructor; it can be used with DynamoDB and any other database integrations in the future, and if more caching options are added to `CacheConfig` they will be automatically supported in all of the feature stores.

### Deprecated:
- The `RedisFeatureStore` constructor in `ldclient.redis_feature_store`. The recommended way to create a Redis feature store now is to use `Redis.new_feature_store` in `ldclient.integrations`.

## [6.6.0] - 2018-11-14
### Added:
- It is now possible to inject feature flags into the client from local JSON or YAML files, replacing the normal LaunchDarkly connection. This would typically be for testing purposes. See `file_data_source.py`.

## [6.5.0] - 2018-10-17
### Added:
- The `all_flags_state` method now accepts a new option, `details_only_for_tracked_flags`, which reduces the size of the JSON representation of the flag state by omitting some metadata. Specifically, it omits any data that is normally used for generating detailed evaluation events if a flag does not have event tracking or debugging turned on.

### Changed:
- The SDK previously contained a copy of code from the `expiringdict` package. This has been changed to use the current version of that package from PyPi.

### Fixed:
- JSON data from `all_flags_state` is now slightly smaller even if you do not use the new option described above, because it omits the flag property for event tracking unless that property is true.

## [6.4.2] - 2018-09-21
### Fixed:
- In polling mode, if the client received an HTTP error from LaunchDarkly, it stopped polling. This has been fixed so it only stops polling if the error is 401 (indicating an invalid SDK key).
- When using a Redis feature store, if the `hgetall` method returned an invalid result, `all_flags` and `all_flags_state` would throw an exception. Instead, `all_flags` will now return an empty dict, and `all_flags_state` will return a state object with no flags and `valid==False`. (Thanks, [thieman](https://github.com/launchdarkly/python-client/pull/99)!)

## [6.4.1] - 2018-09-06
### Fixed:
- In Python 3, if the Redis feature store encountered a Redis exception, it would crash on trying to log the `message` property of the exception, which does not exist in Python 3. This has been fixed. (Thanks, [mattbriancon](https://github.com/launchdarkly/python-client/pull/96)!)

## [6.4.0] - 2018-08-29
### Added:
- The new `LDClient` method `variation_detail` allows you to evaluate a feature flag (using the same parameters as you would for `variation`) and receive more information about how the value was calculated. This information is returned in an `EvaluationDetail` object, which contains both the result value and a "reason" object which will tell you, for instance, if the user was individually targeted for the flag or was matched by one of the flag's rules, or if the flag returned the default value due to an error.

### Fixed:
- When evaluating a prerequisite feature flag, the analytics event for the evaluation did not include the result value if the prerequisite flag was off.

## [6.3.0] - 2018-08-27
### Added:
- The new `LDClient` method `all_flags_state()` should be used instead of `all_flags()` if you are passing flag data to the front end for use with the JavaScript SDK. It preserves some flag metadata that the front end requires in order to send analytics events correctly. Versions 2.5.0 and above of the JavaScript SDK are able to use this metadata, but the output of `all_flags_state()` will still work with older versions.
- The `all_flags_state()` method also allows you to select only client-side-enabled flags to pass to the front end, by using the option `client_side_only=True`.

### Deprecated:
- `LDClient.all_flags()`

## [6.2.0] - 2018-08-03
### Changed:
- In streaming mode, each connection failure or unsuccessful reconnection attempt logs a message at `ERROR` level. Previously, this message included the amount of time before the next retry; since that interval is different for each attempt, that meant the `ERROR`-level messages were all unique, which could cause problems for monitors. This has been changed so the `ERROR`-level message is always the same, and is followed by an `INFO`-level message about the time delay. (Note that in order to suppress the default message, the LaunchDarkly client modifies the logger used by the `backoff` package; if you are using `backoff` for some other purpose and _do_ want to see the default message, set `logging.getLogger('backoff').propagate` to `True`.) ([#88](https://github.com/launchdarkly/python-client/issues/88))

## [6.1.1] - 2018-06-19

### Fixed:
- Removed an unused dependency on the `CacheControl` package.

## [6.1.0] - 2018-06-18

### Changed:
- The client now uses `urllib3` for HTTP requests, rather than the `requests` package. This change was made because `requests` has a dependency on an LGPL-licensed package, and some of our customers cannot use LGPL code. The networking behavior of the client should be unchanged.
- The client now treats most HTTP 4xx errors as unrecoverable: that is, after receiving such an error, it will not make any more HTTP requests for the lifetime of the client instance, in effect taking the client offline. This is because such errors indicate either a configuration problem (invalid SDK key) or a bug in the client, which will not resolve without a restart or an upgrade. This does not apply if the error is 400, 408, 429, or any 5xx error.
- During initialization, if the client receives any of the unrecoverable errors described above, `ldclient.get()` will return immediately; previously it would continue waiting until a timeout. The `is_initialized()` method will return false in this case.

## [6.0.4] - 2018-06-12

### Fixed:
- Fixed a bug introduced in v6.0.3 that caused the user cache for analytics events to never be cleared, also causing an `AttributeError` to appear in the log.

## [6.0.3] - 2018-05-30

### Removed:
- Removed a dependency on the `pylru` package, because it uses a GPL license.

### Fixed:
- Fixed a bug that, in Python 3.x, could cause a timer thread to keep running after the client has been shut down. This bug also caused the message "TypeError: Event object is not callable" to be logged.
- Fixed the `Config` initializer to create a new instance of `InMemoryFeatureStore` if you omit the `feature_store` argument. Previously, all `Config` instances that were created with default parameters would share the same feature store instance.
- Clarified HTTP proxy setup instructions in the readme.

## [6.0.2] - 2018-05-25

### Fixed:
- Fixed a bug that caused an error message to be logged (`KeyError: 'default'`) when evaluating a prerequisite flag (and that also prevented an analytics event from being sent for that flag).
- When running in uWSGI, the client will no longer log an error message if the `enableThreads` option is absent, as long as the `threads` option has been set to a number greater than 1. ([#84](https://github.com/launchdarkly/python-client/issues/84))


## [6.0.1] - 2018-05-25

_This release was broken and has been removed._

## [6.0.0] - 2018-05-10

### Changed:
- To reduce the network bandwidth used for analytics events, feature request events are now sent as counters rather than individual events, and user details are now sent only at intervals rather than in each event. These behaviors can be modified through the LaunchDarkly UI and with the new configuration option `inline_users_in_events`. For more details, see [Analytics Data Stream Reference](https://docs.launchdarkly.com/v2.0/docs/analytics-data-stream-reference).
- The analytics event processor now flushes events at a configurable interval defaulting to 5 seconds, like the other SDKs (previously it flushed if no events had been posted for 5 seconds, or if events exceeded a configurable number). This interval is set by the new `Config` property `flush_interval`.

### Removed:
- Python 2.6 is no longer supported.
- Removed the `Config` property `events_upload_max_batch_size`, which is no longer relevant in the new event flushing logic (see above).


## [5.0.4] - 2018-04-16
## Fixed
- It was not possible to install the SDK with `pip` 10.0.0. This should work now (thanks, [@theholy7](https://github.com/launchdarkly/python-client/pull/82)!) with the latest `pip` as well as previous versions.


## [5.0.3] - 2018-04-10
### Fixed
- Fixed a bug that, in Python 3.x, caused an error when using an integer user attribute to compute a rollout.
- Fixed a bug that, in Python 3.x, made the `all_flags` method return a dictionary with byte-string keys instead of string keys when using the Redis feature store.


## [5.0.2] - 2018-03-27
### Fixed
- In the Redis feature store, fixed a synchronization problem that could cause a feature flag update to be missed if several of them happened in rapid succession.


## [5.0.1] - 2018-02-22
### Added
- Support for a new LaunchDarkly feature: reusable user segments.

### Changed
- The `FeatureStore` interface has been changed to support user segment data as well as feature flags. Existing code that uses `InMemoryFeatureStore` or `RedisFeatureStore` should work as before, but custom feature store implementations will need to be updated.

### Removed
- Twisted is no longer supported.


## [5.0.0] - 2018-02-21

_This release was broken and has been removed._

## [4.3.0] - 2018-02-07

### Changed
- Percentage rollouts can now reference an attribute with an integer value, not just string attributes.

### Fixed
- Fixed a bug that caused unusually slow initialization times when there are large numbers of flags.
- Fixed reporting of events for prerequisite checks.



## [4.2.1] - 2018-01-31

### Changed
- Reduced WARN-level logging for a feature flag not being found to INFO level.

### Fixed
- Fixed a bug where a previously deleted feature flag might be considered still available.
- The private attributes feature added in v4.1.0 was not available in Twisted mode; now it is.


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

