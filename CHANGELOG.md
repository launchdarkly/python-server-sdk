# Change log

All notable changes to the LaunchDarkly Python SDK will be documented in this file. This project adheres to [Semantic Versioning](http://semver.org).

## [8.1.7] - 2023-10-05
### Changed:
- Loosened urllib3 requirement to <3

## [8.1.6] - 2023-09-06
### Changed:
- Recoverable errors are logged as a warning not an error. (Thanks, [fritzdj](https://github.com/launchdarkly/python-server-sdk/pull/219)!)

## [8.1.5] - 2023-08-15
### Changed:
- Loosened the requirements on the semver package. (Thanks, [hauntsaninja](https://github.com/launchdarkly/python-server-sdk/pull/216)!)

## [8.1.4] - 2023-06-01
### Fixed:
- Password will be redacted from redis URL prior to logging.

## [8.1.3] - 2023-05-03
### Fixed:
- Updated usage of `HTTPResponse.getheader` to remove deprecation warning from upstream `urllib3` package. (Thanks, [mnito](https://github.com/launchdarkly/python-server-sdk/pull/206)!)

## [8.1.2] - 2023-05-01
### Fixed:
- Pinned urllib3 dependency to <2. (Thanks, [prpnmac](https://github.com/launchdarkly/python-server-sdk/pull/202)!)

## [8.1.1] - 2023-02-10
### Fixed:
- Fixed indexing error raised by calling `all_flags_state` while using the `TestData` data source.

## [7.6.1] - 2023-02-07
### Fixed:
- Fixed indexing error raised by calling `all_flags_state` while using the `TestData` data source.

## [8.1.0] - 2023-01-31
### Added:
- Introduced support for an `application` config property which sets application metadata that may be used in LaunchDarkly analytics or other product features. This does not affect feature flag evaluations.

## [7.6.0] - 2023-01-31
### Added:
- Introduced support for an `application` config property which sets application metadata that may be used in LaunchDarkly analytics or other product features. This does not affect feature flag evaluations.

## [8.0.0] - 2022-12-30
The latest version of this SDK supports LaunchDarkly's new custom contexts feature. Contexts are an evolution of a previously-existing concept, "users." Contexts let you create targeting rules for feature flags based on a variety of different information, including attributes pertaining to users, organizations, devices, and more. You can even combine contexts to create "multi-contexts." 

For detailed information about this version, please refer to the list below. For information on how to upgrade from the previous version, please read the [migration guide](https://docs.launchdarkly.com/sdk/server-side/python/migration-7-to-8).

### Added:
- In `ldclient`, the `Context` type defines the new context model.
- For all SDK methods that took a user parameter in the form of a `dict`, you can now pass a `Context` instead. You can still pass a `dict` containing user properties, in which case the SDK will convert it to a `Context` transparently; however, `Context` is preferable if you value efficiency since there is some overhead to this conversion.
- The `TestData` flag builder methods have been extended to support now context-related options, such as matching a key for a specific context type other than "user".

### Changed _(breaking changes from 7.x)_:
- It was previously allowable to set a user key to an empty string. In the new context model, the key is not allowed to be empty. Trying to use an empty key will cause evaluations to fail and return the default value.
- There is no longer such a thing as a `secondary` meta-attribute that affects percentage rollouts. If you set an attribute with that name in a `Context`, it will simply be a custom attribute like any other.
- The `anonymous` attribute is now a simple boolean, with no distinction between a false state and a null/undefined state. Previously, a flag rule like `anonymous is false` would not match if the attribute was undefined, but now undefined is treated the same as false.

### Changed (requirements/dependencies/build):
- The minimum Python version is now 3.7.

### Changed (behavioral changes):
- The SDK can now evaluate segments that have rules referencing other segments.
- Analytics event data now uses a new JSON schema due to differences between the context model and the old user model.
- Several optimizations within the flag evaluation logic have improved the performance of evaluations. For instance, target lists are now stored internally as sets for faster matching.

### Removed:
- Removed all types, properties, and methods that were deprecated as of the most recent 5.x release.
- Removed the deprecated `ldclient.flag` module. This was previously an alternate way to import the `EvaluationDetail` type; now, you can only import that type from `ldclient.evaluation`.
- The `alias` method no longer exists because alias events are not needed in the new context model.
- The `inline_users_in_events` option no longer exists because it is not relevant in the new context model.

## [7.5.1] - 2022-09-29
### Added:
- Publishing this package now includes a pre-built wheel distribution in addition to the customary source distribution.

## [7.5.0] - 2022-07-01
### Added:
- A new `redis_opts` parameter is available when configuring a [Redis feature or Big Segment store](https://launchdarkly-python-sdk.readthedocs.io/en/latest/api-integrations.html#ldclient.integrations.Redis). This parameter will be passed through to the underlying redis driver, allowing for greater configurability. (Thanks, [danie1k](https://github.com/launchdarkly/python-server-sdk/pull/170)!)

### Fixed:
- Our previous attempt at adding mypy type checking support missed the inclusion of the required py.typed file. (Thanks, [anentropic](https://github.com/launchdarkly/python-server-sdk/pull/172)!)

## [7.4.2] - 2022-06-16
### Changed:
- Removed upper version restriction on expiringdict. This was originally necessary to allow compatibility with older Python versions which are no longer supported.

## [7.4.1] - 2022-04-22
### Added:
- Added py.typed file to indicate typing support. Thanks [@phillipuniverse](https://github.com/launchdarkly/python-server-sdk/pull/166)

### Fixed:
- Fixed invalid operator in key in TestData.
- Fixed bucketing logic to not treat boolean values as bucketable value types.

## [7.4.0] - 2022-02-16
### Added:
- `TestData`, in the new module `ldclient.integrations.test_data`, is a new way to inject feature flag data programmatically into the SDK for testing—either with fixed values for each flag, or with targets and/or rules that can return different values for different users. Unlike the file data source, this mechanism does not use any external resources, only the data that your test code has provided.

## [7.3.1] - 2022-02-14
### Added:
- CI builds now include a cross-platform test suite implemented in https://github.com/launchdarkly/sdk-test-harness. This covers many test cases that are also implemented in unit tests, but may be extended in the future to ensure consistent behavior across SDKs in other areas.

### Fixed:
- The SDK no longer uses the deprecated method `threading.Condition.notifyAll()`. (Thanks, [jdmoldenhauer](https://github.com/launchdarkly/python-server-sdk/pull/162)!)
- A rule clause that uses a date operator should be considered a non-match, rather than an error, if either value is `None`.
- A rule clause that uses a semver operator should be considered a non-match, rather than an error, if either value is not a string.
- Rules targeting the `secondary` attribute will now reference the correct value.
- The `identify` method should not emit an event if the user key is an empty string.
- Do not include `prereqOf` field in event data if it is null. This is done to save on event transfer bandwidth.
- Data from `all_flags_state` was always including the flag's version even when it was unnecessary.
- Any base URIs set in `Config` will work consistently whether they have trailing slashes or not.
- When using `all_flags_state` to produce bootstrap data for the JavaScript SDK, the Python SDK was not returning the correct metadata for evaluations that involved an experiment. As a result, the analytics events produced by the JavaScript SDK did not correctly reflect experimentation results.
- Data from `all_flags_state` was always including the flag's version even when it was unnecessary.

## [7.3.0] - 2021-12-10
### Added:
- The SDK now supports evaluation of Big Segments. See: https://docs.launchdarkly.com/home/users/big-segments

## [7.2.1] - 2021-12-03
### Changed:
- Added CI testing for Python 3.10.

### Fixed:
- In streaming mode, the SDK could sometimes fail to receive flag data from LaunchDarkly if the data contained characters that are not in the Basic Latin character set. The error was intermittent and would depend on unpredictable factors of speed and network behavior which could cause the first byte of a multi-byte UTF8 character to be processed before the rest of the bytes had arrived.
- Fixed some irregularities in the SSE parsing logic used for stream data. The SDK's CI tests now include a more thorough test suite for SSE behavior that is implemented in https://github.com/launchdarkly/sse-contract-tests, to ensure that it is consistent with other LaunchDarkly SDKs.

## [7.2.0] - 2021-06-17
### Added:
- The SDK now supports the ability to control the proportion of traffic allocation to an experiment. This works in conjunction with a new platform feature now available to early access customers.

## [7.1.0] - 2021-03-11
### Added:
- Added the `alias` method to `LDClient`. This can be used to associate two user objects for analytics purposes with an alias event.


## [7.0.2] - 2021-02-18
### Fixed:
- The SDK could fail to send debug events when event debugging was enabled on the LaunchDarkly dashboard, if the application server&#39;s time zone was not GMT.

## [7.0.1] - 2020-11-25
### Fixed:
- The logic for detecting uWSGI did not account for undocumented behavior in some environments where the `uwsgi` module is present in an incomplete state; this could cause an error on startup in such environments. Also, the log message about threading options related to uWSGI contained a broken link. (Thanks, [andrefreitas](https://github.com/launchdarkly/python-server-sdk/pull/148)!)

## [7.0.0] - 2020-10-28
This major release is for Python compatibility updates and removal of deprecated APIs. It introduces no new functionality except type hints.

### Added:
- Added [type hints](https://docs.python.org/3/library/typing.html) to all SDK methods. Python by itself does not enforce these, but commonly used development tools can provide static checking to trigger warnings or errors if the wrong type is used.

### Changed:
- Python 2.7, 3.3, and 3.4 are no longer supported. The minimum Python version is now 3.5.
- The first parameter to the `Config` constructor, `sdk_key`, is now required. Previously it was possible to omit the `sdk_key` from the `Config` and specify it separately when initializing the SDK. Now, it is always in the `Config`.

### Removed:
- Removed `ldclient.set_sdk_key()`. The correct way to do this now, if you are using the singleton client method `ldclient.get()`, is to call `ldclient.set_config()` with a `Config` object that contains the SDK key.
- Removed the optional SDK key parameter from the [`LDClient`](https://launchdarkly-python-sdk.readthedocs.io/en/latest/api-main.html#ldclient.client.LDClient) constructor. You must now provide a configuration parameter of type [`Config`](https://launchdarkly-python-sdk.readthedocs.io/en/latest/api-main.html#ldclient.config.Config), and set the SDK key within the `Config` constructor: `LDClient(Config(sdk_key = "my-sdk-key", [any other config options]))`. Previously, it was possible to specify the SDK key as a single string parameter and omit the `Config` object—`LDClient("my-sdk-key")`—although this would cause a deprecation warning to be logged; specifying both a key and a `Config` was always an error.
- Removed the individual HTTP-related parameters such as `connect_timeout` from the [`Config`](https://launchdarkly-python-sdk.readthedocs.io/en/latest/api-main.html#ldclient.config.Config) type. The correct way to set these now is with the [`HTTPConfig`](https://launchdarkly-python-sdk.readthedocs.io/en/latest/api-main.html#ldclient.config.HTTPConfig) sub-configuration object: `Config(sdk_key = "my-sdk-key", http = HTTPConfig(connect_timeout = 10))`.
- Removed all other types, parameters, and methods that were deprecated as of the last 6.x release.

## [6.13.3] - 2021-02-23
### Fixed:
- The SDK could fail to send debug events when event debugging was enabled on the LaunchDarkly dashboard, if the application server&#39;s time zone was not GMT.

## [6.13.2] - 2020-09-21
### Fixed:
- The SDK was not recognizing proxy authorization parameters included in a proxy URL (example: `http://username:password@proxyhost:port`). It will now use these parameters if present, regardless of whether you set the proxy URL programmatically or in an environment variable. (Thanks, [gangeli](https://github.com/launchdarkly/python-server-sdk/pull/145)!)

## [6.13.1] - 2020-07-13
### Fixed:
- A problem with the SDK&#39;s use of `urllib3.Retry` could prevent analytics event delivery from being retried after a network error or server error. ([#143](https://github.com/launchdarkly/python-server-sdk/issues/143))

## [6.13.0] - 2020-03-30
### Added:
- The new `Config` parameter `initial_reconnect_delay` allows customizing of the base retry delay for stream connections (that is, the delay for the first reconnection after a failure; subsequent retries use an exponential backoff).
- The new `Config` parameter `http` and the `HTTPConfig` class allow advanced configuration of the SDK&#39;s network behavior, such as specifying a custom certificate authority for connecting to a proxy/gateway that uses a self-signed certificate.

### Changed:
- The retry delay for stream connections has been changed as follows: it uses an exponential backoff no matter what type of error occurred (previously, some kinds of errors had a hard-coded 1-second delay), and each delay is reduced by a random jitter of 0-50% rather than 0-100%. Also, if a connection remains active for at least 60 seconds, the backoff is reset to the initial value. This makes the Python SDK&#39;s behavior consistent with other LaunchDarkly SDKs.

### Deprecated:
- The existing `Config` properties `connect_timeout`, `read_timeout`, and `verify_ssl` are now deprecated and superseded by the equivalent properties in `HTTPConfig`.

## [6.12.2] - 2020-03-19
### Fixed:
- Setting `verify_ssl` to `False` in the client configuration did not have the expected effect of completely turning off SSL/TLS verification, because it still left _certificate_ verification in effect, so it would allow a totally insecure connection but reject a secure connection whose certificate had an unknown CA. This has been changed so that it will turn off certificate verification as well. _This is not a recommended practice_ and a future version of the SDK will add a way to specify a custom certificate authority instead (to support, for instance, using the Relay Proxy with a self-signed certificate).

## [6.12.1] - 2020-02-12
### Fixed:
- When diagnostic events are enabled (as they are by default), the SDK was logging spurious warning messages saying "Unhandled exception in event processor. Diagnostic event was not sent. [&#39;DiagnosticEventSendTask&#39; object has no attribute &#39;_response_fn&#39;]". The events were still being sent; the misleading message has been removed.

## [6.12.0] - 2020-02-11
Note: if you are using the LaunchDarkly Relay Proxy to forward events, update the Relay to version 5.10.0 or later before updating to this Python SDK version.

### Added:
- The SDK now periodically sends diagnostic data to LaunchDarkly, describing the version and configuration of the SDK, the architecture and version of the runtime platform, and performance statistics. No credentials, hostnames, or other identifiable values are included. This behavior can be disabled with the `diagnostic_opt_out` option or configured with `diagnostic_recording_interval`.

### Fixed:
- The SDK now specifies a uniquely identifiable request header when sending events to LaunchDarkly to ensure that events are only processed once, even if the SDK sends them two times due to a failed initial attempt.

## [6.11.3] - 2019-12-30
### Fixed:
- In rare circumstances (depending on the exact data in the flag configuration, the flag's salt value, and the user properties), a percentage rollout could fail and return a default value, logging the error "variation/rollout object with no variation or rollout". This would happen if the user's hashed value fell exactly at the end of the last "bucket" (the last variation defined in the rollout). This has been fixed so that the user will get the last variation.

## [6.11.2] - 2019-12-09
### Fixed:
- Changed `Files.new_data_source()` to use `yaml.safe_load()` instead of `yaml.load()` for YAML/JSON test data parsing. This disables `pyyaml` extended syntax features that could allow arbitrary code execution. ([#136](https://github.com/launchdarkly/python-server-sdk/issues/136))

## [6.11.1] - 2019-11-21
### Fixed:
- Fixed an incompatibility with Python 3.3 due to an unpinned dependency on `expiringdict`.
- Fixed usages that caused a `SyntaxWarning` in Python 3.8. (Thanks, [bunchesofdonald](https://github.com/launchdarkly/python-server-sdk/pull/133)!)
- Updated CI scripts so a `SyntaxWarning` will always cause a build failure, and added a 3.8 build.

## [6.11.0] - 2019-10-31
### Added:
- The new `Config` parameter `http_proxy` allows you to specify a proxy server programmatically rather than by using environment variables. This may be helpful if you want the SDK to use a proxy, but do not want other Python code to use the proxy. (Thanks, [gangeli](https://github.com/launchdarkly/python-server-sdk/pull/130)!)

## [6.10.2] - 2019-10-30
### Fixed:
- Since version 6.1.0, the SDK was not respecting the standard `https_proxy` environment variable for specifying a proxy (because that variable is not used by `urllib3`). This has been fixed.
- In streaming mode, the SDK could fail to apply a feature flag update if it exceeded the LaunchDarkly service's maximum streaming message size; the service uses an alternate delivery mechanism in this case, which was broken in the SDK. This bug was also introduced in version 6.1.0.
- Fixed the generated documentation to exclude special members like `__dict__`.

## [6.10.1] - 2019-08-20
### Fixed:
- Fixed a bug in 6.10.0 that prevented analytics events from being generated for missing flags.

## [6.10.0] - 2019-08-20
### Added:
- Added support for upcoming LaunchDarkly experimentation features. See `LDClient.track()`.

## [6.9.4] - 2019-08-19
### Fixed:
- Under conditions where analytics events are being generated at an extremely high rate (for instance, if an application is evaluating a flag repeatedly in a tight loop on many threads), a thread could be blocked indefinitely within `variation` while waiting for the internal event processing logic to catch up with the backlog. The logic has been changed to drop events if necessary so threads will not be blocked (similar to how the SDK already drops events if the size of the event buffer is exceeded). If that happens, this warning message will be logged once: "Events are being produced faster than they can be processed; some events will be dropped". Under normal conditions this should never happen; this change is meant to avoid a concurrency bottleneck in applications that are already so busy that thread starvation is likely.

## [6.9.3] - 2019-06-11
### Fixed:
- Usages of `Logger.warn()` were causing deprecation warnings in some versions of Python. Changed these to `Logger.warning()`. ([#125](https://github.com/launchdarkly/python-server-sdk/issues/125))

## [6.9.2] - 2019-05-01
### Changed:
- Changed the artifact name from `ldclient-py` to `launchdarkly-server-sdk`
- Changed repository references to use the new URL

There are no other changes in this release. Substituting `ldclient-py` version 6.9.1 with `launchdarkly-server-sdk` version 6.9.2 will not affect functionality.

## [6.9.1] - 2019-04-26
### Fixed:
- The `set_sdk_key` function was comparing the existing SDK key (if any) to the new one by identity (`is`) rather than equality (`==`). In Python, two strings that have the same characters may or may not be the same string instance; in the case where they were not, `set_sdk_key` would inappropriately reinitialize the client even though the SDK key had not really changed. (Thanks, [jpgimenez](https://github.com/launchdarkly/python-server-sdk/pull/121)!)
- Running the SDK unit tests is now simpler in that the database integrations can be skipped. See `CONTRIBUTING.md`.

### Note on future releases:

The LaunchDarkly SDK repositories are being renamed for consistency. This repository is now `python-server-sdk` rather than `python-client`.

The package name will also change. In the 6.9.1 release, it is still `ldclient-py`; in all future releases, it will be `launchdarkly-server-sdk`. No further updates to the `ldclient-py` package will be published after this release.

## [6.9.0] - 2019-04-09
### Added:
- It is now possible to use the `with` statement on an LDClient object, so that `close()` will be called automatically when it goes out of scope.

### Fixed:
- Calling `close()` on the client would cause an error if the configuration included `use_ldd=True`. ([#118](https://github.com/launchdarkly/python-client/issues/118))

## [6.8.4] - 2019-03-29
### Fixed:
- Setting user attributes to non-string values when a string was expected would cause analytics events not to be processed. Also, in the case of the "secondary" attribute, this could cause evaluations to fail for a flag with a percentage rollout. The SDK will now convert attribute values to strings as needed. ([#115](https://github.com/launchdarkly/python-client/issues/115))
- If `track` or `identify` is called without a user, the SDK now logs a warning, and does not send an analytics event to LaunchDarkly (since it would not be processed without a user).

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
- It is now possible to use Consul as a persistent feature store, similar to the existing Redis and DynamoDB integrations. See `Consul` in `ldclient.integrations`, and the reference guide for ["Storing data"](https://docs.launchdarkly.com/sdk/features/storing-data#python).

## [6.7.0] - 2019-01-15
### Added:
- It is now possible to use DynamoDB as a persistent feature store, similar to the existing Redis integration. See `DynamoDB` in `ldclient.integrations`, and the reference guide to ["Storing data"](https://docs.launchdarkly.com/sdk/features/storing-data#python).
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
- To reduce the network bandwidth used for analytics events, feature request events are now sent as counters rather than individual events, and user details are now sent only at intervals rather than in each event. These behaviors can be modified through the LaunchDarkly UI and with the new configuration option `inline_users_in_events`.
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

