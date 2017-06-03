LaunchDarkly SDK for Python
===========================

[![Circle CI](https://img.shields.io/circleci/project/launchdarkly/python-client.png)](https://circleci.com/gh/launchdarkly/python-client)
[![Code Climate](https://codeclimate.com/github/launchdarkly/python-client/badges/gpa.svg)](https://codeclimate.com/github/launchdarkly/python-client)

[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Flaunchdarkly%2Fpython-client.svg?type=shield)](https://app.fossa.io/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Flaunchdarkly%2Fpython-client?ref=badge_shield)

[![PyPI](https://img.shields.io/pypi/v/ldclient-py.svg?maxAge=2592000)](https://pypi.python.org/pypi/ldclient-py)
[![PyPI](https://img.shields.io/pypi/pyversions/ldclient-py.svg)](https://pypi.python.org/pypi/ldclient-py)

[![Twitter Follow](https://img.shields.io/twitter/follow/launchdarkly.svg?style=social&label=Follow&maxAge=2592000)](https://twitter.com/intent/follow?screen_name=launchdarkly)

Quick setup
-----------

1. Install the Python SDK with `pip`

        pip install ldclient-py

2. Configure the library with your sdk key:

        import ldclient

3. Get the client:

        ldclient.set_sdk_key("your sdk key")
        client = ldclient.get()

Your first feature flag
-----------------------

1. Create a new feature flag on your [dashboard](https://app.launchdarkly.com)
2. In your application code, use the feature's key to check whether the flag is on for each user:

        if client.variation("your.flag.key", {"key": "user@test.com"}, False):
            # application code to show the feature
        else:
            # the code to run if the feature is off

Python 2.6
----------
Python 2.6 is supported for polling mode only and requires an extra dependency. Here's how to set it up:

1. Use the `python2.6` extra  in your requirements.txt:
    `ldclient-py[python2.6]`

1. Due to Python 2.6's lack of SNI support, LaunchDarkly's streaming flag updates are not available. Set the `stream=False` option in the client config to disable it. You'll still receive flag updates, but via a polling mechanism with efficient caching. Here's an example:
	`config = ldclient.Config(stream=False, sdk_key="SDK_KEY")`


Twisted
-------
Twisted is supported for LDD mode only. To run in Twisted/LDD mode, 

1. Use this dependency:

	```
	ldclient-py[twisted]>=3.0.1
	```
2. Configure the client:

	```
	feature_store = TwistedRedisFeatureStore(url='YOUR_REDIS_URL', redis_prefix="ldd-restwrapper", expiration=0)
	ldclient.config.feature_store = feature_store
	
	ldclient.config = ldclient.Config(
	    use_ldd=use_ldd,
	    event_consumer_class=TwistedEventConsumer,
	)
	ldclient.sdk_key = 'YOUR_SDK_KEY'
	```
3. Get the client:

	```client = ldclient.get()```

Learn more
-----------

Check out our [documentation](http://docs.launchdarkly.com) for in-depth instructions on configuring and using LaunchDarkly. You can also head straight to the [complete reference guide for this SDK](http://docs.launchdarkly.com/docs/python-sdk-reference).

Testing
-------

We run integration tests for all our SDKs using a centralized test harness. This approach gives us the ability to test for consistency across SDKs, as well as test networking behavior in a long-running application. These tests cover each method in the SDK, and verify that event sending, flag evaluation, stream reconnection, and other aspects of the SDK all behave correctly.

[![Test Coverage](https://codeclimate.com/github/launchdarkly/python-client/badges/coverage.svg)](https://codeclimate.com/github/launchdarkly/python-client/coverage) The Code Climate coverage does not include the coverage provided by this integration test harness.

Contributing
------------

See [CONTRIBUTING](CONTRIBUTING.md) for more information.

About LaunchDarkly
-----------

* LaunchDarkly is a continuous delivery platform that provides feature flags as a service and allows developers to iterate quickly and safely. We allow you to easily flag your features and manage them from the LaunchDarkly dashboard.  With LaunchDarkly, you can:
    * Roll out a new feature to a subset of your users (like a group of users who opt-in to a beta tester group), gathering feedback and bug reports from real-world use cases.
    * Gradually roll out a feature to an increasing percentage of users, and track the effect that the feature has on key metrics (for instance, how likely is a user to complete a purchase if they have feature A versus feature B?).
    * Turn off a feature that you realize is causing performance problems in production, without needing to re-deploy, or even restart the application with a changed configuration file.
    * Grant access to certain features based on user attributes, like payment plan (eg: users on the ‘gold’ plan get access to more features than users in the ‘silver’ plan). Disable parts of your application to facilitate maintenance, without taking everything offline.
* LaunchDarkly provides feature flag SDKs for
    * [Java](http://docs.launchdarkly.com/docs/java-sdk-reference "Java SDK")
    * [JavaScript](http://docs.launchdarkly.com/docs/js-sdk-reference "LaunchDarkly JavaScript SDK")
    * [PHP](http://docs.launchdarkly.com/docs/php-sdk-reference "LaunchDarkly PHP SDK")
    * [Python](http://docs.launchdarkly.com/docs/python-sdk-reference "LaunchDarkly Python SDK")
    * [Python Twisted](http://docs.launchdarkly.com/docs/python-twisted-sdk-reference "LaunchDarkly Python Twisted SDK")
    * [Go](http://docs.launchdarkly.com/docs/go-sdk-reference "LaunchDarkly Go SDK")
    * [Node.JS](http://docs.launchdarkly.com/docs/node-sdk-reference "LaunchDarkly Node SDK")
    * [.NET](http://docs.launchdarkly.com/docs/dotnet-sdk-reference "LaunchDarkly .Net SDK")
    * [Ruby](http://docs.launchdarkly.com/docs/ruby-sdk-reference "LaunchDarkly Ruby SDK")
    * [iOS](http://docs.launchdarkly.com/docs/ios-sdk-reference "LaunchDarkly iOS SDK")
    * [Android](http://docs.launchdarkly.com/docs/android-sdk-reference "LaunchDarkly Android SDK")
* Explore LaunchDarkly
    * [launchdarkly.com](http://www.launchdarkly.com/ "LaunchDarkly Main Website") for more information
    * [docs.launchdarkly.com](http://docs.launchdarkly.com/  "LaunchDarkly Documentation") for our documentation and SDKs
    * [apidocs.launchdarkly.com](http://apidocs.launchdarkly.com/  "LaunchDarkly API Documentation") for our API documentation
    * [blog.launchdarkly.com](http://blog.launchdarkly.com/  "LaunchDarkly Blog Documentation") for the latest product updates
    * [Feature Flagging Guide](https://github.com/launchdarkly/featureflags/  "Feature Flagging Guide") for best practices and strategies
