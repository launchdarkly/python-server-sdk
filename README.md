LaunchDarkly SDK for Python
===========================

[![Circle CI](https://img.shields.io/circleci/project/launchdarkly/python-client.png)](https://circleci.com/gh/launchdarkly/python-client)
[![PyPI](https://img.shields.io/pypi/v/ldclient-py.svg?maxAge=2592000)](https://pypi.python.org/pypi/ldclient-py)
[![PyPI](https://img.shields.io/pypi/dm/ldclient-py.svg?maxAge=2592000)](https://pypi.python.org/pypi/ldclient-py)
[![Twitter Follow](https://img.shields.io/twitter/follow/launchdarkly.svg?style=social&label=Follow&maxAge=2592000)](https://twitter.com/intent/follow?screen_name=launchdarkly)

Quick setup
-----------

1. Install the Python SDK with `pip`

        pip install ldclient-py

2. Configure the library with your api key:

        import ldclient
        ldclient.api_key = "your api key"

3. Get the client:

        client = ldclient.get()

Your first feature flag
-----------------------

1. Create a new feature flag on your [dashboard](https://app.launchdarkly.com)
2. In your application code, use the feature's key to check wthether the flag is on for each user:

        if client.toggle("your.flag.key", {"key": "user@test.com"}, False):
            # application code to show the feature
        else:
            # the code to run if the feature is off

Learn more
-----------

Check out our [documentation](http://docs.launchdarkly.com) for in-depth instructions on configuring and using LaunchDarkly. You can also head straight to the [complete reference guide for this SDK](http://docs.launchdarkly.com/v1.0/docs/python-sdk-reference).

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
    * [JavaScript] (http://docs.launchdarkly.com/docs/js-sdk-reference "LaunchDarkly JavaScript SDK")
    * [PHP] (http://docs.launchdarkly.com/docs/php-sdk-reference "LaunchDarkly PHP SDK")
    * [Python] (http://docs.launchdarkly.com/docs/python-sdk-reference "LaunchDarkly Python SDK")
    * [Go] (http://docs.launchdarkly.com/docs/go-sdk-reference "LaunchDarkly Go SDK")
    * [Node.JS] (http://docs.launchdarkly.com/docs/node-sdk-reference "LaunchDarkly Node SDK")
    * [.NET] (http://docs.launchdarkly.com/docs/dotnet-sdk-reference "LaunchDarkly .Net SDK")
    * [Ruby] (http://docs.launchdarkly.com/docs/ruby-sdk-reference "LaunchDarkly Ruby SDK")
    * [Python Twisted] (http://docs.launchdarkly.com/docs/python-twisted "LaunchDarkly Python Twisted SDK")
* Explore LaunchDarkly
    * [launchdarkly.com] (https://launchdarkly.com/ "LaunchDarkly Main Website") for more information
    * [docs.launchdarkly.com] (http://docs.launchdarkly.com/  "LaunchDarkly Documentation") for our documentation and SDKs
    * [apidocs.launchdarkly.com] (http://apidocs.launchdarkly.com/  "LaunchDarkly API Documentation") for our API documentation
    * [blog.launchdarkly.com] (http://blog.launchdarkly.com/  "LaunchDarkly Blog Documentation") for the latest product updates


