LaunchDarkly SDK for Python
===========================

![Circle CI](https://circleci.com/gh/launchdarkly/python-client.png)

Quick setup
-----------

1. Install the Python SDK with `pip`

        pip install ldclient-py

2. Create a new LDClient with your API key:

        client = LDClient("your_api_key")

Your first feature flag
-----------------------

1. Create a new feature flag on your [dashboard](https://app.launchdarkly.com)
2. In your application code, use the feature's key to check wthether the flag is on for each user:

        if client.toggle("your.flag.key", {"key": "user@test.com"}, False):
            # application code to show the feature
        else:
            # the code to run if the feature is off

Development information (for developing this module itself)
-----------------------------------------------------------

1. Install requirements (run-time & test):

        pip install -r requirements.txt
        pip install -r test-requirements.txt

2. Run tests:

        $ py.test


Learn more
-----------

Check out our [documentation](http://docs.launchdarkly.com) for in-depth instructions on configuring and using LaunchDarkly. You can also head straight to the [complete reference guide for this SDK](http://docs.launchdarkly.com/v1.0/docs/python-sdk-reference).

Contributing
------------

We encourage pull-requests and other contributions from the community. We've also published an [SDK contributor's guide](http://docs.launchdarkly.com/v1.0/docs/sdk-contributors-guide) that provides a detailed explanation of how our SDKs work.
