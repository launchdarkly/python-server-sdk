LaunchDarkly SDK for Python
===========================

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

        if client.get_flag("your.flag.key", {"key": "user@test.com"}, False):
            # application code to show the feature
        else:
            # the code to run if the feature is off