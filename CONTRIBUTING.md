Contributing
------------

We encourage pull-requests and other contributions from the community. We've also published an [SDK contributor's guide](http://docs.launchdarkly.com/docs/sdk-contributors-guide) that provides a detailed explanation of how our SDKs work.

Development information (for developing this module itself)
-----------------------------------------------------------

1. One-time setup:

        mkvirtualenv python-client

1. When working on the project be sure to activate the python-client virtualenv using the technique of your choosing.

1. Install requirements (run-time & test):

        pip install -r requirements.txt
        pip install -r test-requirements.txt

1. When running unit tests, in order for `test_feature_store.py` to run, you'll need all of the supported databases (Redis, Consul, DynamoDB) running locally on their default ports.

1. If you want integration tests to run, set the ```LD_SDK_KEY``` environment variable to a valid production SDK Key.

1. ```$ py.test testing```

1. All code must be compatible with all supported Python versions as described in README. Most portability issues are addressed by using the `six` package. We are avoiding the use of `__future__` imports, since they can easily be omitted by mistake causing code in one file to behave differently from another; instead, whenever possible, use an explicit approach that makes it clear what the desired behavior is in all Python versions (e.g. if you want to do floor division, use `//`; if you want to divide as floats, explicitly cast to floats).

Developing with different Python versions
-----------------------------------------

Example for switching to Python 3:

```virtualenv -p `which python3` ~/.virtualenvs/python-client```