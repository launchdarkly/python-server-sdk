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

1. Run tests: You'll need redis running locally on its default port of 6379.
1. If you want integration tests to run, set the ```LD_SDK_KEY``` environment variable to a valid production SDK Key.
1. ```$ py.test testing```

Developing with different python versions
-----------------------------------------

Example for switching to python 3:

```virtualenv -p `which python3` ~/.virtualenvs/python-client```