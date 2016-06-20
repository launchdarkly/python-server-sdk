Contributing
------------

We encourage pull-requests and other contributions from the community. We've also published an [SDK contributor's guide](http://docs.launchdarkly.com/v1.0/docs/sdk-contributors-guide) that provides a detailed explanation of how our SDKs work.

Development information (for developing this module itself)
-----------------------------------------------------------

1. One-time setup:

       mkvirtualenv python-client

1. When working on the project be sure to activate the python-client virtualenv using the technique of your choosing.

1. Install requirements (run-time & test):

        pip install -r requirements.txt
        pip install -r test-requirements.txt
        pip install -r twisted-requirements.txt

1. Run tests:

        $ py.test testing
