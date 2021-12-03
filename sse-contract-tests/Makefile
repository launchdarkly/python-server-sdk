
TEMP_TEST_OUTPUT=/tmp/sse-contract-test-service.log

# port 8000 is already used in the CI environment because we're running a DynamoDB container
PORT=9000

# we're skipping the "reconnection" test group because the simplified SSE client we're currently using
# does not do automatic retrying of connections - that is done at a higher level in the SDK
EXTRA_TEST_PARAMS=-skip reconnection

build-test-service:
	@pip install -r requirements.txt

start-test-service:
	@python service.py $(PORT)

start-test-service-bg:
	@echo "Test service output will be captured in $(TEMP_TEST_OUTPUT)"
	@make start-contract-test-service >$(TEMP_TEST_OUTPUT) 2>&1 &

run-contract-tests:
	@curl -s https://raw.githubusercontent.com/launchdarkly/sse-contract-tests/master/downloader/run.sh \
      | VERSION=v1 PARAMS="-url http://localhost:$(PORT) -debug -stop-service-at-end $(EXTRA_TEST_PARAMS)" sh

contract-tests: build-test-service start-test-service-bg run-contract-tests

.PHONY: build-test-service start-test-service start-test-service-bg run-contract-tests contract-tests
