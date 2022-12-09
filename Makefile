
PYTEST_FLAGS=-W error::SyntaxWarning

test:
	LD_SKIP_DATABASE_TESTS=1 pytest $(PYTEST_FLAGS)

test-all:
	pytest $(PYTEST_FLAGS)

lint:
	mypy --install-types --non-interactive --config-file mypy.ini ldclient testing

docs:
	cd docs && make html

.PHONY: test test-all lint docs


TEMP_TEST_OUTPUT=/tmp/contract-test-service.log

# TEST_HARNESS_PARAMS can be set to add -skip parameters for any contract tests that cannot yet pass
# Explanation of current skips:
# - We're preparing to migrate the SDK to U2C behavior, but so far we're still using the non-U2C contract
#   tests (v1).
# - The non-U2C tests include alias events, which we have removed, so those tests are disabled.
# - Same for inline users in events.
# - Some custom event tests are disabled because in the v1 test suite, those require inline users.
TEST_HARNESS_PARAMS := $(TEST_HARNESS_PARAMS) \
	-skip 'events/alias' \
	-skip 'events/user properties/inlineUsers=true' \
    -skip 'events/custom events/data and metricValue' \
    -skip 'events/custom events/basic properties/inline user'

# port 8000 and 9000 is already used in the CI environment because we're
# running a DynamoDB container and an SSE contract test
PORT=10000

build-contract-tests:
	@cd contract-tests && pip install -r requirements.txt

start-contract-test-service:
	@cd contract-tests && python service.py $(PORT)

start-contract-test-service-bg:
	@echo "Test service output will be captured in $(TEMP_TEST_OUTPUT)"
	@make start-contract-test-service >$(TEMP_TEST_OUTPUT) 2>&1 &

run-contract-tests:
	curl -s https://raw.githubusercontent.com/launchdarkly/sdk-test-harness/main/downloader/run.sh \
      | VERSION=v1 PARAMS="-url http://localhost:$(PORT) -debug -stop-service-at-end $(TEST_HARNESS_PARAMS)" sh

contract-tests: build-contract-tests start-contract-test-service-bg run-contract-tests

.PHONY: build-contract-tests start-contract-test-service run-contract-tests contract-tests
