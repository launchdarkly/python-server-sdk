PYTEST_FLAGS=-W error::SyntaxWarning

TEMP_TEST_OUTPUT=/tmp/contract-test-service.log

SPHINXOPTS    = -W --keep-going
SPHINXBUILD   = sphinx-build
SPHINXPROJ    = launchdarkly-server-sdk
SOURCEDIR     = docs
BUILDDIR      = $(SOURCEDIR)/build
# port 8000 and 9000 is already used in the CI environment because we're
# running a DynamoDB container and an SSE contract test
SSE_PORT=9000
PORT=10000

.PHONY: help
help: #! Show this help message
	@echo 'Usage: make [target] ... '
	@echo ''
	@echo 'Targets:'
	@grep -h -F '#!' $(MAKEFILE_LIST) | grep -v grep | sed 's/:.*#!/:/' | column -t -s":"

.PHONY: install
install:
	@poetry install --all-extras

#
# Quality control checks
#

.PHONY: test
test: #! Run unit tests
test: install
	@poetry run pytest $(PYTEST_FLAGS)

.PHONY: lint
lint: #! Run type analysis and linting checks
lint: install
	@poetry run mypy ldclient testing

#
# Documentation generation
#

.PHONY: docs
docs: #! Generate sphinx-based documentation
	@poetry install --with docs
	@cd docs
	@poetry run $(SPHINXBUILD) -M html "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

#
# Contract test service commands
#

.PHONY: install-contract-tests-deps
install-contract-tests-deps:
	poetry install --with contract-tests

.PHONY: start-contract-test-service
start-contract-test-service:
	@cd contract-tests && poetry run python service.py $(PORT)

.PHONY: start-contract-test-service-bg
start-contract-test-service-bg:
	@echo "Test service output will be captured in $(TEMP_TEST_OUTPUT)"
	@make start-contract-test-service >$(TEMP_TEST_OUTPUT) 2>&1 &

.PHONY: run-contract-tests
run-contract-tests:
	@curl -s https://raw.githubusercontent.com/launchdarkly/sdk-test-harness/v2/downloader/run.sh \
		| VERSION=v2 PARAMS="-url http://localhost:$(PORT) -debug -stop-service-at-end" sh

.PHONY: contract-tests
contract-tests: #! Run the contract test harness
contract-tests: install-contract-tests-deps start-contract-test-service-bg run-contract-tests

#
# SSE contract test service commands
#

.PHONY: install-sse-contract-tests-deps
install-sse-contract-tests-deps:
	poetry install --with contract-tests

.PHONY: start-sse-contract-test-service
start-sse-contract-test-service:
	@cd sse-contract-tests && poetry run python service.py $(SSE_PORT)

.PHONY: start-sse-contract-test-service-bg
start-sse-contract-test-service-bg:
	@echo "Test service output will be captured in $(TEMP_TEST_OUTPUT)"
	@make start-sse-contract-test-service >$(TEMP_TEST_OUTPUT) 2>&1 &

.PHONY: run-sse-contract-tests
run-sse-contract-tests:
	@curl -s https://raw.githubusercontent.com/launchdarkly/sse-contract-tests/v2.0.0/downloader/run.sh \
      | VERSION=v1 PARAMS="-url http://localhost:$(SSE_PORT) -debug -stop-service-at-end -skip reconnection" sh

.PHONY: sse-contract-tests
sse-contract-tests: #! Run the sse contract test harness
sse-contract-tests: install-sse-contract-tests-deps start-sse-contract-test-service-bg run-sse-contract-tests
