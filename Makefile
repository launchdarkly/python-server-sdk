PYTEST_FLAGS=-W error::SyntaxWarning

TEMP_TEST_OUTPUT=/tmp/contract-test-service.log

SPHINXOPTS    = -W --keep-going
SPHINXBUILD   = sphinx-build
SPHINXPROJ    = launchdarkly-server-sdk
SOURCEDIR     = docs
BUILDDIR      = $(SOURCEDIR)/build
# port 8000 is already used in the CI environment because we're running a DynamoDB container
PORT=9000

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
	@poetry run mypy ldclient

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
start-contract-test-service: install-contract-tests-deps
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
contract-tests: start-contract-test-service-bg run-contract-tests
