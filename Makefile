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
	@uv sync --all-extras

#
# Quality control checks
#

.PHONY: test
test: #! Run unit tests
test: install
	@LD_SKIP_DATABASE_TESTS=1 uv run pytest $(PYTEST_FLAGS)

.PHONY: test-all
test-all: #! Run unit tests (including database integrations)
test-all: install
	@uv run pytest $(PYTEST_FLAGS)

.PHONY: lint
lint: #! Run type analysis and linting checks
lint: install
	@mkdir -p .mypy_cache
	@uv run mypy ldclient
	@uv run isort --check --atomic ldclient contract-tests
	@uv run pycodestyle ldclient contract-tests

#
# Documentation generation
#

.PHONY: docs
docs: #! Generate sphinx-based documentation
	@uv sync --group docs
	@cd docs
	@uv run $(SPHINXBUILD) -M html "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

#
# Contract test service commands
#

.PHONY: install-contract-tests-deps
install-contract-tests-deps:
	# --all-extras is required because persistence integrations (redis, consul, dynamodb)
	# are optional extras, not group deps. uv sync --group alone would strip them.
	# See https://github.com/astral-sh/uv/issues/7033 for a future fix.
	uv sync --all-extras --group contract-tests

.PHONY: start-contract-test-service
start-contract-test-service: install-contract-tests-deps
	@cd contract-tests && uv run python service.py $(PORT)

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
