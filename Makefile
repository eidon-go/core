.DEFAULT_GOAL := help

# Every module in the repository, and the published ones (everything except
# internal/**, i.e. the integration-test module).
MODULES   := $(shell scripts/modules.sh)
PUBLISHED := $(shell scripts/modules.sh --published)

# Keep in sync with the govulncheck step of .github/workflows/ci.yml.
GOVULNCHECK_VERSION := v1.10.0

# Runs a command in every directory of a module list:
#   $(call each,<modules>,<command>)
define each
	@set -e; for m in $(1); do echo "==> $$m"; (cd $$m && $(2)); done
endef

.PHONY: help
help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: modules
modules: ## List the modules of the repository
	@printf '%s\n' $(MODULES)

.PHONY: work
work: ## Generate a local go.work with every module (not committed)
	@rm -f go.work go.work.sum
	@go work init $(MODULES)
	@echo "go.work: $(MODULES)"

.PHONY: build
build: ## Build every published module
	$(call each,$(PUBLISHED),go build ./...)

.PHONY: test
test: ## Run unit tests of every published module with the race detector
	$(call each,$(PUBLISHED),go test -race -count=1 -timeout=5m -short ./...)

.PHONY: lint
lint: ## Run golangci-lint in every module
	$(call each,$(MODULES),golangci-lint run ./...)

.PHONY: format
format: ## Format code in every module with golangci-lint
	$(call each,$(MODULES),golangci-lint fmt)

.PHONY: tidy
tidy: ## Run go mod tidy in every module
	$(call each,$(MODULES),go mod tidy)

.PHONY: tidy-check
tidy-check: ## Fail if go mod tidy would change any module
	$(call each,$(MODULES),go mod tidy -diff)

.PHONY: vuln
vuln: ## Run govulncheck in every published module
	$(call each,$(PUBLISHED),go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...)

.PHONY: test-integration
test-integration: ## Run integration tests (requires Docker)
	@if [ -f internal/integration/go.mod ]; then \
		cd internal/integration && go test -race -count=1 -timeout=15m -v ./...; \
	else \
		echo "no integration module yet"; \
	fi

.PHONY: test-all
test-all: test test-integration ## Run all tests (unit + integration)
