# pulse-ingestor — development commands.
#
# Works whether or not a Rust toolchain is installed on this machine. If `cargo`
# is on PATH it is used directly; otherwise every target runs in a throwaway
# rust:1-bookworm container against the same cached volumes. `make where` says
# which one you are getting.
#
# All of it targets the shared local stack in ../pulse-infra. Bring it up first:
#
#     make infra-up
#
# ADDRESSES DIFFER BY WHERE THE PROCESS RUNS, and this is the one thing that
# catches people out:
#
#   on the host          localhost:19092 / localhost:4443   (what .env holds)
#   inside pulse-infra   kafka-1:9092    / fake-gcs:4443
#
# The container targets below inject the in-network addresses as explicit -e
# overrides. dotenvy does not override variables that are already set, so those
# win over the .env that happens to be bind-mounted.

IMAGE      ?= pulse-ingestor:local
NETWORK    ?= pulse-infra
RUST_IMAGE ?= rust:1-bookworm
CARGO_VOL  ?= pulse-ingestor-cargo
TARGET_VOL ?= pulse-ingestor-target
RUSTUP_VOL ?= pulse-ingestor-rustup
INFRA      ?= ../pulse-infra
PROFILE    ?= core
NAME       ?=

HAVE_CARGO := $(shell command -v cargo 2>/dev/null)

# Shared docker-run preamble. The named volumes are why a rebuild takes seconds
# instead of the ~90s a cold dependency build costs; CARGO_TARGET_DIR points
# away from the bind mount so container artifacts never collide with the host.
DOCKER_BASE = docker run --rm \
	-v "$(CURDIR)":/src -w /src \
	-v $(CARGO_VOL):/usr/local/cargo/registry \
	-v $(RUSTUP_VOL):/usr/local/rustup \
	-v $(TARGET_VOL):/target -e CARGO_TARGET_DIR=/target

# In-network addresses, for anything running inside the stack's network.
NET_ENV = -e KAFKA_BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092 \
	-e STORAGE_EMULATOR_HOST=fake-gcs:4443

DOCKER_CARGO     = $(DOCKER_BASE) $(RUST_IMAGE) cargo
DOCKER_CARGO_NET = $(DOCKER_BASE) --network $(NETWORK) $(RUST_IMAGE) cargo
DOCKER_CARGO_RUN = $(DOCKER_BASE) --network $(NETWORK) $(NET_ENV) $(RUST_IMAGE) cargo

# The rust image ships no clippy or rustfmt. Installing them costs one download
# on first use and nothing afterwards, because RUSTUP_VOL keeps the toolchain.
ADD_COMPONENTS = rustup component add clippy rustfmt >/dev/null 2>&1;

ifeq ($(HAVE_CARGO),)
  CARGO     = $(DOCKER_CARGO)
  CARGO_NET = $(DOCKER_CARGO_NET)
  RUN_CMD   = $(DOCKER_CARGO_RUN) run
  WHERE     = container ($(RUST_IMAGE)) — no host toolchain found
else
  CARGO     = cargo
  CARGO_NET = cargo
  RUN_CMD   = cargo run
  WHERE     = host toolchain ($(HAVE_CARGO))
endif

.DEFAULT_GOAL := help
.PHONY: help where build run run-image image test test-one test-integration \
	lint fmt fmt-check check clean clean-cache shell env-check \
	infra-up infra-down infra-health

help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk -F':.*?## ' '{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "  toolchain: $(WHERE)"

where: ## Show which toolchain these targets will use
	@echo "$(WHERE)"

# ─── Build and run ───────────────────────────────────────────────────────────

build: ## Compile the binary and tests
	$(CARGO) build --all-targets

run: env-check ## Run the service against the local stack (host or container)
	$(RUN_CMD)

image: ## Build the production container image
	docker build -t $(IMAGE) .

run-image: env-check image ## Run the built image against the local stack
	docker run --rm --network $(NETWORK) --env-file .env $(NET_ENV) $(IMAGE)

# ─── Tests ───────────────────────────────────────────────────────────────────

test: ## Unit tests — no stack, no network required
	$(CARGO) test

test-one: ## Run a single test: make test-one NAME=a_partial_batch
	@test -n "$(NAME)" || { echo "usage: make test-one NAME=<substring>"; exit 1; }
	$(CARGO) test $(NAME)

test-integration: ## Integration tests — REQUIRES the stack (make infra-up)
	$(CARGO_NET) test --features integration --test integration

# ─── Quality ─────────────────────────────────────────────────────────────────

lint: ## clippy, warnings are errors
ifeq ($(HAVE_CARGO),)
	$(DOCKER_BASE) $(RUST_IMAGE) sh -c '$(ADD_COMPONENTS) cargo clippy --all-targets -- -D warnings'
else
	cargo clippy --all-targets -- -D warnings
endif

fmt: ## Format in place
ifeq ($(HAVE_CARGO),)
	$(DOCKER_BASE) $(RUST_IMAGE) sh -c '$(ADD_COMPONENTS) cargo fmt'
else
	cargo fmt
endif

fmt-check: ## Fail if anything is unformatted
ifeq ($(HAVE_CARGO),)
	$(DOCKER_BASE) $(RUST_IMAGE) sh -c '$(ADD_COMPONENTS) cargo fmt --check'
else
	cargo fmt --check
endif

check: fmt-check lint test ## Everything CI would run, minus the integration tests

# ─── Housekeeping ────────────────────────────────────────────────────────────

shell: ## Interactive shell in the build container
	$(DOCKER_BASE) --network $(NETWORK) -it $(RUST_IMAGE) bash

clean: ## Remove host build artifacts
	rm -rf target

clean-cache: ## Also drop the cached registry and target volumes (forces a cold build)
	-docker volume rm $(CARGO_VOL) $(TARGET_VOL) $(RUSTUP_VOL)

env-check:
	@test -f .env || { \
	  echo "No .env found. Create one from the template:"; \
	  echo "    cp .env.example .env"; \
	  exit 1; \
	}

# ─── Shared infrastructure (../pulse-infra) ──────────────────────────────────

infra-up: ## Bring up the shared stack (PROFILE=core|full|lite)
	$(MAKE) -C $(INFRA) up PROFILE=$(PROFILE)

infra-health: ## Report stack health
	$(MAKE) -C $(INFRA) health

infra-down: ## Stop the shared stack
	$(MAKE) -C $(INFRA) down
