.EXPORT_ALL_VARIABLES:

# DOCKER_IMAGE ?= eventstore/eventstore:23.10.0-bookworm-slim
#DOCKER_IMAGE ?= docker.eventstore.com/eventstore/eventstoredb-ee:24.10.0-x64-8.0-bookworm-slim
#DOCKER_IMAGE ?= docker.eventstore.com/kurrent-latest/kurrentdb:25.0.0-x64-8.0-bookworm-slim
DOCKER_IMAGE ?= kurrentplatform/kurrentdb:26.1.0

PYTHONUNBUFFERED=1

POETRY_VERSION=2.2.1
POETRY ?= poetry@$(POETRY_VERSION)

.PHONY: install-poetry
install-poetry:
	@pipx install --suffix="@$(POETRY_VERSION)" "poetry==$(POETRY_VERSION)"
	$(POETRY) --version

.PHONY: install
install:
	$(POETRY) sync --all-extras $(opts)

.PHONY: update
update: update-lock install

.PHONY: update-lock
update-lock:
	$(POETRY) update --lock -v


.PHONY: fmt
fmt: fmt-isort fmt-black

.PHONY: fmt-ruff
fmt-ruff:
	$(POETRY) run ruff check --fix .

.PHONY: fmt-black
fmt-black:
	$(POETRY) run black .

.PHONY: fmt-isort
fmt-isort:
	$(POETRY) run isort .


.PHONY: lint
lint: lint-black lint-ruff lint-isort lint-mypy lint-pyright

.PHONY: lint-black
lint-black:
	$(POETRY) run black --check --diff .

.PHONY: lint-ruff
lint-ruff:
	$(POETRY) run ruff check .

.PHONY: lint-isort
lint-isort:
	$(POETRY) run isort --check-only --diff .

.PHONY: lint-pyright
lint-pyright:
	$(POETRY) run pyright .

.PHONY: lint-mypy
lint-mypy:
	$(POETRY) run mypy


.PHONY: test
test:
	$(POETRY) run coverage run -m unittest discover . -v
	$(POETRY) run coverage report --fail-under=100 --show-missing

.PHONY: build
build:
	$(POETRY) build
# 	$(POETRY) build -f sdist    # build source distribution only

.PHONY: publish
publish:
	$(POETRY) publish

.PHONY: start-kurrentdb-insecure
start-kurrentdb-insecure:
	@docker run -d -i -t -p 2113:2113 \
    --env "KURRENTDB_ADVERTISE_HOST_TO_CLIENT_AS=localhost" \
    --env "KURRENTDB_ADVERTISE_NODE_PORT_TO_CLIENT_AS=2113" \
    --env "KURRENTDB_RUN_PROJECTIONS=All" \
    --env "KURRENTDB_START_STANDARD_PROJECTIONS=true" \
    --env "KURRENTDB_ENABLE_ATOM_PUB_OVER_HTTP=true" \
    --env "KURRENTDB_ALLOW_UNKNOWN_OPTIONS=true" \
    --env "KURRENTDB_TELEMETRY_OPTOUT=true" \
    --env "EVENTSTORE_ADVERTISE_HOST_TO_CLIENT_AS=localhost" \
    --env "EVENTSTORE_ADVERTISE_NODE_PORT_TO_CLIENT_AS=2113" \
    --env "EVENTSTORE_RUN_PROJECTIONS=All" \
    --env "EVENTSTORE_START_STANDARD_PROJECTIONS=true" \
    --env "EVENTSTORE_ENABLE_ATOM_PUB_OVER_HTTP=true" \
    --name my-kurrentdb-insecure \
    $(DOCKER_IMAGE) \
    --insecure

.PHONY: start-kurrentdb-secure
start-kurrentdb-secure:
	@docker run -d -i -t -p 2114:2113 \
    --env "HOME=/tmp" \
    --env "KURRENTDB_ADVERTISE_HOST_TO_CLIENT_AS=localhost" \
    --env "KURRENTDB_ADVERTISE_NODE_PORT_TO_CLIENT_AS=2114" \
    --env "KURRENTDB_RUN_PROJECTIONS=All" \
    --env "KURRENTDB_START_STANDARD_PROJECTIONS=true" \
    --env "KURRENTDB_ALLOW_UNKNOWN_OPTIONS=true" \
    --env "KURRENTDB_TELEMETRY_OPTOUT=true" \
    --env "EVENTSTORE_ADVERTISE_HOST_TO_CLIENT_AS=localhost" \
    --env "EVENTSTORE_ADVERTISE_NODE_PORT_TO_CLIENT_AS=2114" \
    --env "EVENTSTORE_RUN_PROJECTIONS=All" \
    --env "EVENTSTORE_START_STANDARD_PROJECTIONS=true" \
    --name my-kurrentdb-secure \
    $(DOCKER_IMAGE) \
    --dev


.PHONY: attach-kurrentdb-insecure
attach-kurrentdb-insecure:
	@docker exec -it my-kurrentdb-insecure /bin/bash

.PHONY: attach-kurrentdb-secure
attach-kurrentdb-secure:
	@docker exec -it my-kurrentdb-secure /bin/bash

.PHONY: stop-kurrentdb-insecure
stop-kurrentdb-insecure:
	@docker stop my-kurrentdb-insecure
	@docker rm my-kurrentdb-insecure

.PHONY: stop-kurrentdb-secure
stop-kurrentdb-secure:
	@docker stop my-kurrentdb-secure
	@docker rm my-kurrentdb-secure

.PHONY: start-kurrentdb
start-kurrentdb: start-kurrentdb-insecure start-kurrentdb-secure
	@echo "Waiting for containers to be healthy"
	@until docker ps | grep "my-kurrentdb" | grep -in "healthy" | wc -l | grep -in 2 > /dev/null; do printf "." && sleep 1; done; echo ""
	@docker ps
	@sleep 15


.PHONY: stop-kurrentdb
stop-kurrentdb: stop-kurrentdb-insecure stop-kurrentdb-secure
