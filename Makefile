# Everything runs in a container: no tooling is installed on the host.
#
#   make test       run the test suite
#   make lint       ruff + mypy
#   make check      lint then test (what "done" means in AGENTS.md)
#   make lock       regenerate requirements*.txt from requirements*.in
#   make build      build the deployment image
#   make up/down    run the collector via compose (compose.yaml)
#   make logs       follow the collector's logs
#   make doctor     check config, paths, permissions and connectivity
#   make shell      a shell in the dev image
#
# Deployment to Unraid with Dockge is covered in DEPLOY.md; it uses the same
# compose.yaml, so what you run here is what runs there.

IMAGE_DEV := tick-collector-dev
IMAGE_APP := tick-collector
BASE      := python:3.12-slim
# Mount the source read-write-free; tests write only to /tmp inside the container.
RUN_DEV   := docker run --rm -e COLLECTOR_DATA_ROOT=/tmp/collector-data \
             -v "$(CURDIR)/src:/app/src:ro" -v "$(CURDIR)/tests:/app/tests:ro" \
             -v "$(CURDIR)/config:/app/config:ro" -w /app $(IMAGE_DEV)

.PHONY: dev-image test lint fmt typecheck check build up down logs doctor config shell lock clean

dev-image:
	docker build --target dev -t $(IMAGE_DEV) .

test: dev-image
	$(RUN_DEV) python -m pytest

cov: dev-image
	$(RUN_DEV) python -m pytest --cov=collector --cov-report=term-missing

lint: dev-image
	$(RUN_DEV) python -m ruff check src tests
	$(RUN_DEV) python -m ruff format --check src tests

fmt: dev-image
	$(RUN_DEV) python -m ruff check --fix src tests
	$(RUN_DEV) python -m ruff format src tests

typecheck: dev-image
	$(RUN_DEV) python -m mypy

check: lint typecheck test

build:
	docker build --target app -t $(IMAGE_APP) .

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f collector

doctor:
	docker compose exec collector python -m collector doctor

# Validate the deployment stack without starting anything -- the quickest check
# that compose.yaml and .env agree.
config:
	docker compose config

shell: dev-image
	docker run --rm -it -e COLLECTOR_DATA_ROOT=/tmp/collector-data \
		-v "$(CURDIR)/src:/app/src:ro" -v "$(CURDIR)/tests:/app/tests:ro" \
		-w /app $(IMAGE_DEV) bash

# Pinned requirements are generated in the image's own base so the resolution
# matches what actually gets installed.
lock:
	docker run --rm -v "$(CURDIR):/w" -w /w $(BASE) sh -c '\
		pip install -q --no-cache-dir pip-tools && \
		pip-compile --quiet --strip-extras -o requirements.txt requirements.in && \
		pip-compile --quiet --strip-extras -o requirements-dev.txt requirements-dev.in'

clean:
	docker rmi -f $(IMAGE_DEV) $(IMAGE_APP) 2>/dev/null || true
