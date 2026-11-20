IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
PLATFORM ?= linux/amd64
DEV   ?= dev
ANSWER ?= dev-answer
export IMAGE CPUS MEM PLATFORM

.PHONY: build fixtures run-dev check-dev

build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

fixtures: build
	python3 scripts/run_fixtures.py fixtures -- scripts/docker_run.sh

run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out

check-dev:
	python3 scripts/compare.py out $(ANSWER)
