#!/usr/bin/env make -f

.PHONY: all executor scheduler clean check

all: scheduler executor

executor/dist/cook-executor:
	mkdir -p executor/dist
	docker build -t cook-executor-build -f executor/Dockerfile.build executor
	-docker rm cook-executor-build
	docker run --name cook-executor-build cook-executor-build
	docker cp cook-executor-build:/opt/cook/dist/cook-executor executor/dist/cook-executor

executor: executor/dist/cook-executor

scheduler/resources/public/cook-executor: executor
	mkdir -p scheduler/resources/public
	cp executor/dist/cook-executor scheduler/resources/public

scheduler: scheduler/resources/public/cook-executor
	docker build -t cook-scheduler scheduler

clean:
	-rm scheduler/resources/public/cook-executor
	-docker rmi --force cook-scheduler
	-rm executor/dist/cook-executor
	-docker rmi --force cook-executor-builder

check:
	@type docker >/dev/null 2>&1
	@type minimesos >/dev/null 2>&1
