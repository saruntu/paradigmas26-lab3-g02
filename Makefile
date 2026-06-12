JAVA_HOME := /usr/lib/jvm/java-17-openjdk-amd64
PATH := $(JAVA_HOME)/bin:$(PATH)

SBT_OPTS := --add-exports=java.base/sun.nio.ch=ALL-UNNAMED
TEST_SBT_OPTS := --sun-misc-unsafe-memory-access=allow --enable-native-access=ALL-UNNAMED

SBT := JAVA_HOME=$(JAVA_HOME) PATH="$(PATH)" SBT_OPTS="$(SBT_OPTS)" sbt
SBT_TEST := JAVA_HOME=$(JAVA_HOME) PATH="$(PATH)" SBT_OPTS="$(TEST_SBT_OPTS)" sbt

SUBS ?= data/local_subscriptions.json
ENTITIES ?= data/valid_entities
TOPK ?=

RUN_ARGS = --subscription-file $(SUBS) --entities-dir $(ENTITIES)

ifneq ($(TOPK),)
RUN_ARGS += --top-k $(TOPK)
endif

.PHONY: run test clean compile

run:
	$(SBT) "run $(RUN_ARGS)"

run-top2:
	$(SBT) "run $(RUN_ARGS) --top-k 2"

test:
	bash tests.sh

compile:
	$(SBT) compile

clean:
	$(SBT) clean