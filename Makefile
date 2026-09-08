# Development entry points. `make` with no target lists them.
#
# This repository is a library, so there is nothing to `make run`. What there is instead is
# a suite that cannot mean anything without a database, and a mutation gate that has to be
# runnable by hand -- a gate nobody can run is a gate nobody trusts.
#
# Every target that needs configuration reads .env and nothing here is shell-specific: the
# same commands work from cmd.exe, PowerShell, and a POSIX shell, because recipes run under
# cmd regardless of the shell that invoked make.

SHELL := cmd.exe
.SHELLFLAGS := /c

# -include, not include: fmt, vet, build and test-unit must work in a fresh clone with no
# .env, and in CI, where the environment comes from the workflow rather than from a file.
-include .env
export

.DEFAULT_GOAL := help
.PHONY: help env db test test-unit test-integration test-ci mutate fmt vet build arch \
        tidy gates coverage clean

help:
	@echo Targets:
	@echo   make env               copy .env.example to .env (does not overwrite)
	@echo   make db                create the throwaway test database .env names
	@echo   make test              everything, with -race
	@echo   make test-unit         no database needed
	@echo   make test-integration  requires .env and a running PostgreSQL
	@echo   make test-ci           against a CI-shaped owner, not the local one
	@echo   make mutate            restore the dead_letter timestamp defect on purpose;
	@echo                          the two temporal gates MUST go red, then it restores
	@echo   make gates             everything CI runs: fmt vet build arch tidy test
	@echo   make coverage          coverage over the library packages
	@echo   make clean             drop the build and test caches

# Not a copy that overwrites: .env holds working local credentials, and clobbering it from
# a template is the kind of loss noticed one debugging hour later.
env:
	@if exist .env (echo .env already exists -- leaving it alone) else (copy .env.example .env >nul && echo Created .env from .env.example)

# ---------------------------------------------------------------------------
# The database
# ---------------------------------------------------------------------------
#
# Every psql call puts its options BEFORE the connection string and passes it with -d. The
# Windows psql stops parsing options at the first positional argument, so
# `psql "$DSN" -c "..."` warns that -c was ignored, reads an empty stdin, and exits 0 --
# a target that silently does nothing while reporting success.
#
# Note the local server here is PostgreSQL 15 while CI pins 17 by digest. Nothing in this
# repository is known to depend on the difference, but a green local run is therefore not
# the same evidence as a green CI run.

TEST_DATABASE ?= platform_test

db:
	@if "$(ADMIN_DSN)"=="" (echo No ADMIN_DSN. Run: make env && exit 1)
	@psql -v ON_ERROR_STOP=1 -q -c "SELECT 'creating' WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname='$(TEST_DATABASE)')" -d "$(ADMIN_DSN)" >nul
	@psql -v ON_ERROR_STOP=1 -q -c "CREATE DATABASE $(TEST_DATABASE)" -d "$(ADMIN_DSN)" 2>nul && echo Created $(TEST_DATABASE). || echo $(TEST_DATABASE) already exists -- the suite drops its schema on every run anyway.

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

# -p 1 because the integration packages share one database and each drops and rebuilds the
# platform schema in it. Run in parallel they interfere, and the failures look like locking
# bugs rather than like a test-harness problem.
test:
	go test -race -p 1 -count=1 ./...

test-unit:
	go test -race -short -count=1 ./...

test-integration:
	@if not exist .env (echo No .env yet. Run: make env && exit 1)
	set REQUIRE_INTEGRATION=1&& go test -race -p 1 -count=1 ./...

# CI owns its database as `platform`, deliberately not as the local owner. organization-control
# had a suite that passed locally and failed in CI purely because a test hardcoded the owner's
# name, and nothing local could catch it. This target reproduces that shape.
CI_DATABASE ?= platform_ci
CI_OWNER ?= platform
CI_OWNER_PASSWORD ?= platform
CI_DSN ?= postgres://$(CI_OWNER):$(CI_OWNER_PASSWORD)@localhost:5432/$(CI_DATABASE)?sslmode=disable

test-ci:
	@if "$(ADMIN_DSN)"=="" (echo No ADMIN_DSN. Run: make env && exit 1)
	@psql -v ON_ERROR_STOP=1 -q -c "DO $$$$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='$(CI_OWNER)') THEN CREATE ROLE $(CI_OWNER) LOGIN SUPERUSER PASSWORD '$(CI_OWNER_PASSWORD)'; ELSE ALTER ROLE $(CI_OWNER) LOGIN SUPERUSER PASSWORD '$(CI_OWNER_PASSWORD)'; END IF; END $$$$;" -d "$(ADMIN_DSN)"
	@psql -v ON_ERROR_STOP=1 -q -c "DROP DATABASE IF EXISTS $(CI_DATABASE);" -c "CREATE DATABASE $(CI_DATABASE) OWNER $(CI_OWNER);" -d "$(ADMIN_DSN)"
	@set "TEST_DATABASE_URL=$(CI_DSN)"&& set "REQUIRE_INTEGRATION=1"&& go test -race -p 1 -count=1 ./...

# ---------------------------------------------------------------------------
# The mutation gate, runnable by hand
# ---------------------------------------------------------------------------
#
# 0003 moved dead_lettered_at's default from now() -- the dispatcher transaction's start,
# which precedes every publish attempt in the batch -- to statement_timestamp(), the row's
# own transition. Outside this repository that value is read as evidence that a projection
# snapshot taken after it saw the failed event's effect, so an understated value lets a
# revoked membership keep being served.
#
# This target restores the defect and requires the suite to notice. It is a target rather
# than a CI-only step because the alternative is a claim: a gate whose red has never been
# seen is indistinguishable from a gate that cannot go red.
#
# The substitution is verified before anything is written. A mutation check that finds
# nothing to mutate REPORTS SUCCESS -- foundation-reference's gate went green that way for
# a while after a column was renamed and the pattern here was not, so the not-found branch
# fails loudly instead.

MIGRATION = migrations\platform\0003_dead_letter_temporal_boundary.sql
TEMPORAL_TESTS = TestDeadLetteredAtNamesTheTransitionNotTheTransactionStart|TestEachDeadLetterInABatchCarriesItsOwnTransition

mutate:
	@if not exist .env (echo No .env yet. Run: make env && exit 1)
	@copy $(MIGRATION) $(MIGRATION).orig >nul
	@powershell -NoProfile -Command "$$p='$(MIGRATION)'; $$t=[IO.File]::ReadAllText($$p); $$n=$$t -replace 'SET DEFAULT statement_timestamp\(\)','SET DEFAULT now()'; if ($$n -eq $$t) { exit 3 }; [IO.File]::WriteAllText($$p,$$n)" || (echo The mutation found nothing to change -- this gate is silently passing and must be updated && copy $(MIGRATION).orig $(MIGRATION) >nul && del $(MIGRATION).orig && exit 1)
	@echo Mutated: dead_lettered_at is back to DEFAULT now(). Both temporal gates must now fail.
	@go test ./outbox/ -run "$(TEMPORAL_TESTS)" -count=1 && (echo && echo The suite PASSED with the defect restored, so it does not test it. && copy $(MIGRATION).orig $(MIGRATION) >nul && del $(MIGRATION).orig && exit 1) || echo Both gates went red, as they must.
	@copy $(MIGRATION).orig $(MIGRATION) >nul
	@del $(MIGRATION).orig
	@echo Restored. Re-run `make test-integration` to confirm they pass again.

# ---------------------------------------------------------------------------
# Gates
# ---------------------------------------------------------------------------

build:
	go build ./...

# gofmt -l reports by printing names and exits 0 either way, so the check is whether it
# printed anything. findstr is the test rather than `for /f`, which exits 1 over an empty
# file -- the target then fails on a clean tree while printing no filename, which is worse
# than having no gate: it says the code is unformatted and does not say where.
fmt:
	@gofmt -l . > .fmt.tmp
	@findstr /r /c:"." .fmt.tmp >nul && (echo Not gofmt-clean: && type .fmt.tmp && del .fmt.tmp && exit 1) || (del .fmt.tmp && echo gofmt clean)

vet:
	go vet ./...

arch:
	go run ./tools/archcheck ./...

tidy:
	go mod tidy
	@git diff --exit-code go.mod go.sum || (echo go.mod or go.sum changed -- commit the result && exit 1)

gates: fmt vet build arch tidy test
	@echo All gates passed.

coverage:
	go test -count=1 -coverprofile=.coverage.out ./...
	@go tool cover -func=.coverage.out | findstr /c:"total:"

clean:
	go clean -cache -testcache
	@if exist .coverage.out del .coverage.out
	@if exist .fmt.tmp del .fmt.tmp
