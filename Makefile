shell:
	docker-compose run python make in-docker-configure
	docker-compose run --rm python /bin/bash

down:
	docker-compose down

clean:
	- docker-compose down --rmi local -v --remove-orphans

test-shell:
	docker-compose -p gainy_compute_test -f docker-compose.test.yml run test-python make in-docker-configure
	docker-compose -p gainy_compute_test -f docker-compose.test.yml run --rm test-python /bin/bash

install:
	poetry install

build: install
	poetry build

publish: build
	poetry config repositories.gainy $(shell aws codeartifact get-repository-endpoint --domain gainy-app --repository gainy-app --format pypi --query repositoryEndpoint --output text)
	poetry publish -n -u aws -p $(shell aws codeartifact get-authorization-token --domain gainy-app --query authorizationToken --output text) -r gainy

in-docker-configure: install
	apt update && apt install -y postgresql-client
	PGPASSWORD=${PG_PASSWORD} psql -h ${PG_HOST} -p ${PG_PORT} -U ${PG_USERNAME} ${PG_DBNAME} -P pager -c "CREATE SCHEMA IF NOT EXISTS $$PUBLIC_SCHEMA_NAME;"
	find ./fixtures -iname '*.sql' | sort | while read -r i; do PGOPTIONS="--search_path=$$PUBLIC_SCHEMA_NAME" PGPASSWORD=${PG_PASSWORD} psql -h ${PG_HOST} -p ${PG_PORT} -U ${PG_USERNAME} ${PG_DBNAME} -P pager -f "$$i"; done

in-docker-test: in-docker-configure
	poetry run gainy_recommendation --batch_size=100
	poetry run gainy_optimize_collections -o /tmp/gainy_optimize_collections.csv
	poetry run pytest tests/*

test-build:
	docker-compose -p gainy_compute_test -f docker-compose.test.yml build

test-python:
	docker-compose -p gainy_compute_test -f docker-compose.test.yml run --rm test-python make in-docker-test

test-clean:
	docker-compose -p gainy_compute_test -f docker-compose.test.yml down --rmi local -v
	docker-compose -p gainy_compute_test -f docker-compose.test.yml rm -sv

test: test-clean test-build test-python test-clean

style-check:
	yapf --diff -r .

style-fix:
	yapf -i -r .
