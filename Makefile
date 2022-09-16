shell:
	docker-compose run --rm python /bin/bash

clean:
	- docker-compose down --rmi local -v --remove-orphans

test-shell:
	docker-compose -p gainy_compute_test -f docker-compose.test.yml run test-python make in-docker-test-configure
	docker-compose -p gainy_compute_test -f docker-compose.test.yml run --rm test-python /bin/bash

build:
	poetry build

publish: build
	poetry config repositories.gainy $(shell aws codeartifact get-repository-endpoint --domain gainy-app --repository gainy-app --format pypi --query repositoryEndpoint --output text)
	poetry publish -n -u aws -p $(shell aws codeartifact get-authorization-token --domain gainy-app --query authorizationToken --output text) -r gainy

in-docker-test-configure:
	apt update && apt install -y postgresql-client
	find tests/fixtures -iname '*.sql' | sort | while read -r i; do PGPASSWORD=${PG_PASSWORD} psql -h ${PG_HOST} -p ${PG_PORT} -U ${PG_USERNAME} ${PG_DBNAM} -P pager -f "$$i"; done

in-docker-test: in-docker-test-configure
	poetry run gainy_recommendation --batch_size=100
	poetry run pytest tests/*

test-build:
	docker-compose -p gainy_compute_test -f docker-compose.test.yml build

test-python:
	docker-compose -p gainy_compute_test -f docker-compose.test.yml run --rm test-python make in-docker-test

test-clean:
	docker-compose -p gainy_compute_test -f docker-compose.test.yml down --rmi local -v
	docker-compose -p gainy_compute_test -f docker-compose.test.yml rm -sv

test: test-build test-python test-clean

style-check:
	yapf --diff -r .

style-fix:
	yapf -i -r .
