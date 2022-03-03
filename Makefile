install:
	poetry install

build:
	poetry build

publish: build
	poetry config repositories.gainy $(shell aws codeartifact get-repository-endpoint --domain gainy-app --repository gainy-app --format pypi --query repositoryEndpoint --output text)
	poetry publish -n -u aws -p $(shell aws codeartifact get-authorization-token --domain gainy-app --query authorizationToken --output text) -r gainy

in-docker-test:
	poetry run pytest tests/*

test-build:
	docker-compose -p gainy_compute_test -f docker-compose.test.yml build

test-python:
	docker-compose -p gainy_compute_test -f docker-compose.test.yml run test-python make in-docker-test

test-clean:
	docker-compose -p gainy_test -f docker-compose.test.yml down --rmi local -v
	docker-compose -p gainy_test -f docker-compose.test.yml rm -sv

test: test-build test-python test-clean
