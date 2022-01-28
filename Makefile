install:
	poetry install

build:
	poetry build

publish: build
	poetry config repositories.gainy $(shell aws codeartifact get-repository-endpoint --domain gainy-app --repository gainy-app --format pypi --query repositoryEndpoint --output text)
	poetry publish -n -u aws -p $(shell aws codeartifact get-authorization-token --domain gainy-app --query authorizationToken --output text) -r gainy

test:
	poetry run pytest tests/*
