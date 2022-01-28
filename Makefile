poetry-build:
	poetry build

poetry-install:
	poetry install

poetry-publish:
	poetry publish -u aws -p $CODEARTIFACT_AUTH_TOKEN -r gainy-app

install: poetry-build poetry-install

install: poetry-build poetry-publish

test:
	poetry run pytest tests/recommendations/*
