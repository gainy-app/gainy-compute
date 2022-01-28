install:
	- poetry install

test:
	- poetry run pytest tests/recommendations/*
