install:
	- poetry init && poetry install

test:
	- poetry run pytest tests/*
