.PHONY: help setup up down test dbt-run lint

help:
	@echo "Brewery Analytics Pipeline — available commands:"
	@echo "  make setup      Install Python dependencies"
	@echo "  make up         Start all Docker services"
	@echo "  make down       Stop all Docker services"
	@echo "  make test       Run unit tests"
	@echo "  make dbt-run    Run dbt models"
	@echo "  make lint       Run flake8 linter"

setup:
	pip install -r requirements.txt

up:
	docker compose -f docker/docker-compose.yml up -d

down:
	docker compose -f docker/docker-compose.yml down

test:
	pytest tests/ -v

dbt-run:
	cd dbt/brewery_dbt && dbt run

lint:
	flake8 ingestion/ spark/ airflow/ tests/
