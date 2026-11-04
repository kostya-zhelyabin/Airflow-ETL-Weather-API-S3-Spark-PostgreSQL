.PHONY: init up down logs reset

init:
	test -f .env || cp .env.example .env
	docker compose up -d --build

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f airflow-scheduler airflow-dag-processor

reset:
	docker compose down -v --remove-orphans
