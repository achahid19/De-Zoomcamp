up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

log_pipeline:
	docker compose logs -f pipelines

log_db:
	docker compose logs -f sql_service

debug_pipeline:
	docker run -it --rm --entrypoint bash etl_image:latest

connect_db:
	uv run pgcli -h localhost -p ${DB_PORT} -U ${DB_USER} -d ${DB_NAME}

ingest_data:
	uv run python ingest_data.py

ps:
	docker compose ps

clean:
	docker compose down -v --rmi all --remove-orphans

clear:
	docker system prune -a -f --volumes

