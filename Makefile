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

ps:
	docker compose ps

clean:
	docker compose down -v --rmi all --remove-orphans

clear:
	docker system prune -a -f --volumes
