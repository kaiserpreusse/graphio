
help:
	@echo "make localdb - run DB only in background"
	@echo "make stop - stop all containers"


localdb:
	docker compose down
	docker compose up -d

stop:
	docker compose down