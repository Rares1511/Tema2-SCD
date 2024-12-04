
build:
	docker compose -f tema2.yaml build
	docker compose -f tema2.yaml up

clean:
	docker compose -f tema2.yaml down