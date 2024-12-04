build:
	docker compose -f tema2.yaml up --build

clean:
	docker compose -f tema2.yaml down