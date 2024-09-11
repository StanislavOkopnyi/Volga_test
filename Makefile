- install:
	uv pip install -r requirements.txt

- lock:
	uv pip compile pyproject.toml -o requirements.txt

- run:
	(\
	python -m venv .venv ; \
	source ./.venv/bin/activate ; \
	pip install uv ; \
	uv pip install -r requirements.txt ; \
	docker-compose up -d ; \
	python ./app/main.py \
	)
