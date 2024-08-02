VENV_PATH='.venv/bin/activate'
DOCKER_NAME='ocelot'
DOCKER_TAG='0.0.1'
AZURE_CONTAINER_REGISTRY='dndsregistry.azurecr.io'

lint:
	black ocelot/

install:
	python3 -m pip install --upgrade pip
	# Used for packaging and publishing
	pip install setuptools wheel twine
	# Used for linting
	pip install black
	# Used for testing
	pip install pytest
	# Requirements
	pip install -r requirements.txt

env:
	. .env

build:
	docker build -t $(AZURE_CONTAINER_REGISTRY)/$(DOCKER_NAME):$(DOCKER_TAG) .

push:
	docker push $(AZURE_CONTAINER_REGISTRY)/$(DOCKER_NAME):$(DOCKER_TAG)

pull:
	docker pull $(AZURE_CONTAINER_REGISTRY)/$(DOCKER_NAME):$(DOCKER_TAG)

run:
	docker run -it -p 8080:80 --env-file .env $(AZURE_CONTAINER_REGISTRY)/$(DOCKER_NAME):$(DOCKER_TAG) bash

up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f
