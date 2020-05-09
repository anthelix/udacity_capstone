.PHONY: 1 2 3 

# spark workflow
1: 
	if test -d data/postgres; then echo exists; else mkdir -m 755 -p ./data/postgres; fi
	@docker stack deploy -c stack.yml pyspark
	@sleep 2
	@docker stack ps pyspark --no-trunc
	

2:
	@docker exec -it $(shell docker ps -q --filter ancestor=jupyter/all-spark-notebook) pip install psycopg2-binary
	@docker logs $(shell docker ps -q --filter ancestor=jupyter/all-spark-notebook)
	@echo admin running on http://localhost:8080
	@echo admin credentials
	@echo system: postgresSQL
	@echo serveur:postgres
	@echo utilisateur:postgres
	@echo mdp:postgres1234
	@echo click authentifications
	
3:
	@docker stack rm pyspark



config:
	@cp settings/local/secret_template.yaml  ./airflow-secret.yaml
	@chmod 755 ./airflow-secret.yaml
	$(info Make: >>> ****** Complete the new file "./airflow-secret.yaml" with your credentials, please ****** <<<)	

up:
	@cp ./airflow-secret.yaml settings/local/secret.yaml
	@chmod -w settings/local/secret.yaml
	$(info Make: Starting containers)
	@docker-compose up --build -d 
	@echo airflow running on http://localhost:8080

.airflow-secret:
	@rm -f ./airflow-secret.yaml
	@sleep 10	
	@docker-compose exec webserver bash -c "python3 settings/import-secret.py"
	@rm -f settings/local/secret.yaml

variable:
	@docker-compose run --rm webserver airflow variables --import /usr/local/airflow/dags/config/variables.json
	@echo airflow setup variables

start: config up .airflow-secret variable

clean:	down
	$(info Make: Removing secret files and Docker logs)	
	@rm -f settings/local/secret.yaml
	@docker-compose rm -f
	@rm -rf logs/*

kill:
	$(info Make: Kill docker-airflow containers.)
	@echo "Killing docker-airflow containers"
	docker kill $(shell docker ps -q --filter ancestor=puckel/docker-airflow)

tty:
	docker exec -i -t $(shell docker ps -q --filter ancestor=puckel/docker-airflow) /bin/bash

psql:
	docker exec -i -t $(shell docker ps -q --filter ancestor=postgres:9.6) psql -U airflow

