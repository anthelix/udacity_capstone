AIRFLOW SPARK JUPYTER 


[bakery](https://towardsdatascience.com/getting-started-with-data-analytics-using-jupyter-notebooks-pyspark-and-docker-57c1aaab2408)


Nous ne sommes pas limités aux NOtebooks de Jupyter pour interagir avec Spark. 
Nous pouvons également soumettre des scripts directement à Spark depuis le terminal 
Jupyter. 
C'est généralement ainsi que Spark est utilisé dans une production pour 
effectuer des analyses sur de grands ensembles de données, souvent selon un 
calendrier régulier, en utilisant des outils tels que Apache Airflow. 

Avec Spark, vous chargez des données provenant d'une ou plusieurs sources de données. Après 
avoir effectué des opérations et des transformations sur les données, les données 
sont conservées dans un entrepôt de données, tel qu'un fichier ou une base de données, 
ou transmises à un autre système pour un traitement ultérieur.

## WORKFLOW WITH SPARK (stack.yml)
1. Clone this project from GitHub:

    ```bash
    git clone 
    ```

2. Go to the wordir folder
    `cd Capstone`

3. Create ./data/postgres` directory for PostgreSQL files: `mkdir -p ./data/postgres` --> Makefile do this.

4. Optional, pull docker images first:

    ```bash
    docker pull jupyter/all-spark-notebook:latest

    ```
5. `docker swarm init`
6. Deploy Docker Stack: `docker stack deploy -c stack.yml pyspark`
7. `docker stack ps pyspark --no-trunc`
8. `docker exec -it $(docker ps | grep pyspark_pyspark | awk '{print $NF}') pip install psycopg2-binary`
9. Retrieve the token to log into Jupyter: `docker logs $(docker ps | grep pyspark_pyspark | awk '{print $NF}')`

Adminer should be available on localhost port 8080.
The password credentials are available in the stack.yml file. 
The server name, postgres, is the name of the PostgreSQL container. This is the domain name the Jupyter container will use to communicate with the PostgreSQL container.

	@echo system: postgresSQL
	@echo serveur:postgres
	@echo utilisateur:postgres
	@echo mdp:postgres1234
	@echo click authentifications

## Demo

From a Jupyter terminal window:

1. Sample Python script, run `python3 01_simple_script.py` from Jupyter terminal
2. Sample PySpark job, run `$SPARK_HOME/bin/spark-submit 02_pyspark_job.py` from Jupyter terminal
3. Load PostgreSQL sample data, run `python3 03_load_sql.py` from Jupyter terminal
4. Sample Jupyter Notebook, open `04_notebook.ipynb` from Jupyter Console
5. Sample Jupyter Notebook, open `05_notebook.ipynb` from Jupyter Console
6. Try the alternate Jupyter stack with [nbextensions](https://jupyter-contrib-nbextensions.readthedocs.io/en/latest/install.html) pre-installed, first `cd docker_nbextensions/`, then run `docker build -t garystafford/all-spark-notebook-nbext:latest .` to build the new image
7. Then, to delete the previous stack, run `docker stack rm jupyter`, followed by creating the new stack, run `cd -` and `docker stack deploy -c stack-nbext.yml jupyter`