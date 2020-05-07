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
## comme j'utilise Docker swarn et qu'il ne reconnait pas la commande Docker build, j'ai creer des images dans mon docker hub, qui sont charger par docker. 

```
# commande pour crere et push des images sur le hub de docker
docker build -t my_postgres ./docker/postgres
docker images
sudo docker tag 6813196e69e4 anthelix/my_postgres:latest
docker images
sudo docker login
sudo docker push anthelix/my_postgres



```

Try the alternate Jupyter stack with [nbextensions](https://jupyter-contrib-nbextensions.readthedocs.io/en/latest/install.html) pre-installed, first `cd docker`, then run `docker build -t garystafford/all-spark-notebook-nbext:latest .` to build the new image

1. Clone this project from GitHub:

    ```bash
    git clone 
    ```

2. Go to the wordir folder
    `cd Capstone`
    `WORKDIR=$PWD`

3. Create $WORKDIR/data/postgres` directory for PostgreSQL files: `mkdir -p $WORDIR/data/postgres`
4. Optional, for local development, install Python packages: `python3 -m pip install -r requirements.txt`
5. Optional, pull docker images first:

    ```bash
    docker pull jupyter/all-spark-notebook:latest
    docker pull anthelix/my_postgres
    docker pull anthelix/my_airflow

    ```
6. Build `docker build -t  anthelix/my_postgres ./docker/postgres`, `docker build -t  anthelix/my_airflow ./docker/airflow`, `docker pull jupyter/all-spark-notebook:latest

`docker swarm init`
memo: 
Swarm initialized: current node (j69pk5v0laudyephn42chy444) is now a manager.

To add a worker to this swarm, run the following command:

    docker swarm join --token SWMTKN-1-5rt5xwrzf1jzb7i3hemzj604mr8a1v158hhmru7y0jeshb1u0x-33ui0effqrc1ho1tse0iw7ley 192.168.0.61:2377

To add a manager to this swarm, run 'docker swarm join-token manager' and follow the instructions.
7. Deploy Docker Stack: `docker stack deploy -c stack.yml jupyter`
8. `docker stack ps jupyter --no-trunc`
8. Retrieve the token to log into Jupyter: `docker logs $(docker ps | grep jupyter_spark | awk '{print $NF}')`
9. From the Jupyter terminal, run the install script: `sh bootstrap_jupyter.sh`
Export your Plotly username and api key to `.env` file:

    ```bash
    echo "PLOTLY_USERNAME=your-username" >> .env
    echo "PLOTLY_API_KEY=your-api-key" >> .env
    ```

## Demo

From a Jupyter terminal window:

1. Sample Python script, run `python3 01_simple_script.py` from Jupyter terminal
2. Sample PySpark job, run `$SPARK_HOME/bin/spark-submit 02_pyspark_job.py` from Jupyter terminal
3. Load PostgreSQL sample data, run `python3 03_load_sql.py` from Jupyter terminal
4. Sample Jupyter Notebook, open `04_notebook.ipynb` from Jupyter Console
5. Sample Jupyter Notebook, open `05_notebook.ipynb` from Jupyter Console
6. Try the alternate Jupyter stack with [nbextensions](https://jupyter-contrib-nbextensions.readthedocs.io/en/latest/install.html) pre-installed, first `cd docker_nbextensions/`, then run `docker build -t garystafford/all-spark-notebook-nbext:latest .` to build the new image
7. Then, to delete the previous stack, run `docker stack rm jupyter`, followed by creating the new stack, run `cd -` and `docker stack deploy -c stack-nbext.yml jupyter`