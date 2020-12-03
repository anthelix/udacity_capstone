#### Udacity Nanodegree

# udacity_capstone

The purpose of this project is to study the foreign students. The goal is to offer Data teams Analysts a selection of data concerning immigration to the United States.

The demography of the United States was built thanks to the arrival of the first settlers of various European nationalities. In the 20th century, the population increased due to immigration. Here I would like to make the link with the world news and the variations of the entry to the USA via the airports. 

The database will be answers questions about the foreign students.
Where are the go to studies in USA?
Where are they from? 
It's provide some indicators about their origin countries and the arrival state.


I'll look at the `[US National Tourism and Trade Office](https://travel.trade.gov/research/reports/i94/historical/2016.html)` along with various immigration and other information about arriving person. I-94 Website is the Official Site for Travelers Visisting the United States. An I-94 form is needed by all Air and Sea travelers. 
The GDELT Project, [here](https://www.gdeltproject.org/), monitors the news around our world. 
These will form the base of my project. I need to add more information. 

## Data Source

Data |File |Data Source
-|-|-|
I94 Immigration | immigration_data_sample.csv| [US National Tourism and Trade Office](https://travel.trade.gov/research/programs/i94/description.asp)
I94 Description Labels  Description|I94_SAS_Labels_Descriptions.SAS |US National Tourism and Trade Office
Global Land Temperature|GlobalLandTemperaturesByCity.csv| [Berkeley Earth](http://berkeleyearth.org/)
Global Airports|airports-extended.csv| [OpenFlights.org and user contributions](https://www.kaggle.com/open-flights/airports-train-stations-and-ferry-terminals)
Airports codes |airport-codes_csv.csv| provide by Udacity
Iso country | wikipedia-iso-country-codes.csv|[Kaggle](https://www.kaggle.com/juanumusic/countries-iso-codes)
US Cities Demographic| us-cities-demographics.csv|provide by Udacity
Indicators developpment| WDIData.csv| [Kaggle](https://www.kaggle.com/xavier14/wdidata)
Education-statistics| EdStatsData.csv|provide by Kaggle [World Bank](https://www.kaggle.com/kostya23/

## Tools used

I used Python and Spark for ETL. I try the twice to improve my code. I used Pyspark and Pandas libraries. 
I run notebooks, python scripts and Spark jobs in a Docker Yarm. `git lfs track "dataset.tar.gz"` to push on GitHub large files.

## Folder structure

tree -CAL 2
.
├── data
│   ├── 18-83510-I94-Data-2016
│   ├── airport-codes_csv.csv
│   ├── airports-extended.csv
│   ├── airports_us.csv
│   ├── dataset.tar.gz
│   ├── GlobalLandTemperaturesByCity.csv
│   ├── I94_SAS_Labels_Descriptions.SAS
│   ├── immigration_data_sample.csv
│   ├── postgres
│   ├── us-cities-demographics.csv
│   ├── WDIData.csv
│   └── wikipedia-iso-country-codes.csv
├── dl.cfg
├── docker_nbextensions
│   ├── Dockerfile
│   └── docker.txt
├── output
│   ├── country.parquet
│   ├── demograph.parquet
│   ├── fact_student.parquet
│   ├── indicator.parquet
│   ├── us_airport.parquet
├── README.md
├── stack.yml
└── work
    ├── bootstrap_jupyter.sh
    ├── log4j.properties
    ├── Makefile
    ├── notebook
    │   ├── 0_Take_a_look_dataset.ipynb
    │   ├── 1_Exploration_python.ipynb
    │   ├── 2_Data_dictionnary.ipynb
    │   ├── 3_Explanation_capstone_project.ipynb
    │   ├── 4_Create_etl_work_process.ipynb
    │   ├── 5_create_etl_workinCopy.ipynb
    │   ├── etl.py
    │   ├── parse_file1.py
    │   ├── postgresql-42.2.10.jar
    │   ├── process_tables.py
    │   ├── read_file1.py
    ├── postgresql-42.2.10.jar
    └── requirements.txt





## How To Do

* I use compress the dataset in  `data.zip` 
* then , to upload large file to github
  * ```
        git lfs track "*.zip"
        git add .gitattributes
        git commit -m "Updated the attributes"
        git push
        git add my_large_file.zip
        git lfs ls-files
            And here I would ensure that I saw my_large_file.zip being tracked.
        git commit -m "Now I am adding the large file"
        git push
    ```


* In a terminal, run `git clone https://github.com/anthelix/udacity_capstone.git <folder>,` 
* `cd <folder>`
* Create `<folder>/data/postgres` directory for PostgreSQL files: `mkdir -p ./data/postgres`


  * In `folder`, unzip the file `data.zip` in a folder `data`



* Optional, for local development, install Python packages: `python3 -m pip install -r requirements.txt`
Optional, pull docker images first:

    ```bash
    docker pull jupyter/all-spark-notebook:latest
    docker pull postgres:12-alpine
    dock pull adminer:latest
    ```

* Deploy Docker Stack: `docker stack deploy -c stack.yml jupyter`
* Retrieve the token to log into Jupyter: `docker logs $(docker ps | grep jupyter_spark | awk '{print $NF}')`
* From the Jupyter terminal, run the install script: `Make install` == > sh bootstrap_jupyter.sh 
* then `Make etl` ==> python3 etl.py
* At the end, `docker stack rm stack.yml jupyter`, `docker swarm leave --force`, `docker rmi -f $(docker images -qa)`, `docker system prune --volumes`, `sudo docker images`

## ETL process

* Load the files
  * Load the files. The files are '.sas7bdat', '.csv', and '.parquet'
  * Function example:
    ```
        def load_global_airports(path, file):
            df = spark.read \
                .format("csv") \
                .option('header', 'True') \
                .option('inferSchema', 'true') \
                .schema(global_airports_schema) \
                .load(path+file)
            nb_rows = df.count()
            print(f'*****         Loading {nb_rows} rows')
            print(f'*****         Display the Schema')
            df.printSchema()
            print(f'*****         Display few rows')
            df.show(3, truncate = False)
            return df, nb_rows
    ```

  * we get those dataframe

  ```
  Variable             Type         Data/Info
  -------------------------------------------
  df_airport_code      DataFrame    DataFrame[ident: string, <...>ring, local_code: string]
  df_demograph         DataFrame    DataFrame[City: string, S<...>Race: string, Count: int]
  df_global_airports   DataFrame    DataFrame[airport_ID: int<...>ry: string, iata: string]
  df_immigration       DataFrame    DataFrame[cicid: double, <...>string, visatype: string]
  df_indicator_dev     DataFrame    DataFrame[Country Name: s<...>de: string, 2015: double]
  df_iso_country       DataFrame    DataFrame[Country: string<...>ring, ISO_3166-2: string]
  df_temperature       DataFrame    DataFrame[AverageTemperat<...> string, Country: string]
  ```

  * I have processed the file 'I94 Description Labels Description` to extract and store in parquet files data. 
  
    ```
    Running "../../data/I94_SAS_Labels_Descriptions.SAS"
    
    There are 583 rows in i94port.parquet
    There are 3 rows in i94visa.parquet
    There are 55 rows in i94addr.parquet
    There are 289 rows in i94cit_i94res.parquet
    There are 4 rows in i94mode.parquet
    
    ***** Make i94 labels files is done!
    ```

* Cleasning
  * I kept data useful, rename columns, drop duplicates
  * Function examples:
  
    ```
    def clean_global_airports(df_global_airports):
        drop_cols = ["icao","type", "latitude", "longitude", "altitude", "timezone", "dst", "tz_timezone", "data_source"]
        newdf = df_global_airports.filter(df_global_airports.type.isin('airport', 'unknown')) \
                            .drop(*drop_cols)

        df_clean_global_airports = newdf.select(F.col("airport_ID").alias("airport_id").cast("int"), \
                                                F.col("name").alias("airport_name"), \
                                                F.col("city").alias("city_name"), \
                                                F.col("country").alias("country_name"), \
                                                F.col("iata").alias("iata_code")) \
                                        .dropDuplicates()    
        print('***** Make df_clean_global_airports processing ')
        df_clean_global_airports.printSchema()
        df_clean_global_airports.show(2)
        return(df_clean_global_airports)
    ```

    ```
        ***** Make df_clean_global_airports processing 
    root
    |-- airport_id: integer (nullable = true)
    |-- airport_name: string (nullable = true)
    |-- city_name: string (nullable = true)
    |-- country_name: string (nullable = true)
    |-- iata_code: string (nullable = true)

    +----------+--------------------+---------+------------+---------+
    |airport_id|        airport_name|city_name|country_name|iata_code|
    +----------+--------------------+---------+------------+---------+
    |       263|Margaret Ekpo Int...|  Calabar|     Nigeria|      CBQ|
    |       428|       Ivalo Airport|    Ivalo|     Finland|      IVL|
    +----------+--------------------+---------+------------+---------+
    only showing top 2 rows

    ```

* Create Dimensions anf fact tables
    * 
    ```
    def create_country_table(df_clean_iso_country, df_clean_temperature, output_parquet):
    # create country table
    # output_parquet = '../../output/'
    tic = df_clean_iso_country.alias('tic')
    tt = df_clean_temperature.alias('tt')
    df_join = tic.join(tt, (tic.country_name == tt.country), how='left')
    dim_country = df_join \
                        .filter('country_num != ""' and 'country_iso3 != ""') \
                        .drop_duplicates(subset = ['country_name']) \
                        .orderBy('country_name') \
                        .drop('country')
    dim_country.show(5)
    dim_country.collect()
    parquet_path = output_parquet + 'country.parquet'
    write_parquet(dim_country, parquet_path)
    return(dim_country)
    ```

    ```
        +--------------+------------+------------+-----------+---------------+
    |  country_name|country_iso2|country_iso3|country_num|avg_temperature|
    +--------------+------------+------------+-----------+---------------+
    |   Afghanistan|          AF|         AFG|          4|      13.816497|
    |       Albania|          AL|         ALB|          8|      15.525828|
    |       Algeria|          DZ|         DZA|         12|      17.763206|
    |American Samoa|          AS|         ASM|         16|           null|
    |       Andorra|          AD|         AND|         20|           null|
    +--------------+------------+------------+-----------+---------------+
    only showing top 5 rows
    ```





