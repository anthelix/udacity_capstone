#### Udacity Nanodegree

# udacity_capstone
## Project Summary

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

## Tools used

I used **Python** to explore and **PySpark** for ETL. I try the twice to improve my code. I used **Pyspark and Pandas libraries**. 
I run notebooks, python scripts and Spark jobs in a **Docker Yarm**. 
* I use compress the dataset in  `data.zip` 
  * then , to upload large file to github
     ```
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

### **Folder structure**

<details>
  <summary>Click here to see the result of the command TREE of the main folder</summary>
<pre>
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
</pre>
</details>
</br>

### **To Run**
I used Docker.

* In a terminal, run `git clone https://github.com/anthelix/udacity_capstone.git <folder>,` 
* `cd <folder>`
* Unzip `data.zip` : It's make a folder `data` in `<folder>`
* in `<folder>`:
  * pull docker images first:
    ```
    docker pull jupyter/all-spark-notebook:latest
    ```
  * Deploy Docker Stack: `docker stack deploy -c stack.yml jupyter`
  * Retrieve the token to log into Jupyter: `docker logs $(docker ps | grep jupyter_spark | awk '{print $NF}')`
    * ctrl + clic on "http://127. ..."
* From the Jupyter terminal, run the install script: `Make install` == > sh bootstrap_jupyter.sh (in jupyter Home , at the right, click button `new`, then terminal)
* then `Make etl` ==> python3 etl.py
* A new folder `output` will created with the dimensions and fact table in `<folder>`
* At the end, `docker stack rm stack.yml jupyter`, `docker swarm leave --force`, `docker rmi -f $(docker images -qa)`, `docker system prune --volumes`, `sudo docker images`

Without Docker:
  * Go to the `./work` directory
  * for local development, install Python packages: `python3 -m pip install -r requirements.txt`
  * Run Anaconda, then Jupyter Notebook
  * From the Jupyter terminal, run the install script: `Make install` == > sh bootstrap_jupyter.sh (in jupyter Home , at the right, click button `new`, then `terminal`)
  * Then `Make etl
  * `Make re` clean the folder and delete folders `input` and `output`

## ETL process

* Process the file 'I94 Description Labels Description` to extract and store details in parquet files data. 
    <details>
      <summary>Click here to see the result.</summary>
    <pre>
    
      ```
      Running "../../data/I94_SAS_Labels_Descriptions.SAS"
      
      There are 583 rows in i94port.parquet
      There are 3 rows in i94visa.parquet
      There are 55 rows in i94addr.parquet
      There are 289 rows in i94cit_i94res.parquet
      There are 4 rows in i94mode.parquet
      
      ***** Make i94 labels files is done!
      ```
    </pre>
    </details>

* Load the files
  * The files are  in '.sas7bdat', '.csv', and '.parquet', create dataframes.
  <details>
    <summary>Click here to see the result.</summary>
  <pre>
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
  </pre>
  </details>

* Cleasning
  * I kept data useful, rename columns, drop duplicates
  <details>
    <summary>Click here to see the result.</summary>
  <pre>
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
  </pre>
  </details>

* Create Dimensions anf fact tables in a new folder `<folder>/ouput`
  <details>
    <summary>Click here to see the result.</summary>
  <pre>
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
  </pre>
  </details>

# Summary
## [Step 1: Scope the Project and Gather Data](#step-1-scope-the-project-and-gather-data) 
## [Step 2: Explore and Assess the Data](#step-2-explore-and-assess-the-data)
## [Step 3: Define the Data Model](#step-3-define-the-data-model)
## [Step 4: Run ETL to model the data](#step-4-run-etl-to-model-the-data)
## [Step 5: Complete Project write up](#step-5-complete-project-write-up)

___
</br>

## **Step 1: Scope the Project and Gather Data**
Data warehouse allow us to collect, transform and manage data from varied sources. Then, Data Team Business connect to it and analyse data.  

The main dataset includes data on immigration to the United State.
The questions about foreign students and their choice to come to US may be useful to propose services.   
How many students arrived in US in April?    
Which Airline bring the most student in April?    
What are the top city to arrive in the USA?   
Where are from?   
what are the student profils (age, country born, country indicators)?   


In this project, the data is transforming, cleansing, staging and load into a datawarehouse.
* [Take a look to the dataset](./work/notebook/0_Take_a_look_dataset.ipynb)
* [Exploration with Python](./work/notebook/1_Exploration_python.ipynb)
* [Load raw data and saved in staging files](./work/notebook/read_file1.py)
* [Data cleansing](./work/notebook/cleasning.py)
* [Create dimensions and fact tables](./work/notebook/process_tables.py)
* Answer to the questions

I use Python, PySpark, Docker, Amazon S3. 

### **Describe and Gather Data**

[Data dictionnary](./2_Data_dictionnary.ipynb) provide informations about dataset and tables used.

#### **Data Source**

Data |File |Data Source
-|-|-|
 I94 Immigration data | 18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat| [US National Tourism and Trade Office](https://travel.trade.gov/research/programs/i94/description.asp)
Airports data |airport-codes_csv.csv| provide by Udacity
Global Airports|airports-extended.csv| [OpenFlights.org and user contributions](https://www.kaggle.com/open-flights/airports-train-stations-and-ferry-terminals)
Global Land Temperature|GlobalLandTemperaturesByCity.csv| [Berkeley Earth](http://berkeleyearth.org/)
I94 Description Labels  Description|I94_SAS_Labels_Descriptions.SAS |US National Tourism and Trade Office
US Cities Demographic| us-cities-demographics.csv|provide by Udacity
Indicators developpment| WDIData.csv| [Kaggle](https://www.kaggle.com/xavier14/wdidata)
Iso country | wikipedia-iso-country-codes.csv|[Kaggle](https://www.kaggle.com/juanumusic/countries-iso-codes)
</br>

  <details>
    <summary>Click for details about I94 Immigration data .</summary>
  <pre>
#### I94 Immigration data  Description: 
file : 18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat
Each line correspond to a record of I-94 Form from the U.S. immigration officers. It's provide information about Arrival/Departure to foreign visitors. Some explanation about the [Visitor Arrivals Program (I-94 Form)](https://travel.trade.gov/research/programs/i94/description.asp).  

Dataset information: There is a file per month for 2016, storage format is sas7bdat. These records are described according to 28 variables.   
A small description is provided [here](./2_Data_dictionnary.ipynb#I94)  
I keep this variables for this project( _df_immigration_ ):
    
Column Name | Description | Example | Type
-|-|-|-|
**cicid**|     ID uniq per record in the dataset | 4.08e+06 | float64
**i94yr**|     4 digit year  | 2016.0 | float64
**i94mon**|    Numeric month |  4.0 | float64      
**i94cit**|     3 digit code of source city for immigration (Born country) | 209.0 | float64
**i94res**|    3 digit code of source country for immigration |209.0 | float64
**i94port**|   Port addmitted through | HHW | object
**arrdate**|   Arrival date in the USA | 20566.0 | float64
**i94mode**|   Mode of transportation (1 = Air; 2 = Sea; 3 = Land; 9 = Not reported) | 1.0 | float
**i94addr**|   State of arrival | HI | object
**i94bir**|    Age in years | 61.0 | float
**i94visa**|   Visa Code - 1 = Business / 2 = Pleasure / 3 = Student |2.0 | float
**dtadfile**|  Date Field in I94 files |20160422| int 64
**gender**|    Gender|M| object
**visatype**|  Class of admission legally admitting the non-immigrant to temporarily stay in U.S.|WT|object
**airline**|Airline used to arrive in U.S.|MU|Object
  </pre>
  </details>

  <details>
    <summary>Click for details about Airports Data.</summary>
  <pre>
#### Airports Data Description
file : airport-codes_csv.csv
The airport code refers to the IATA airport code, 3 letters code unique for all airports in the world. It's a code used in passenger reservation, ticket and baggage-handling too.     
Dataset information: The airport-codes_csv.csv provides informations about aiports and have 12 variables. A small description is provided [here](./2_Data_dictionnary.ipynb#airportscode). I keep this variables for this project ( _df_airport_code_ ):

Column Name | Description | Example | Type
-|-|-|-|
**ident**| Unique identifier Airport code| 00AK| object 
**type**| Type of airport | small_airport |object
**name**| Name of the airport | Lowell Field | object
**iso_country**| ISO code of airport country |US| object
**iso_region**| ISO code of the region airport | US-KS|object
**municipality**| City name where the airport is located | Anchor Point|object
**iata_code**| IATA code of the airport| | object
  </pre>
  </details>

  <details>
    <summary>Click for details about Global Airports Data.</summary>
  <pre>

#### Global Airports Data
file : airports-extended.csv
This is a database of airports, train stations, and ferry terminals around the world. Some of the data come from public sources and some of it comes from OpenFlights.org user contributions.      
Dataset information: A small description is provided [here](./2_Data_dictionnary.ipynb#globalairports). I give name and keep this variables ( _df_global_airports_ ):

Column Name | Description | Example | Type
-|-|-|-|
**airport_ID**|Id in the table|1| Int
**airport_name**|Name of airport|Nadzab Airport|Object
**airport_city**|Main city served by airport|Nadzab|Object
**airport_country**|Country or territory where airport is located|Papua New Guinea|Object
**airport_iata**|3-letter IATA code|LAE|Object
  </pre>
  </details>

  <details>
    <summary>Click for details about Global Land Temperature Data.</summary>
  <pre>
####  Global Land Temperature Data  Description
file : GlobalLandTemperaturesByCity.csv
The Berkeley Earth Surface Temperature Study provide climate information. Each line correspond to a record of temperature per day from city around the world.     
Dataset information: the GlobalLandTemperaturesByCity.csv has 7 variables. A small description is provided [here](./2_Data_dictionnary.ipynb#temperature). I keep this variables for this project ( _df_temperature_ ):

Column Name | Description | Example | Type
-|-|-|-|
**dt**|Date format YYYY-MM-DD| 1743-11-01| object
**AverageTemperature**|Average Temperature for the city to th date dt|6.07|float64
**City**| City name| Århus| object
**Country**| Country name | Denmark | object
  </pre>
  </details>

  <details>
    <summary>Click for details about I94 Description Labels.</summary>
  <pre>

#### I94 Description Labels  Description
file : I94_SAS_Labels_Descriptions.SAS
The I94_SAS_Labels_Description.SAS file is provide to add explanations  about code used in _data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat._ 
I parse this file, save the result in 5 .csv files. 
   * i94visa Data
    * i94country and i94residence Data
    * i94port Data
    * i94mode Data
    * i94addr  
    
A small description is provided [here](./2_Data_dictionnary.ipynb#labels)
  </pre>
  </details>

  <details>
    <summary>Click for details about US cities Demographics data.</summary>
  <pre>

#### US cities Demographics
file: us-cities-demographics.csv
This dataset contains information about the demographics of all US cities and come from the US Census Bureau.     
Dataset information: A small description is provided [here](./2_Data_dictionnary.ipynb#uscities). 
This dataset contains 12 variables and provides simple informations about us state population. 
I keep this variables for this project ( _df_demograph_ ):

Column Name | Description | Example | Type
-|-|-|-|
**City**|Name of the city|Silver Spring|Object
**State**|US state of the city|Maryland|Object
**Median Age**|The median of the age of the population|33.8|Float64
**Male Population**|Number of the male population|40601.0|Float64
**Female Population**|Number of the female population|41862.0|Float64
**Total Population**|Number of the total population|82463 	|Float64
**Foreign-born**|Number of residents of the city that were not born in the city|30908.0|Float64
**State Code**|Code of the state of the city|MD|Object|
**Race**|Race class|Hispanic or Latino|Object
**Count**|Number of individual of each race|25924|Int64
  </pre>
  </details>

  <details>
    <summary>Click for details about  World Development Indicators data.</summary>
  <pre>
#### World Development Indicators
file : WDIData.csv
The primary World Bank collection of development indicators, compiled from officially-recognized international sources. It presents the most current and accurate global development data available, and includes national, regional and global estimates.   
Dataset information: This dataset contains 64 variables with economics context , most of which are variables per year(1960 to 2018).
A small description is provided [here](./2_Data_dictionnary.ipynb#indicators).
I keep this variables for this project ( _df_indicator_dev_ ):

Column Name | Description | Example | Type
-|-|-|-|
**Country Name**|Name of the country|Arab World|Object|
**Country Code**|3 letters code of country|ARB|Object
**Indicator Name**|indicators of economic development|2005 PPP conversion factor, GDP (LCU per inter...|Object
**Indicator Code**|letters indicator code|PA.NUS.PPP.05|Object
**1960 ...2018**|one column per year since 1960|2018|Float64
  </pre>
  </details>

  <details>
    <summary>Click for details about  Iso country data.</summary>
  <pre>
#### Iso country
file: wikipedia-iso-country-codes.csv
This is a database about the different code useful to identify country.        
Datasset information: A small description is provided [here](./2_Data_dictionnary.ipynb#isocountry). This table gives us informations about Country codes used to identify each country and contains 4 variables. I keep this variables for this project ( _df_iso_country_ ):

Column Name | Description | Example | Type
-|-|-|-|
**English short name lower case**|Country Name in English|Wallis and Futuna|Object
**Alpha-2 code**|code 2 letter code for the country|WF|Object
**Alpha-3 code**|code 3 letter code for the country|WLF|Object
**Numeric code**|ISO 3166-2 code|876|Int
  </pre>
  </details>
</br>

## **Step 2: Explore and Assess the Data**
### **Explore the Data** 
#### Data Source

[Data dictionnary](2_data_dictionnary.ipynb) provides informations about dataset and tables used. [This notebook](1_Exploration_python.ipynb) performs a first exploration with Python and explain the datasets, which variables I kept. 

Dataset |File |Data Source|Dataframe Name
|-|-|-|-|
|I94 Immigration | immigration_data_sample.csv| [US National Tourism and Trade Office](https://travel.trade.gov/research/programs/i94/description.asp)| df_immigration
I94 Description Labels  Description|I94_SAS_Labels_Descriptions.SAS |US National Tourism and Trade Office|
Global Land Temperature|GlobalLandTemperaturesByCity.csv| [Berkeley Earth](http://berkeleyearth.org/)|df_temperature
Global Airports|airports-extended.csv| [OpenFlights.org and user contributions](https://www.kaggle.com/open-flights/airports-train-stations-and-ferry-terminals)|df_global_airports
Airports codes |airport-codes_csv.csv| provide by Udacity|df_airport_code
Iso country | wikipedia-iso-country-codes.csv|[Wikipedia](https://gist.github.com/radcliff/f09c0f88344a7fcef373)|df_iso_country
US Cities Demographic| us-cities-demographics.csv|provide by Udacity|df_demograph
Indicators developpment| WDIData.csv| [World Bank](https://www.kaggle.com/xavier14/wdidata)|df_indicator_dev
</br>

<details>
  <summary>Click here to deploy the whole resume of the dataset.</summary>
<pre>
##### I94 Immigration Data
* Source: https://travel.trade.gov/research/reports/historical/2016.html
    * data 'data/18-83510-I94-Data-2016', provide one file per month
        * These records are described according to 28 variables and 3M  rows per file
        *  It's provide information about Arrival/Departure to foreign visitors        
    * I94_SAS_Labels_Description.SAS for variable descriptions
    
##### Global Land Temperature Data
* Source: http://berkeleyearth.org/
    * data 'GlobalLandTemperaturesByCity.csv' provide climate information
        * Each line correspond to a record of temperature per day from city around the world.
        * The GlobalLandTemperaturesByCity.csv has 7 variables and 8599213 rows.
        
##### Global Airports Data
* Source: https://www.kaggle.com/open-flights/airports-train-stations-and-ferry-terminals
    * data 'airports-extended.csv'. Some of the data come from public sources and some of it comes from OpenFlights.org user contributions.
        * It's provide informatioms about of airports, train stations, and ferry terminals around the world.
        * There are 4 variables in 'airports-extended.csv'and 10668 rows
        
##### Airports Data Description Data
* Source: https://datahub.io/core/airport-codes#data
    * airport-codes_csv.csv. The airport code refers to the IATA airport code, 3 letters code unique for all airports in the world
        * The airport-codes_csv.csv provides informations about aiports.
        * There are 55075 rows and 12 columns in airport-codes_csv.csv.
        
##### Iso country Data
* Source: https://gist.github.com/radcliff/f09c0f88344a7fcef373
    * data 'wikipedia-iso-country-codes.csv'. This is a database about the different code useful to identify country.
        * This table gives us informations about Country codes used to identify each country
        * There are 4 variables and 247 rows.
        
##### US cities Demographics Data
* Source: https://data.census.gov/cedsci/. 
    * data 'us-cities-demographics.csv'. This dataset contains information about the demographics of all US cities and come from the US Census Bureau.
        * Provides simple informations about US State population
        * Contains 12 variables and 2892 rows
        
##### World Development Indicators Data
* Source: https://www.kaggle.com/xavier14/wdidata
    * data 'WDIData.csv'. The primary World Bank collection of development indicators, compiled from officially-recognized international sources. 
        * It presents the most current and accurate global development data available, and includes national, regional and global estimates.
        * Contains 64 variables, most of which are variables per year(1960 to 2018), with economics context and 422137 rows.
               
##### i94addr Data
* Source: I94_SAS_Labels_Description.SAS
    * US States code defined in I94_SAS_Labels_Description.SAS
        * data 'i94addr.csv' provides State Id and State name  
        
##### i94city_i94res Data
* Source: I94_SAS_Labels_Description.SAS
    * data 'i94cit_i94res.csv' defined Code Country by 3 digits
        * data 'i94cit_i94res.csv' provides Country Id and Country name
        
##### i94mode Data
* Source: I94_SAS_Labels_Description.SAS
    * data 'i94mode.csv' defined arrival US
        * data 'i94mode.csv' provides code Mode and name Code.
        
##### i94port Data
* Source: I94_SAS_Labels_Description.SAS
    * data 'i94port.csv'
        * data 'i94port.csv' provides Port Id, Port city and State Id.
        
##### i94visa Data
* Source: I94_SAS_Labels_Description.SAS
    * data 'i94visa.csv'
        * data 'i94visa.csv' povides code Visa ans Visa
</pre>
</details>  
</br>
In the first one, the exploration was done with Python and an extract from I94. In a second time, the ETL script uses Pyspark given the I94 file size. 

[This notebook 1_Exploration_python](./work/notebook/1_Exploration_python.ipynb#explore) performs the exploration with python.

## **Step 3: Define the Data Model**
3.1 Conceptual Data Model

On the basis of a star schema, this allows to quickly find the elements linked to each other.It consists of a large fact table and a circle of other tables that contain the descriptive elements of the fact, called "dimensions". Table fact contaiins observable data (the facts) that we have on a subject and that we want to study, according axes of analysis (the dimensions).
The immigration dataset is the center of this project and allow us to explore foreign visitors. It will the fact table. Dimension tables give us information about a piece of this visitors, country, airport, indicator economics, and us demography. 

---
3.2 Mapping out data pipeline
# TODO : add schema of table nullable and unique
# TODO: Revoir la fact table? nettoyage? remplacement des missing value, format?
TODO: les scripts clean doivent lire depuis le datalake, depuis input, RAS le 9/12
# TODO: regarder dans project 4 ou 3, script pour S3 et Redshift, et donc revoir la structure du dossier? est ce que je le fais avec docker?

```
***** create fact_student is processing...
root
 |-- id_i94: double (nullable = true)
 |-- year: double (nullable = true)
 |-- month: double (nullable = true)
 |-- country_born_num: integer (nullable = true)
 |-- country_res_num: integer (nullable = true)
 |-- age: integer (nullable = true)
 |-- gender: string (nullable = true)
 |-- ethnic: string (nullable = true)
 |-- iata_code: string (nullable = true)
 |-- avg_temperature: float (nullable = true)
 |-- avg_2015: decimal(22,2) (nullable = true)
 |-- city_name: string (nullable = true)
 |-- visatype: string (nullable = true)
 |-- ti.id_i94: long (nullable = false)
```
```
***** create_demography_table is processing...
root
 |-- state_id: string (nullable = true)
 |-- ethnic: string (nullable = true)
 |-- avg_ethnic: decimal(22,2) (nullable = true)
```

```
***** create_indicator_table is processing...
root
 |-- country_code: string (nullable = true)
 |-- indicator_group: string (nullable = true)
 |-- avg_2015: decimal(22,2) (nullable = true)
```

```
***** create_country_table is processing...
root
 |-- country_name: string (nullable = true)
 |-- country_iso2: string (nullable = true)
 |-- country_iso3: string (nullable = true)
 |-- country_num: integer (nullable = true)
 |-- avg_temperature: float (nullable = true)
```
--- 

## Step 4: Run ETL to model the data




## Step 5: Complete Project write up
    