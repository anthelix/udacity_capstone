#### Udacity Nanodegree

# udacity_capstone

The purpose of this project is to study the foreign students. The goal is to offer Data teams Analysts a selection of data concerning immigration to the United States.

[Here](https://anthelix.github.io/blog/), I write a blog during my capstaone project. 

The demography of the United States was built thanks to the arrival of the first settlers of various European nationalities. In the 20th century, the population increased due to immigration. Here I would like to make the link with the world news and the variations of the entry to the USA via the airports. 

The database will be answers questions about the foreign students.
Where are the go to studies in USA?
Where are they from? 
It's provide some indicators about their origin countries and the arrival state.


I'll look at the `[US National Tourism and Trade Office](https://travel.trade.gov/research/reports/i94/historical/2016.html)` along with various immigration and other information about arriving person. I-94 Website is the Official Site for Travelers Visisting the United States. An I-94 form is needed by all Air and Sea travelers. 
The GDELT Project, [here](https://www.gdeltproject.org/), monitors the news around our world. 
These will form the base of my project. I need to add more information. 

#### Data Source

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
Education-statistics| EdStatsData.csv|provide by Kaggle [World Bank](https://www.kaggle.com/kostya23/worldbankedstatsunarchived) # Edit: not used

#### Tools used

I used Python and Spark for ETL. I try the twice to improve my code. I used Pyspark and Pandas libraries
I run notebooks, python scripts and Spark jobs in a Docker Yarm.


#### ETL process
* Load the files
    * Load the files from '../../data/'. The files are '.sas7bdat', '.csv', and '.parquet'
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





