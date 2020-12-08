#!/usr/bin/python3
#modif dans le docker deouis jupyter de etl.py 11:43

from pyspark.sql import types as T
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.functions import isnan, when, count, col
from pyspark.sql.types import FloatType, StringType, DecimalType
from pyspark.sql.functions import monotonically_increasing_id

from datetime import datetime
import datetime as dt
import pandas as pd
import configparser
import sys
import os
import re

from parse_file1 import parse_file
from read_file1 import read_csv, read_csv_global_airports, read_csv_iso_country, read_sas, read_labels_to_df
from process_tables import create_usairport_table, create_country_table, create_indicator_table, \
    create_demography_table, create_fact_student_table

def get_credentials():
    """
    get AWS keys
    """
    try:
        # parse file
        config = configparser.ConfigParser()
        config.read('dl.cfg')
        # set AWS variables
        os.environ['AWS_ACCESS_KEY_ID']     = config['AWS']['KEY']
        os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['SECRET']
    except Exception as e:
        print("Unexpected error: %s" % e)


def create_spark_session():
    """
    create or load a Spark session
    """
    try: 
        print("\n")
        print("...Create a Spark session...")
        spark = SparkSession \
            .builder \
            .appName("Us_student_immigation") \
            .config("spark.jars.packages","saurfang:spark-sas7bdat:3.0.0-s_2.12") \
            .enableHiveSupport() \
            .getOrCreate()
        return(spark)
    except Exception as e:
        print("Unexpected error: %s" % e)


def create_parquet_label(input_data):
    """
    Create parquet files from I94 Description Labels.
    """
    try:
        print("...Begin to create I94 Labels files")
        file = 'I94_SAS_Labels_Descriptions.SAS'
        ## make i94port.parquet
        key = "i94port"
        nb = parse_file(input_data, file, key)
        print(f'There are {nb} rows in {key}.parquet')
        
        ## make i94visa.csv
        key = "i94visa"
        nb = parse_file(input_data, file, key)
        print(f'There are {nb} rows in {key}.parquet')
        ## make i94addr.csv
        key = "i94addr"
        nb = parse_file(input_data, file, key)
        print(f'There are {nb} rows in {key}.parquet')
        # make i94cit_i94res.csv
        key = "i94cit_i94res"
        nb = parse_file(input_data, file, key)
        print(f'There are {nb} rows in {key}.parquet')
        # make i94mode.csv
        key = "i94mode"
        nb = parse_file(input_data, file, key)
        print(f'There are {nb} rows in {key}.parquet')
        return
    except Exception as e:
        print("Unexpected error: %s" % e)

def read_sas_csv(input_data, spark):
    """
    This function read sas file and csv, with functions in read_file1.py
    return dataframe
    """
    try:
        # df_immigration
        print('_____df_imigration____')
        cols = ['cicid','i94yr','i94mon','i94cit','i94res','i94port','i94mode', 'i94addr','i94bir','i94visa','dtadfile', 'gender','airline','visatype']
        file = '18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
        # todo :refaire avec S3 et tous les fichiers (get_path_sas_folder parquet file)
        df_immigration = read_sas(spark, input_data, file, cols)
        
        # df_temperature
        print('_____df_temperature____')
        cols = ['AverageTemperature', 'City', 'Country']
        file = 'GlobalLandTemperaturesByCity.csv'
        delimiter = ','
        df_temperature = read_csv(spark, input_data, file, cols, delimiter)
        
        # df_airport_code
        print('_____df_airport_code____')
        file = 'airport-codes_csv.csv'
        cols = ['ident', 'type','name', 'iso_country', 'iso_region', 'municipality', 'iata_code', 'local_code']
        delimiter= ','
        df_airport_code = read_csv(spark, input_data, file, cols, delimiter)
        
        # df_global_airports
        print('_____df_global_airports____')
        file = 'airports-extended.csv'
        cols = ['airport_ID','type','name', 'city', 'country', 'iata']
        delimiter = ','
        #header = False
        schema = T.StructType([
            T.StructField('airport_ID', T.IntegerType(), False),
            T.StructField('name', T.StringType(), False),
            T.StructField('city', T.StringType(), False),
            T.StructField('country', T.StringType(), False),
            T.StructField('iata', T.StringType(), False),
            T.StructField('icao', T.StringType(), False),
            T.StructField('latitude', T.StringType(), False),
            T.StructField('longitude', T.StringType(), False),
            T.StructField('altitude', T.IntegerType(), False),
            T.StructField('timezone', T.StringType(), False),
            T.StructField('dst', T.StringType(), False),
            T.StructField('tz_timezone', T.StringType(), False),
            T.StructField('type', T.StringType(), False),
            T.StructField('data_source', T.StringType(), False)
            ])
        df_global_airports = read_csv_global_airports(spark, input_data, file, cols, delimiter, schema, header=False )
        
        # df_iso_country
        print('_____df_iso_country____')
        file = 'wikipedia-iso-country-codes.csv'
        #cols = ['Country', 'Alpha_2','Alpha_3', 'Num_code', 'ISO_3166-2']
        #delimiter =','
        file = 'wikipedia-iso-country-codes.csv' 
        df_iso_country  = read_csv_iso_country(spark, input_data, file)
        
        # df_demograph
        print('_____df_demograph____')
        file = 'us-cities-demographics.csv'
        cols = ['City', 'State', 'Median Age', 'Male Population', 'Female Population', 'Total Population', 'Number of Veterans', 'Foreign-born', 'Average Household Size', 'State Code', 'Race', 'Count']
        delimiter = ';'
        df_demograph  = read_csv(spark, input_data, file, cols, delimiter)
        
        # df_indicator_dev
        print('_____df_indicator_dev____')
        file = 'WDIData.csv'
        delimiter = ','
        header =  False
        cols = ['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code', '2015']
        df_indicator_dev  = read_csv(spark, input_data, file, cols, delimiter)
        return(df_immigration, df_temperature, df_airport_code, df_global_airports, df_iso_country, df_demograph, df_indicator_dev)
    except Exception as e:
        print("Unexpected error: %s" % e)




def clean_immigration(df_immigration, i94_port):
    """
    clean and format the dataframe df_immigration
    """
    try:
        # create dictionnary from i94_port
        port_state_dic = dict([(i,a) for i, a in zip(i94_port.Port_id, i94_port.State_id)])
        # setup drop column
        drop_col = ['depdate', 'count', 'occup', 'entdepa', 'entdepd', 'entdepu', 'matflag', 'biryear', \
                    'insnum','visapost', 'fltno', 'admnum', 'insnum', 'dtaddto', 'arrdate', 'dtadfile']
        user_func =  udf(lambda x: port_state_dic.get(x))
        
        # drop columns
        newdf = df_immigration.drop(*drop_col) \
                            .withColumn('i94addr', F.when((F.col('i94addr').isNull()), \
                                                    user_func(df_immigration.i94port)) \
                                                .otherwise(F.col('i94addr')))
        # replace the null value and cast the columns in integer
        # int_col = ['cicid', 'i94yr', 'i94mon','i94cit', 'i94res', 'i94mode', 'i94bir', 'i94visa']
        null_int = {'cicid': -1, 'i94yr': -1, 'i94mon': -1,'i94cit': 239, 'i94res': 239, 'i94mode': 9, 'i94bir': -1, 'i94visa': -1}
        for k in null_int:
                newdf = newdf.withColumn(k, F.when((F.col(k).isNull()), null_int[k])
                            .otherwise(F.col(k).cast("int")))

        # replace the null value for the string
        # str_cols = ['i94addr', 'i94port', 'gender', 'airline', 'visatype']
        null_str = {'i94addr': '99', 'i94port': '999', 'gender': 'U', 'airline': 'unknown', 'visatype': '99' }
        for k in null_str:
                newdf = newdf.withColumn(k, F.when((F.col(k).isNull()), null_str[k])
                                        .otherwise(F.col(k)))

        df_immigration_clean = (df_immigration.withColumnRenamed("cicid", "id_i94") \
                    .withColumnRenamed("i94yr", "year") \
                    .withColumnRenamed("i94mon", "month") \
                    .withColumnRenamed("i94cit", "country_born_num") \
                    .withColumnRenamed("i94res", "country_res_num") \
                    .withColumnRenamed("i94port", "iata_code") \
                    .withColumnRenamed("i94mode", "arri_mode") \
                    .withColumnRenamed("i94addr", "state_id_arrival") \
                    .withColumnRenamed("i94bir", "age") \
                    .withColumnRenamed("i94visa", "arr_reason") \
                    .withColumnRenamed("gender", "gender") \
                    .withColumnRenamed("airline","airline") \
                    .withColumnRenamed("visatype:", "visatype"))
        df_immigration_clean = df_immigration_clean \
                .withColumn('arr_reason', df_immigration_clean.arr_reason.cast('int')) \
                .withColumn('arri_mode', df_immigration_clean.arri_mode.cast('int'))\
                .withColumn('country_res_num', df_immigration_clean.country_res_num.cast('int')) \
                .withColumn('country_born_num', df_immigration_clean.country_born_num.cast('int')) \
                .withColumn('age', df_immigration_clean.age.cast('int')) \
                .filter('arr_reason == 3') \
                .dropDuplicates()

        print('***** Make df_immigration_clean processing ')
        df_immigration_clean.printSchema()
        df_immigration_clean.show(2)
        return(df_immigration_clean)
    except Exception as e:
        print("Unexpected error: %s" % e)




def clean_temperature(df_temperature):
    """
    Clean and format dataframe df_temperature
    """
    try:
        # drop column "AverageTemperatureUncertainty"
        drop_cols = ["dt", "AverageTemperatureUncertainty", "Latitude", "Longitude", "city"]
        newdf = df_temperature.drop(*drop_cols)
        # make aggregation by temperature
        newdf = newdf.groupBy('Country') \
            .agg(F.avg("AverageTemperature")) \
            .orderBy('Country') \
            .dropDuplicates()
        newdf = (newdf.withColumnRenamed("Country", "country") \
                .withColumnRenamed("avg(AverageTemperature)", "avg_temperature"))
        newdf = newdf.withColumn("avg_temperature", newdf.avg_temperature.cast('float'))
        df_clean_temperature = newdf.orderBy('country')
        print('***** Make df_clean_temperature processing ')
        df_clean_temperature.printSchema()
        df_clean_temperature.show(2)
        return(df_clean_temperature)
    except Exception as e:
        print("Unexpected error: %s" % e)

def  clean_airport_code(df_airport_code):
    """
    clean dataframe df_airport_code and return a dataframe
    """
    try:
        # drop columns
        # filter closed , heliport and seaplace base airport, small_airport
        # keep us airport
        drop_cols = ["elevation_ft","continent", "gps_code", "coordinates"]
        drop_airport = ['closed', 'heliport', 'seaplane_base', 'small_airport', 'balloonport']
        keep_us = ['US']
        newdf =df_airport_code.drop(*drop_cols) \
                            .filter(~df_airport_code.type.isin(drop_airport)) \
                            .filter(df_airport_code.iso_country.isin(keep_us))
        #airport_code.groupBy('iso_country', 'iso_region').agg(count("*")).show()
        #l = ['US']
        newdf = newdf.withColumn("myisocountry", split(col("iso_region"), "-").getItem(0)) \
                    .withColumn("myisoregion", split(col("iso_region"), "-").getItem(1))
        newdf = newdf.withColumn("myisocountry",coalesce(newdf.myisocountry,newdf.iso_country))
        drop_cols = ['myisocountry', 'iso_region', 'local_code']
        newdf = newdf.drop(*drop_cols)
        airport_code = newdf.filter(~newdf.iata_code.isNull()).dropDuplicates()
        df_clean_airport_code = (airport_code.withColumnRenamed("ident", "ident") \
                            .withColumnRenamed("type", "airport_type") \
                            .withColumnRenamed("name", "airport_name") \
                            .withColumnRenamed("iso_country", "country_iso2") \
                            .withColumnRenamed("municipality", "city_name" ) \
                            .withColumnRenamed("iata_code", "iata_code") \
                            .withColumnRenamed("myisoregion", "state_id"))
        print('***** Make df_clean_airport_code processing ')
        df_clean_airport_code.printSchema()
        df_clean_airport_code.show(2)
        return(df_clean_airport_code)
    except Exception as e:
        print("Unexpected error: %s" % e)

def clean_global_airports(df_global_airports):
    """
    clean dataframe df_global_airports and return a dataframe
    """
    try:
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
    except Exception as e:
        print("Unexpected error: %s" % e)

def clean_iso_country(df_iso_country):
    """
    clean dataframe df_iso_country and return a dataframe
    """
    try:
        df = (df_iso_country.withColumnRenamed('English short name lower case','country_name') \
                            .withColumnRenamed('Alpha_2', 'country_iso2') \
                            .withColumnRenamed('Alpha_3', 'country_iso3') \
                            .withColumnRenamed('Num_code','country_num'))

        df_clean_iso_country =  df_iso_country.drop("ISO_3166-2") \
                                    .select(F.col("Country").alias("country_name"), \
                                        F.col("Alpha_2").alias("country_iso2"), \
                                        F.col("Alpha_3").alias("country_iso3"), \
                                        F.col("Num_code").alias("country_num") \
                                    .cast("int")) \
                                    .dropDuplicates()
        print('***** Make df_clean_iso_country processing ')
        df_clean_iso_country.printSchema()
        df_clean_iso_country.show(2)
        return(df_clean_iso_country)
    except Exception as e:
        print("Unexpected error: %s" % e) 

def clean_demograph(df_demograph): 
    """
    clean dataframe df_demograph and return a dataframe
    """
    try:
        drop_cols = ["Number_of_Veterans"]
        newdf = df_demograph.drop(*drop_cols) \
                    .select(F.col("City").alias("city_name"), \
                            F.col("State").alias("state_name"), \
                            F.col("Median Age").alias("median_age"), \
                            F.col("Male Population").alias("male_population"), \
                            F.col("Female Population").alias("female_population"), \
                            F.col("Total Population").alias("totale_population"), \
                            F.col("Foreign-born").alias("foreign_born"), \
                            F.col("State Code").alias("state_id"), \
                            F.col("Race").alias("ethnic"), \
                            F.col("Count"))
        df_clean_demograph = newdf.groupBy("state_name", "state_id", "city_name", "median_age", "male_population", "female_population", "ethnic") \
                                .agg(F.avg("Count").cast('int').alias("ethnic_count")) \
                                .orderBy("state_name", "city_name", "ethnic") \
                                .dropDuplicates()
        print('***** Make df_clean_demograph processing ')
        df_clean_demograph.printSchema()
        df_clean_demograph.show(2)
        return(df_clean_demograph)
    except Exception as e:
        print("Unexpected error: %s" % e)

def clean_indicator_dev(df_indicator_dev):
    """
    clean dataframe df_indicator_dev and return a dataframe
    """
    try:
        # get key words for indicators fields
        demography = ['population','birth','death','fertility','mortality','expectancy']
        food = ['food','grain','nutrition','calories']
        trade = ['trade','import','export','good','shipping','shipment']
        health = ['health','desease','hospital','mortality','doctor']
        economy = ['income','gdp','gni','deficit','budget','market','stock','bond','infrastructure']
        energy = ['fuel','energy','power','emission','electric','electricity']
        education = ['education','literacy']
        employment =['employed','employment','umemployed','unemployment']
        rural = ['rural','village']
        urban = ['urban','city']
        # select data in '2015'
        newdf = df_indicator_dev.where(F.col("2015").isNotNull())
        newdf = newdf.withColumnRenamed('2015', 'indic_2015')
        newdf = newdf.withColumn('indic_2015', newdf.indic_2015.cast(DecimalType(18, 2)))
        # create columns 'Indicator_group' to setup indicator fields
        newdf = newdf.withColumn(
            "indicator_group", 
            F.when( F.lower(F.col('Indicator Name')).rlike('|'.join(demography)), F.lit('demography').cast('string')) \
            .when( F.lower(F.col('Indicator Name')).rlike('|'.join(food)), F.lit('food').cast('string')) \
            .when( F.lower(F.col('Indicator Name')).rlike('|'.join(trade)), F.lit('trade').cast('string')) \
            .when( F.lower(F.col('Indicator Name')).rlike('|'.join(health)), F.lit('health').cast('string')) \
            .when( F.lower(F.col('Indicator Name')).rlike('|'.join(economy)), F.lit('economy').cast('string')) \
            .when( F.lower(F.col('Indicator Name')).rlike('|'.join(energy)), F.lit('energy').cast('string')) \
            .when( F.lower(F.col('Indicator Name')).rlike('|'.join(education)), F.lit('education').cast('string')) \
            .when( F.lower(F.col('Indicator Name')).rlike('|'.join(employment)), F.lit('employment').cast('string')) \
            .when( F.lower(F.col('Indicator Name')).rlike('|'.join(rural)), F.lit('rural').cast('string')) \
            .when( F.lower(F.col('Indicator Name')).rlike('|'.join(urban)), F.lit('urban').cast('string')))  
        # make aggregation 
        newdf = newdf.groupBy('Country Name', 'Country Code', 'indicator_group') \
                .agg(F.avg('indic_2015')).alias('avg_2015') \
                .orderBy('Country Name', 'indicator_group') \
                .where(F.col('indicator_group').isNotNull())
        df_clean_indicator_dev = newdf \
                            .select(F.col('Country Name').alias('country_name'), \
                                    F.col('Country code').alias('country_code'), \
                            'indicator_group', \
                            F.round(F.col('avg(indic_2015)'), 2).alias('avg_2015'))
        print('***** Make df_clean_indicator_dev processing ')
        df_clean_indicator_dev.printSchema()
        df_clean_indicator_dev.show(2)
        return( df_clean_indicator_dev)
    except Exception as e:
        print("Unexpected error: %s" % e)

def main():
    # input_data = "s3a://udacity-dend/"            # Data from Udacity
    input_data = "../data/"                               # Run locally

    # output_data = "s3a://dend-paris/sparkify/"    # Anthelix bucket(me)
    # output_data = "s3a://<YOUR_BUCKET>/"          # Visitor bucket
    output_data = "../output/"                     # Run localy
    output_parquet = "../output/"
    # Create a SparkSession
    spark = create_spark_session()
    print("... SparkSesson created is done...")

    # Create parquet files from I94 Description Labels.    
    create_parquet_label(input_data)
    print(' ')
    print('***** Make i94 labels files is done')
    print(' ')
    print(10*'#',5*' ', 'READING FILES AND CREATING DATAFRAME', 5*' ', 10*'#' )


    # read files and create dataframes
    # Todo: make a class with this stuff
    df_immigration, df_temperature, df_airport_code, df_global_airports, df_iso_country, df_demograph, df_indicator_dev  = read_sas_csv(input_data, spark)
    
    i94_mode, i94_ctry, i94_addr, i94_visa, i94_port = read_labels_to_df(input_data)
    print('***** All Dataframe are ready!')



    print(" ")
    # process df_immigration
    df_clean_immigration = clean_immigration(df_immigration, i94_port)
    print('df_immigration_clean is done')
    print(" ")
    # process df_temperature    
    df_clean_temperature = clean_temperature(df_temperature)
    print('df_temperature_clean is done')
    print(" ")
    # process df_airport_code
    df_clean_airport_code = clean_airport_code(df_airport_code)
    print('df_clean_airport_code is done')
    print(" ")
    # process df_global_airports
    df_clean_global_airports = clean_global_airports(df_global_airports)
    print('df_clean_global_airports is done')
    print(" ")
    # process df_iso_country
    df_clean_iso_country = clean_iso_country(df_iso_country) 
    print('df_clean_iso_country is done')
    print(" ")
    # process df_demograph
    df_clean_demograph = clean_demograph(df_demograph)
    print('df_clean_demograph is done')
    print(" ")
    # process df_indicator_dev
    df_clean_indicator_dev = clean_indicator_dev(df_indicator_dev)
    print('df_clean_indicator_dev is done')

    # Create dimensions tables and fact table, saved in parquet files
    print('***** create_usairport_table is processing...')
    dim_airport_us = create_usairport_table(df_clean_airport_code, df_clean_global_airports, output_parquet)
    print('Done!')
    print(" ")
    print('***** create_country_table is processing...')
    dim_country = create_country_table(df_clean_iso_country, df_clean_temperature, output_parquet)
    print('Done!')
    print(" ")
    print('***** create_indicator_table is processing...')
    dim_indicator = create_indicator_table(df_clean_indicator_dev, output_parquet)
    print('Done!')
    print(" ")
    print('***** create_demography_table is processing...')
    dim_demography = create_demography_table(df_clean_demograph, output_parquet)
    print('Done!')
    print(" ")
    print('***** create fact_student is processing...')
    fact_student = create_fact_student_table( df_clean_immigration, dim_airport_us, dim_country, dim_demography, dim_indicator, output_parquet)
    print('Done!')
    print(" ")

    # make check about fact table



if __name__ == "__main__":
    main()