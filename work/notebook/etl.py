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
from read_file1 import *
from process_tables import *
from cleasning import *

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
        sys.exit()


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
        print("Unexpected error in create_spark_session(): %s" % e)
        sys.exit()


def create_parquet_label(path_raw_data):
    """
    Create parquet files from I94 Description Labels.
    """
    try:
        print("...Begin to create I94 Labels files")
        file = 'I94_SAS_Labels_Descriptions.SAS'
        ## make i94port.parquet
        key = "i94port"
        print("*********")
        nb = parse_file(path_raw_data, file, key)
        print(f'There are {nb} rows in {key}.parquet')        
        ## make i94visa.csv
        key = "i94visa"
        nb = parse_file(path_raw_data, file, key)
        print(f'There are {nb} rows in {key}.parquet')
        ## make i94addr.csv
        key = "i94addr"
        nb = parse_file(path_raw_data, file, key)
        print(f'There are {nb} rows in {key}.parquet')
        # make i94cit_i94res.csv
        key = "i94cit_i94res"
        nb = parse_file(path_raw_data, file, key)
        print(f'There are {nb} rows in {key}.parquet')
        # make i94mode.csv
        key = "i94mode"
        nb = parse_file(path_raw_data, file, key)
        print(f'There are {nb} rows in {key}.parquet')
        return
    except Exception as e:
        print("Unexpected error in create_parquet_label(): %s" % e)
        sys.exit()

def read_sas_csv(path_raw_data, spark):
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
        df_immigration = read_sas(spark, path_raw_data, file, cols)
        
        # df_temperature
        print('_____df_temperature____')
        cols = ['AverageTemperature', 'City', 'Country']
        file = 'GlobalLandTemperaturesByCity.csv'
        delimiter = ','
        df_temperature = read_csv(spark, path_raw_data, file, cols, delimiter)
        
        # df_airport_code
        print('_____df_airport_code____')
        file = 'airport-codes_csv.csv'
        cols = ['ident', 'type','name', 'iso_country', 'iso_region', 'municipality', 'iata_code', 'local_code']
        delimiter= ','
        df_airport_code = read_csv(spark, path_raw_data, file, cols, delimiter)
        
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
            T.StructField('latitude', T.StringType(), True),
            T.StructField('longitude', T.StringType(), True),
            T.StructField('altitude', T.IntegerType(), True),
            T.StructField('timezone', T.StringType(), True),
            T.StructField('dst', T.StringType(), True),
            T.StructField('tz_timezone', T.StringType(), True),
            T.StructField('type', T.StringType(), True),
            T.StructField('data_source', T.StringType(), True)
            ])
        df_global_airports = read_csv_global_airports(spark, path_raw_data, file, cols, delimiter, schema, header=False )
        
        # df_iso_country
        print('_____df_iso_country____')
        file = 'wikipedia-iso-country-codes.csv'
        #cols = ['Country', 'Alpha_2','Alpha_3', 'Num_code', 'ISO_3166-2']
        #delimiter =','
        file = 'wikipedia-iso-country-codes.csv' 
        df_iso_country  = read_csv_iso_country(spark, path_raw_data, file)
        
        # df_demograph
        print('_____df_demograph____')
        file = 'us-cities-demographics.csv'
        cols = ['City', 'State', 'Median Age', 'Male Population', 'Female Population', 'Total Population', 'Number of Veterans', 'Foreign-born', 'Average Household Size', 'State Code', 'Race', 'Count']
        delimiter = ';'
        df_demograph  = read_csv(spark, path_raw_data, file, cols, delimiter)
        
        # df_indicator_dev
        print('_____df_indicator_dev____')
        file = 'WDIData.csv'
        delimiter = ','
        header =  False
        cols = ['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code', '2015']
        df_indicator_dev  = read_csv(spark, path_raw_data, file, cols, delimiter)
        return(df_immigration, df_temperature, df_airport_code, df_global_airports, df_iso_country, df_demograph, df_indicator_dev)
    except Exception as e:
        print("Unexpected error in read_sas_csv: %s" % e)
        sys.exit()


def main():
    # path_raw_data = "s3a://udacity-dend/"            # Data from Udacity
    path_raw_data = "../data/"                               # Run locally
    input_data = "../input/"
    # output_data = "s3a://dend-paris/sparkify/"    # Anthelix bucket(me)
    # output_data = "s3a://<YOUR_BUCKET>/"          # Visitor bucket
    output_data = "../output/"                     # Run localy
    output_parquet = "../output/"
    
    # Create a SparkSession
    spark = create_spark_session()
    print("... SparkSesson created is done...")

    # Create parquet files from I94 Description Labels.    
    create_parquet_label(path_raw_data)
    print(' ')
    print('***** Make i94 labels files is done')
    print(' ')
    print(10*'#',5*' ', 'READING FILES AND CREATING DATAFRAME', 5*' ', 10*'#' )


    # read files and create dataframes
    df_immigration, df_temperature, df_airport_code, df_global_airports, df_iso_country, df_demograph, df_indicator_dev  = read_sas_csv(path_raw_data, spark)
    
    i94_mode, i94_ctry, i94_addr, i94_visa, i94_port = read_labels_to_df(input_data)
    print('***** All Dataframe are ready!')



    print(" ")
    # process df_immigration
    df_clean_immigration = clean_immigration(spark, input_data)
    print('df_immigration_clean is done')
    print(" ")
    # process df_temperature    
    df_clean_temperature = clean_temperature(spark, input_data)
    print('df_temperature_clean is done')
    print(" ")
    # process df_airport_code
    df_clean_airport_code = clean_airport_code(spark, input_data)
    print('df_clean_airport_code is done')
    print(" ")
    # process df_global_airports
    df_clean_global_airports = clean_global_airports(spark, input_data)
    print('df_clean_global_airports is done')
    print(" ")
    # process df_iso_country
    df_clean_iso_country = clean_iso_country(spark, input_data) 
    print('df_clean_iso_country is done')
    print(" ")
    # process df_demograph
    df_clean_demograph = clean_demograph(spark, input_data)
    print('df_clean_demograph is done')
    print(" ")
    # process df_indicator_dev
    df_clean_indicator_dev = clean_indicator_dev(spark, input_data)
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
    print('***** create fact_immigration is processing...')
    fact_immigration = create_fact_immigration_table(df_clean_immigration, output_parquet)
    print('Done!')
    print(" ")

if __name__ == "__main__":
    main()