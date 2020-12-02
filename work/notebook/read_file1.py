#!/usr/bin/python3

import pandas as pd
import sys
import re

def read_sas(spark, path, file, cols):
    """
    read file from '18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    return numbers of rows and dataframe 'df_immigration'
    """ 
    print(" ")
    print(f"...Path file is :  {path}{file} is processing...")
    df = spark.read \
        .format('com.github.saurfang.sas.spark') \
        .option('header', 'true') \
        .load(path+file) \
        .select(cols)
    nb_rows = df.count()
    print(" ")
    print(f'*****         Loading {nb_rows} rows')
    print(f'*****         Display the Schema')
    df.printSchema()
    print(f'*****         Display few rows')
    df.show(3, truncate = False)
    return df

def read_csv(spark, path, file, cols, delimiter):
    """
    read csv file and return a dataframe
    """
    print(" ")
    print(f"...Path file is :  {path}{file} is processing...")
    df = spark.read \
        .format("csv") \
        .option('header', 'true') \
        .option('inferSchema', 'true') \
        .option('delimiter', delimiter) \
        .load(path+file) \
        .select(cols)
    nb_rows = df.count()
    print(f'*****         Loading {nb_rows} rows')
    print(f'*****         Display the Schema')
    df.printSchema()
    print(f'*****         Display few rows')
    df.show(3, truncate = False)
    return df

def read_csv_global_airports(spark, path, file, cols, delimiter,schema, header):
    """
    read csv file with a custom schema
    return a dataframe
    """
    print(" ")
    print(f"...Path file is :  {path}{file} is processing...")
    
  
    df = spark.read \
        .format("csv") \
        .option('header', header) \
        .option('inferSchema', 'true') \
        .option('delimiter', delimiter) \
        .schema(schema) \
        .load(path+file) \
        .select(cols)
    nb_rows = df.count()
    print(f'*****         Loading {nb_rows} rows')
    print(f'*****              Display the Schema')
    df.printSchema()          
    print(f'*****              Display few rows')
    df.show(3, truncate = False)
    return df

def read_csv_iso_country(spark, path, file):
    """
    read csv file 
    return a dataframe
    """
    print(" ")
    print(f"...Path file is :  {path}{file} is processing...")
    #cols = ['English short name lower case', 'Alpha-2 code','Alpha-3 code', 'Numeric code', 'ISO_3166-2']
    
    # *********************************************** remove .schema(schema\ .select(cols)
    df = spark.read \
        .format("csv") \
        .option('header', 'true') \
        .option('inferSchema', 'true') \
        .load(path+file) 
        
    df.show(3, truncate = False)
    
    df = df.withColumnRenamed("English short name lower case", "Country")\
           .withColumnRenamed("Alpha-2 code", "Alpha_2")\
           .withColumnRenamed("Alpha-3 code", "Alpha_3")\
           .withColumnRenamed("Numeric code", "Num_code")
    
    nb_rows = df.count()
    print(f'*****         Loading {nb_rows} rows')
    print(f'*****              Display the Schema')
    df.printSchema()          
    print(f'*****              Display few rows')
    df.show(3, truncate = False)
    return df

def read_labels_to_df(input_data):
    """
    read data from parquet file and return dataframe
    """
    i94_mode = pd.read_parquet(input_data+'i94mode.parquet')
    print(f'***** Dataframe i94_mode *****')
    print("There are {} rows.".format(len(i94_mode)))
    print(' ')

    i94_ctry = pd.read_parquet(input_data+'i94cit_i94res.parquet')
    print(f'***** Dataframe i94_ctry *****')
    print("There are {} rows.".format(len(i94_ctry)))
    print(' ')

    i94_addr = pd.read_parquet(input_data+'i94addr.parquet')
    print(f'***** Dataframe i94_addr *****')
    print("There are {} rows.".format(len(i94_addr)))
    print(' ')

    i94_visa = pd.read_parquet(input_data+'i94visa.parquet')
    print(f'***** Dataframe i94_visa *****')
    print("There are {} rows.".format(len(i94_visa)))
    print(' ')

    i94_port = pd.read_parquet(input_data+'i94port.parquet')
    print(f'***** Dataframe i94_port *****')
    print("There are {} rows.".format(len(i94_port)))
    print(' ')
    return(i94_mode,i94_ctry,i94_addr,i94_visa,i94_port)