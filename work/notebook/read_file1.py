#!/usr/bin/python3

import pandas as pd
import sys
import re

from pyspark.sql import functions as F

from process_tables import write_parquet

# invalid characters in parquet column names are replaced by _
def canonical(x): return re.sub("[ ,;{}()\n\t=]+", '_', x.lower())

def read_sas(spark, path, file, cols):
    """
    read file from '18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
    return dataframe 'df_immigration' and make parquet file ./input/i94_apr16
    """ 
    output_parquet = '../input/'
    key = 'i94_apr16'
    print(" ")
    print(f"...Load the file :   {path}{file}.")
    df = spark.read \
        .format('com.github.saurfang.sas.spark') \
        .option('header', 'true') \
        .load(path+file) \
        .select(cols)
    nb_rows = df.count()
    print(" ")
    #print(f'*****         Loading {nb_rows} rows')
    #print(f'*****         Display the Schema')
    #df.printSchema()
    #print(f'*****         Display few rows')
    df.show(3, truncate = False)
    parquet_path = output_parquet + key
    write_parquet(df, parquet_path)
    # check data
    check = spark.read.parquet(parquet_path).select("cicid")
    nb_check = check.count()
    try:
        if nb_check == nb_rows:
            print(f" CHECK {parquet_path} SUCESS")
        else:
            print(f" CHECK {parquet_path} FAILLLLL")
            print(f" WITH {nb_rows} IN DATAFRAME ")
            print(f" AND {nb_check} AFTER LOADING FILE")
            sys.exit()
    except Exception as e:
        print(f"{parquet_path}")
        print("Unexpected error in check : %s" % e)
        sys.exit()
    return df

def read_csv(spark, path, file, cols, delimiter):
    """
    read csv file, return a dataframe
    Create a csv file ./input/namefile
    """
    output_parquet = '../input/'
    key = file.split('.')[0]
    print(" ")
    print("                                           read_csv")
    print(f"...Load the file :   {path}{file}.")
    df = spark.read \
        .format("csv") \
        .option('header', 'true') \
        .option('inferSchema', 'true') \
        .option('delimiter', delimiter) \
        .load(path+file) \
        .select(cols)

    df = df.select([F.col(col).alias(col.replace(' ,;{}()\n\t=', '')) for col in df.columns])
    nb_rows = df.count()
    #print(f'*****         Loading {nb_rows} rows')
    #print(f'*****         Display the Schema')
    #df.printSchema()
    #print(f'*****         Display few rows')
    df.show(3, truncate = False)
    parquet_path = output_parquet + key
    renamed_cols = [canonical(c) for c in df.columns]
    df = df.toDF(*renamed_cols)
    write_parquet(df, parquet_path)
    # check data
    check = spark.read.parquet(parquet_path)
    nb_check = check.count()

    try:
        if nb_check == nb_rows:
            print(f" CHECK {parquet_path} SUCESS")
        else:
            print(f" CHECK {parquet_path} FAILLLLL")
            print(f" WITH {nb_rows} IN DATAFRAME ")
            print(f" AND {nb_check} AFTER LOADING FILE")
            sys.exit()
    except Exception as e:
        print(f"{parquet_path}")
        print("Unexpected error in check : %s" % e)
        sys.exit()
    return df

def read_csv_global_airports(spark, path, file, cols, delimiter,schema, header):
    """
    read csv file with a custom schema
    return a dataframe
    """
    output_csv = '../input/'
    key = file.split('.')[0]
    print("                                           read_csv global airport")
    print(" ")
    print(f"...Load the file :   {path}{file}.")  
    df = spark.read \
        .format("csv") \
        .option('header', header) \
        .option('inferSchema', 'true') \
        .option('delimiter', delimiter) \
        .schema(schema) \
        .load(path+file) \
        .select(cols)
    df.printSchema()    
    
    nb_rows = df.count()
    #print(f'*****         Loading {nb_rows} rows')
    #print(f'*****              Display the Schema')
    #df.printSchema()          
    #print(f'*****              Display few rows')
    df.show(3, truncate = False)
    path_file = output_csv + key + ".csv"
    df.toPandas().to_csv(path_file)
    # check data
    check = spark.read.csv(f'{output_csv}{key}.csv')
    nb_check = check.count()        
    nb_rows +=1
    try:
        if nb_check == nb_rows:
            print(f" CHECK {path_file} SUCESS")
        else:
            print(f" CHECK {path_file} FAILLLLL")
            print(f" WITH {nb_rows} IN DATAFRAME ")
            print(f" AND {nb_check} AFTER LOADING FILE")
            sys.exit()
    except Exception as e:
        print(f"{path_file}")
        print("Unexpected error in check : %s" % e)
        sys.exit()
    return df

def read_csv_iso_country(spark, path, file):
    """
    read csv file 
    return a dataframe
    """
    output_csv = '../input/'
    key = file.split('.')[0]
    print("                                           read_csv_iso_country")
    print(" ")
    print(f"...Process the file :   {path}{file}.")

    #cols = ['English short name lower case', 'Alpha-2 code','Alpha-3 code', 'Numeric code', 'ISO_3166-2']
    
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
    nb_rows +=1
    #print(f'*****         Loading {nb_rows} rows')
    #print(f'*****              Display the Schema')
    #df.printSchema()          
    #print(f'*****              Display few rows')
    #df.show(3, truncate = False)
    df.toPandas().to_csv(f'{output_csv}{key}.csv')
    check = spark.read.csv(f'{output_csv}{key}.csv')
    nb_check = check.count()
    path_file = output_csv + key + ".csv"
    try:
        if nb_check == nb_rows:
            print(f" CHECK {path_file} SUCESS")
        else:
            print(f" CHECK {path_file} FAILLLLL")
            print(f" WITH {nb_rows} IN DATAFRAME ")
            print(f" AND {nb_check} AFTER LOADING FILE")
            sys.exit()
    except Exception as e:
        print(f"{path_file}")
        print("Unexpected error in check : %s" % e)
        sys.exit()
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