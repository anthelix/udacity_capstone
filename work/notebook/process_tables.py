#!/usr/bin/python3
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import monotonically_increasing_id
import sys

def write_parquet(table, parquet_path):
    """
    write parquet files 
    """
    try:
        table.write.parquet(parquet_path, mode = 'overwrite')
    except Exception as e:
        print("Unexpected error: %s" % e)


def create_usairport_table(df_clean_airport_code, df_clean_global_airports, output_parquet):
    """
    create airport table and saved in parquet file
    # output_parquet = '../../output/'
    """
    try:
        tac = df_clean_airport_code.alias('tac')
        tga = df_clean_global_airports.alias('tga')
        df_join = tac.join(tga, ((tac.iata_code == tga.iata_code) | (tac.airport_name == tga.airport_name) | (tac.city_name == tga.city_name)), how='left')
        dim_airport_us = df_join \
                        .filter('tga != tga.iata_code ""' and 'state_id !=""') \
                        .selectExpr("airport_id",
                                   "ident",
                                   "tga.iata_code",
                                   "tac.airport_name",
                                   "tac.city_name",
                                   "state_id") \
                        .sort('ident') \
                        .dropDuplicates(['iata_code'])
        dim_airport_us.printSchema()
        print(dim_airport_us.count())
        dim_airport_us.collect()
        parquet_path = output_parquet + 'usairport_table'
        dim_airport_us.toPandas().to_csv(output_parquet+"usairport_table.csv", header=True)
        write_parquet(dim_airport_us, parquet_path)
        return(dim_airport_us)
    except Exception as e:
        print("Unexpected error: %s" % e)
        sys.exit()

def create_country_table(df_clean_iso_country, df_clean_temperature, output_parquet):
    """
    # create country table
    # output_parquet = '../../output/'
    """
    try:
        tic = df_clean_iso_country.alias('tic')
        tt = df_clean_temperature.alias('tt')
        df_join = tic.join(tt, (tic.country_name == tt.country), how='left')
        dim_country = df_join \
                            .filter('country_num != ""' and 'country_iso3 != ""') \
                            .drop_duplicates(subset = ['country_name']) \
                            .orderBy('country_name') \
                            .drop('country')
        dim_country.printSchema()
        dim_country.show(2)
        dim_country.collect()
        dim_country.toPandas().to_csv(output_parquet+"country_table.csv", header=True)
        parquet_path = output_parquet + 'country_table'
        write_parquet(dim_country, parquet_path)
        return(dim_country)
    except Exception as e:
        print("Unexpected error: %s" % e)
        sys.exit()

def create_indicator_table(df_clean_indicator_dev, output_parquet):
    """
    # create indicator table
    # output_parquet = '../../output/'
    """
    try:
        dim_indicator = df_clean_indicator_dev \
                    .filter('country_code !=""') \
                    .dropDuplicates() \
                    .orderBy('country_name') \
                    .select('country_code', 'indicator_group', 'avg_2015' )
        dim_indicator.printSchema()
        dim_indicator.show(2)
        dim_indicator.collect()
        dim_indicator.toPandas().to_csv(output_parquet+"indicator_table.csv", header=True)
        parquet_path = output_parquet + 'indicator_table'
        write_parquet(dim_indicator, parquet_path)
        return( dim_indicator)
    except Exception as e:
        print("Unexpected error: %s" % e)
        sys.exit()

def create_demography_table(df_clean_demograph, output_parquet):
    # create demography table per ethnic per state
    try:
        dim_demography = df_clean_demograph \
                        .groupBy( 'state_id', 'ethnic') \
                        .agg((F.avg('ethnic_count').cast(T.DecimalType(22, 2))).alias('avg_ethnic')) \
                        .dropDuplicates() \
                        .orderBy("state_id")                 

        dim_demography.printSchema()
        #print(dim_airport_us.count())
        dim_demography.show(2)
        dim_demography.collect()
        dim_demography.toPandas().to_csv(output_parquet+"demograph_table.csv", header=True)
        parquet_path = output_parquet + 'demograph_table'
        write_parquet(dim_demography, parquet_path)
        
        return(dim_demography)
    except Exception as e:
        print("Unexpected error: %s" % e)
        sys.exit()

def create_fact_immigration_table(df_clean_immigration, output_parquet ):  
    """
    create facts tables
    """
    try:
        fact_student = df_clean_immigration.alias('fact')

        fact_student.printSchema()
        fact_student.count()
        fact_student.toPandas().to_csv(output_parquet+"fact_immigration.csv", header=True)
        fact_student.collect()
        parquet_path = output_parquet + 'fact_immigration'
        write_parquet(fact_student, parquet_path)
        return(fact_student)
    except Exception as e:
        print("Unexpected error: %s" % e)
        sys.exit()