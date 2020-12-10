#!/usr/bin/python3
#modif dans le docker deouis jupyter de etl.py 11:43

import pandas as pd
import sys
import re
import os

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


def clean_immigration(spark, input_data):
    """
    clean and format the dataframe df_immigration
    """
    try:
        #read files
        i94_port = pd.read_parquet(input_data+'i94port.parquet')
        i94_visa = pd.read_parquet(input_data+'i94visa.parquet')
        i94_mode = pd.read_parquet(input_data+'i94mode.parquet')
        df_immigration = spark.read.option("header","true").option("recursiveFileLookup","true").parquet(input_data+'i94_apr16')
        # create dictionnary from i94_port
        port_state_dic = dict([(i,a) for i, a in zip(i94_port.Port_id, i94_port.State_id)])
        # create dictionnary from i94_visa
        visa_dic = dict([(i,a) for i, a in zip(i94_visa.Code_visa.astype('float'), i94_visa.Visa)])
        # create dictionnary from i94_mode
        mode_dic = dict([(i,a) for i, a in zip(i94_mode.Mode_id.astype('string'), i94_mode.Mode)])
        # setup drop column
        drop_col = ['depdate', 'count', 'occup', 'entdepa', 'entdepd', 'entdepu', 'matflag', 'biryear', \
                    'insnum','visapost', 'fltno', 'admnum', 'insnum', 'dtaddto', 'arrdate', 'dtadfile']
        user_func = udf(lambda x: port_state_dic.get(x))
        visa_func = udf(lambda x: visa_dic.get(x))
        mode_func = udf(lambda x: mode_dic.get(x))
        
        # drop columns
        df_immigration = df_immigration.withColumn('i94visa',df_immigration['i94visa'].cast("float").alias('i94visa')) \
                                        .withColumn('i94mode',df_immigration['i94mode'].cast('int').cast("string").alias('i94mode'))
  
        
        newdf = df_immigration.drop(*drop_col) \
                            .withColumn('i94addr', F.when((F.col('i94addr').isNull()), \
                                                            user_func(df_immigration.i94port)) \
                                                    .otherwise(F.col('i94addr'))) \
                            .withColumn('i94visa', F.when((F.col('i94visa').isNull()), \
                                                            F.col('i94visa')) \
                                                    .otherwise(visa_func(df_immigration.i94visa))) \
                            .withColumn('i94mode', F.when((F.col('i94mode').isNull()), \
                                                            F.col('i94mode')) \
                                                    .otherwise(mode_func(df_immigration.i94mode)))
               
        # display(newdf.select([count(when(col(c).isNull(), c)).alias(c) for c in newdf.columns]).toPandas())
        # replace the null value and cast the columns in integer
        # int_col = ['cicid', 'i94yr', 'i94mon','i94cit', 'i94res', 'i94mode', 'i94bir', 'i94visa']
        null_int = {'cicid': -1, 'i94yr': -1, 'i94mon': -1,'i94cit': 239, 'i94res': 239, 'i94bir': -1}
        for k in null_int:
                newdf = newdf.withColumn(k, F.when((F.col(k).isNull()), null_int[k])
                            .otherwise(F.col(k).cast("int")))

        # replace the null value for the string
        # str_cols = ['i94addr', 'i94port', 'gender', 'airline', 'visatype', 'i94mode', 'i94visa']
        null_str = {'i94addr': '99', 'i94port': '999', 'gender': 'U', 'airline': 'unknown', 'i94mode': 'unknown', 'visatype': '99', 'i94visa': 'unknown'}
        for k in null_str:
                newdf = newdf.withColumn(k, F.when((F.col(k).isNull()), null_str[k])
                                        .otherwise(F.col(k)))

        df_immigration_clean = (newdf.withColumnRenamed("cicid", "id_i94") \
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
                .withColumn('arr_reason', df_immigration_clean.arr_reason.cast('string')) \
                .withColumn('arri_mode', df_immigration_clean.arri_mode.cast('string'))\
                .withColumn('country_res_num', df_immigration_clean.country_res_num.cast('int')) \
                .withColumn('country_born_num', df_immigration_clean.country_born_num.cast('int')) \
                .withColumn('age', df_immigration_clean.age.cast('int')) \
                .dropDuplicates() \
                .filter(df_immigration_clean.arri_mode.isNotNull()) \
                .fillna('99', subset=['state_id_arrival']) \
                .fillna('unknown', subset=['airline']) \
                .fillna('U', subset=['gender']) \
                .fillna(-1, subset=['age'])\
                .filter('arr_reason == "Student"')

        print('***** Make df_immigration_clean processing ')
        df_immigration_clean.printSchema()
        df_immigration_clean.show(2)        
    except Exception as e:
        print("Unexpected error: %s" % e)
    else:
        return(df_immigration_clean)

def clean_temperature(spark, input_data):
    """
    Clean and format dataframe df_temperature
    """
    try:
        df_temperature = spark.read.option("header","true").option("recursiveFileLookup","true").parquet(input_data+'GlobalLandTemperaturesByCity')
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
    except Exception as e:
        print("Unexpected error: %s" % e)
    else:
        return(df_clean_temperature)

def  clean_airport_code(spark, input_data):
    """
    clean dataframe df_airport_code and return a dataframe
    """
    try:
        #read file
        df_airport_code = spark.read.option("header","true").option("recursiveFileLookup","true").parquet(input_data+'airport-codes_csv')
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
    except Exception as e:
        print("Unexpected error: %s" % e)
    else:
        return(df_clean_airport_code)

def clean_global_airports(spark, input_data):
    """
    clean dataframe df_global_airports and return a dataframe
    """
    try:
        #read file
        df_global_airports = spark.read.option("header","true").csv(input_data+'airports-extended.csv')
        drop_cols = ["icao","type", "latitude", "longitude", "altitude", "timezone", "dst", "tz_timezone", "data_source"]
        newdf = df_global_airports.filter(df_global_airports.type.isin('airport', 'unknown')) \
                            .drop(*drop_cols)

        df_clean_global_airports = newdf.select(F.col("airport_ID").alias("airport_id").cast("int"), \
                                                F.col("name").alias("airport_name"), \
                                                F.col("city").alias("city_name"), \
                                                F.col("country").alias("country_name"), \
                                                F.col("iata").alias("iata_code")) \
                                        .dropDuplicates()  \
                                        .fillna("unknown", subset=['city_name',"iata_code"])  
        print('***** Make df_clean_global_airports processing ')
        df_clean_global_airports.printSchema()
        df_clean_global_airports.show(2)
    except Exception as e:
        print("Unexpected error: %s" % e)
    else:
        return(df_clean_global_airports)

def clean_iso_country(spark, input_data):
    """
    clean dataframe df_iso_country and return a dataframe
    """
    try:
        #read file
        df_iso_country = spark.read.option("header","true").csv(input_data+'wikipedia-iso-country-codes.csv')
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
    except Exception as e:
        print("Unexpected error: %s" % e)
    else:
        return(df_clean_iso_country)

def clean_demograph(spark, input_data): 
    """
    clean dataframe df_demograph and return a dataframe
    """
    try:
        #read file
        df_demograph = spark.read.option("header","true").option("recursiveFileLookup","true").parquet(input_data+'us-cities-demographics')
        drop_cols = ["Number_of_Veterans"]
        newdf = df_demograph.drop(*drop_cols) \
                    .select(F.col("city").alias("city_name"), \
                            F.col("state").alias("state_name"), \
                            F.col("median_age"), \
                            F.col("male_population"), \
                            F.col("female_population"), \
                            F.col("total_population"), \
                            F.col("foreign-born"), \
                            F.col("state_code").alias("state_id"), \
                            F.col("race").alias("ethnic"), \
                            F.col("count"))
        df_clean_demograph = newdf.groupBy("state_name", "state_id", "city_name", "median_age", "male_population", "female_population", "ethnic") \
                                .agg(F.avg("count").cast('int').alias("ethnic_count")) \
                                .orderBy("state_name", "city_name", "ethnic") \
                                .dropDuplicates() \
                                .fillna(-1, subset=['male_population','female_population'])
        print('***** Make df_clean_demograph processing ')
        df_clean_demograph.printSchema()
        df_clean_demograph.show(2)
    except Exception as e:
        print("Unexpected error: %s" % e)
    else:
        return(df_clean_demograph)

def clean_indicator_dev(spark, input_data):
    """
    clean dataframe df_indicator_dev and return a dataframe
    """
    try:
        #read file
        df_indicator_dev = spark.read.option("header","true").option("recursiveFileLookup","true").parquet(input_data+'WDIData')
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
            F.when( F.lower(F.col('indicator_name')).rlike('|'.join(demography)), F.lit('demography').cast('string')) \
            .when( F.lower(F.col('indicator_name')).rlike('|'.join(food)), F.lit('food').cast('string')) \
            .when( F.lower(F.col('indicator_name')).rlike('|'.join(trade)), F.lit('trade').cast('string')) \
            .when( F.lower(F.col('indicator_name')).rlike('|'.join(health)), F.lit('health').cast('string')) \
            .when( F.lower(F.col('indicator_name')).rlike('|'.join(economy)), F.lit('economy').cast('string')) \
            .when( F.lower(F.col('indicator_name')).rlike('|'.join(energy)), F.lit('energy').cast('string')) \
            .when( F.lower(F.col('indicator_name')).rlike('|'.join(education)), F.lit('education').cast('string')) \
            .when( F.lower(F.col('indicator_name')).rlike('|'.join(employment)), F.lit('employment').cast('string')) \
            .when( F.lower(F.col('indicator_name')).rlike('|'.join(rural)), F.lit('rural').cast('string')) \
            .when( F.lower(F.col('indicator_name')).rlike('|'.join(urban)), F.lit('urban').cast('string')))  
        # make aggregation 
        newdf = newdf.groupBy('country_name', 'country_code', 'indicator_group') \
                .agg(F.avg('indic_2015')).alias('avg_2015') \
                .orderBy('country_name', 'indicator_group') \
                .where(F.col('indicator_group').isNotNull())
        df_clean_indicator_dev = newdf \
                            .select(F.col('country_name'), \
                                    F.col('country_code'), \
                            'indicator_group', \
                            F.round(F.col('avg(indic_2015)'), 2).alias('avg_2015'))
        print('***** Make df_clean_indicator_dev processing ')
        df_clean_indicator_dev.printSchema()
        df_clean_indicator_dev.show(2)
    except Exception as e:
        print("Unexpected error: %s" % e)
    else:
        return(df_clean_indicator_dev)
