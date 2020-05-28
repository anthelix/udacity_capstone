"""
SCRIPT ETL:  LOAD S3 AND WRITE IN PARQUET FILES
Choose in the main():
    input_data = "s3a://udacity-dend/"
    input_data = ""

    output_data = "./output/" to run locally
    output_data = "s3a://<YOUR BUCKET>" 
"""




def main():
    # input_data = "s3a://udacity-dend/"            # Data from Udacity
    input_data = ""                               # Run locally

    # output_data = "s3a://dend-paris/sparkify/"    # Anthelix bucket(me)
    # output-data = "s3a://<YOUR_BUCKET>/"          # Visitor bucket
    output_data = "./output/"                       # Run localy

    print("\n")
    print("...Get AWS keys...")
    get_credentials()
    print("\n")
    print("...Create a Spark session...")
    spark = create_spark_session()
    print("\n")
    print("...Create dataframe for Song Data...")
    df_song = create_song_data(spark, input_data)
    print("\n")
    print("...Create dataframe for Log Data...")
    df_log = create_log_data(spark, input_data)
    print("\n")
    print("...Process song data...")
    songs_table, artists_table = process_song_data(df_song, output_data)
    print("\n")
    print("...Process log data...")
    process_log_data(df_log, df_song, output_data)

if __name__ == "__main__":
    main()