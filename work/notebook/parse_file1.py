#!/usr/bin/python3

import pandas as pd
import sys
import re
import os

def parse_file(input_data, file, key):
    """
    fonction to parse file and create parquet file
    """
   
    output_parquet = '../data/'
    path_file = input_data + file
    

    #file_parse = 'I94_SAS_Labels_Descriptions.SAS'
    with open(path_file, 'r') as f:
        file = f.read()
    sas_dict={}
    key_name = ''

    for line in file.split("\n"):
        line = re.sub(r"\s+", " ", line)
        if '/* I94' in line :         
            line = line.strip('/* ')
            key_name = line.split('-')[0].replace("&", "_").replace(" ", "").strip(" ").lower() 
            sas_dict[key_name] = []
        elif '=' in line and key_name != '' :
            #line_trans = re.sub("([A-Z]*?),(\s*?[A-Z]{2}\s)","\\1=\\2", line)
            #print(line_trans)
            sas_dict[key_name].append([item.strip(' ').strip(" ';") for item in line.split('=')])
        
    if key == "i94port":
        #pattern = r'[^()]*\s*\([^()]*\)'
        columns = ["Port_id", "Port_city", "State_id"]
        swap = sas_dict[key]          
        sas_dict[key] = []
        for x in swap:           
            if "," in x[1]:
                mylist=[]
                a = x[1].rsplit(",", 1)
                b = a[0]
                c = a[1].strip()
                mylist.extend([x[0], b, c])
                sas_dict[key].append(item for item in mylist)
    if key == "i94cit_i94res":
        columns = ["Country_id", "Country"]
        swap = sas_dict[key]
        for x in swap:
            #x[0] = int(x[0])
            if "mexico" in x[1]:
                x[1] = "mexico"        
    if key == "i94mode":
        columns = ["Mode_id", "Mode"]
        #swap = sas_dict[key]
        #for x in swap:
        #    x[0] = int(x[0])
    if key == "i94addr":
        columns = ["State_id", "State"]
    if key == "i94visa":
        columns = ["Code_visa", "Visa"]
        #swap = sas_dict[key]
        #for x in swap:
        #    x[0] = int(x[0])
            
    df = ""                  
    if key in sas_dict.keys():
        if len(sas_dict[key]) > 0:
            df = pd.DataFrame(sas_dict[key], columns = columns)
            df.sort_values(df.columns[0], inplace=True)
        #with io.open(f"../../data/{key}.csv", "w") as f:
        #    df.to_csv(f, index=False)
        df.to_parquet(f'{output_parquet}{key}.parquet')
        
        
    return(len(sas_dict[key]))