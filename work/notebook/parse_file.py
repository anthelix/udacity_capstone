#!/usr/bin/python3

#I94_SAS_Labels_Description.SAS
#def SAS_parser(file_parse, item, columns):
import re
import io
import sys
import pandas as pd

def parse_file(path, file, key):
    """
    fonction to parse file and create parquet file
    """
    output_parquet = '../../output/'
    path_file = path + file
    
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
        

    if key is "i94port":
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

                
                
    if key is "i94cit_i94res":
        columns = ["Country_id", "Country"]
        swap = sas_dict[key]
        for x in swap:
            #x[0] = int(x[0])
            if "mexico" in x[1]:
                x[1] = "mexico"        
        
    if key is "i94mode":
        columns = ["Mode_id", "Mode"]
        #swap = sas_dict[key]
        #for x in swap:
        #    x[0] = int(x[0])
            
    if key is "i94addr":
        columns = ["State_id", "State"]
        
    if key is "i94visa":
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

if __name__ == '__main__':


    path = sys.argv[1]
    file = sys.argv[2]
    print(f'Running "{path+file}"')
    print(' ')

    # make i94port.parquet
    key = "i94port"
    nb = parse_file(path, file, key)
    print(f'There are {nb} rows in {key}.parquet')

    # make i94visa.csv
    key = "i94visa"
    nb =parse_file(path, file, key)
    print(f'There are {nb} rows in {key}.parquet')

    # make i94addr.csv
    key = "i94addr"
    nb =parse_file(path, file, key)
    print(f'There are {nb} rows in {key}.parquet')

    # make i94cit_i94res.csv
    key = "i94cit_i94res"
    nb =parse_file(path, file, key)
    print(f'There are {nb} rows in {key}.parquet')

    # make i94mode.csv
    key = "i94mode"
    nb =parse_file(path, file, key)
    print(f'There are {nb} rows in {key}.parquet')
    print(' ')
    print('***** Make i94 labels files is done!')