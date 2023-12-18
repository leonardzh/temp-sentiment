import os
import json
import pandas as pd
import multiprocessing as mp
from multiprocessing import Pool

os.chdir('/gpfs/data1/oshangp/liuz/sesync/data/tweets/trunk')

#All files
fs = os.listdir()

jsons = [f for f in fs if 'csv' not in f]
csvs = [f for f in fs if 'json' not in f]

fs = [f for f in jsons if (f[:-5] + '.csv') not in csvs]

fs.sort()
print(fs)

#original 
def convert(f):
    print(f)
    with open(f, 'r') as file:
        d = json.loads(file.read())
        # Add commas between JSON objects and enclose them in square brackets

    
    new = []
    for i in d:
        if 'geo' in i.keys():
            #Flatten geo vars
            i.update({'lat': i['geo']['coordinates'][1]})
            i.update({'lon': i['geo']['coordinates'][0]})
            del i['geo']
        if 'user' in i.keys():
            del i['user']
        new.append(i)
    newdf = pd.DataFrame(new)
    newdf.to_csv(f.replace('json', 'csv'), index=False)

def process_dict(input_dict):
    # Process the input_dict as needed
    # Create a DataFrame from the processed data
    if 'geo' in input_dict.keys():
            #Flatten geo vars
        input_dict.update({'lat': input_dict['geo']['coordinates'][1]})
        input_dict.update({'lon': input_dict['geo']['coordinates'][0]})
        del input_dict['geo']
    if 'user' in input_dict.keys():
        del input_dict['user']
    
    return input_dict

# parallelization over one CSV
def pool_convert(f):
    num_processes = 40  # You can adjust this number
    pool = mp.Pool(processes=num_processes)
    print(f)
    with open(f, 'r') as file:
        d = json.loads(file.read())
        # Add commas between JSON objects and enclose them in square brackets
    print(f,'loaded')
    # Use pool.map to apply the function to each dictionary in parallel
    result_dicts = pool.map(process_dict, d)

    # Close the pool to free up resources
    pool.close()
    pool.join()
    final_df = pd.DataFrame(result_dicts)
    final_df.to_csv(f.replace('json', 'csv'), index=False)

# for f in fs:
#     from datetime import datetime
#     pool_convert(f)
#     print(f,datetime.now())
p = Pool(40)
p.map(convert, fs)
p.close()
p.join()
