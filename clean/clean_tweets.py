import os
import json
import pandas as pd
import multiprocessing as mp
from multiprocessing import Pool
import sys

os.chdir('/gpfs/data1/oshangp/liuz/sesync/data/tweets')

# high memory usage, no chance for bug
def clean_single_load(f):
    # Define the input and output file names
    input_file = f
    output_file = f'cleaned/{f}'

    # Define the string to be replaced and the replacement string
    string_to_replace = '}{'
    replacement_string = '},{'

    # Open the input and output files
    with open(input_file, 'r') as input_file, open(output_file, 'w') as output_file:
        output_file.write('[')  # Write an opening bracket '[' at the beginning
        chunk = input_file.read()  # Read a chunk of data (adjust the size as needed)
            
        modified_chunk = chunk.replace(string_to_replace, replacement_string)
        
        # Write the modified chunk to the output file
        output_file.write(modified_chunk)
        
        output_file.write(']')  # Write a closing bracket ']' at the end

    # Close the files
    input_file.close()
    output_file.close()
    print(f,"finished")


# low memory usage, may contain bugs
def clean_stream(f):
    # Define the input and output file names
    input_file = f
    output_file = f'cleaned/{f}'

    # Define the string to be replaced and the replacement string
    string_to_replace = '}{'
    replacement_string = '},{'

    # Open the input and output files
    with open(input_file, 'r') as input_file, open(output_file, 'w') as output_file:
        output_file.write('[')  # Write an opening bracket '[' at the beginning
        
        while True:
            
            chunk = input_file.read(1024)  # Read a chunk of data (adjust the size as needed)
            while chunk and chunk[-1] == '}':
                nxt = input_file.read(1)
                if nxt:
                    chunk += nxt
                else:
                    break
            if not chunk:
                break  # End of file
            
            # Perform the string replacement on the chunk
            modified_chunk = chunk.replace(string_to_replace, replacement_string)
            
            # Write the modified chunk to the output file
            output_file.write(modified_chunk)
        
        output_file.write(']')  # Write a closing bracket ']' at the end

    # Close the files
    input_file.close()
    output_file.close()
    print(f,"finished")

if len(sys.argv) != 2:
    print("Usage: python split_tweets.py <year>")
    sys.exit(1)

# Get the filename from the command-line arguments
year = sys.argv[1]

clean_single_load(f"usa_tweets_{year}.json")

#All files in stream mode
fs = [f for f in os.listdir() if os.path.isfile(f)]
fsc = [f for f in os.listdir('cleaned') if os.path.isfile(f)]
jsons = [f for f in fs if 'csv' not in f]
jsonsc = [f for f in fsc if 'csv' not in f]
fs = [f for f in jsons if f not in jsonsc]
fs.sort()
print(fs)
# p = Pool(10)
# p.map(clean_stream, fs)

