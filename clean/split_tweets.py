'''
python script to split a json file which consists of a list of 1000 dictionaries into 10 json files, each has 100 dictionaries 
'''

import json
import os
import sys
os.chdir('/gpfs/data1/oshangp/liuz/sesync/data/tweets')
# Input JSON file
if len(sys.argv) != 2:
    print("Usage: python split_tweets.py <year>")
    sys.exit(1)

# Get the filename from the command-line arguments
year = sys.argv[1]
# input_file = f'cleaned/usa_tweets_{year}.json'
input_file = f'usa_tweets_{year}.json'

# Output directory
output_dir = 'trunk'

# Create the output directory if it doesn't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)


# brute-force method to split large json into small chunks
def split_json(input_file):
    # Read the input JSON file
    with open(input_file, 'r') as file:
        data = json.load(file)
    print('file loaded')
    # Split the list of dictionaries into 10 chunks, each with 100 dictionaries
    chunk_size = 1000000
    chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

    print(len(chunks),' trunks')
    # Write each chunk to a separate JSON file
    for i, chunk in enumerate(chunks):
        chunk_filename = os.path.join(output_dir, f'{input_file[8:-5]}_chunk_{i + 1}.json')
        
        with open(chunk_filename, 'w') as chunk_file:
            json.dump(chunk, chunk_file, indent=4)  # Use indent for pretty formatting if needed

    print("Splitting completed.")

#split_json(input_file)


# input_file = f'usa_tweets_{year}.json'

# # Output directory
# output_dir = 'rawcode_chunk'

# # Create the output directory if it doesn't exist
# if not os.path.exists(output_dir):
#     os.makedirs(output_dir)

#slow way to split raw concatenated json to chunks 
def raw_decode(input_file):

    # Open the input file for reading
    with open(input_file, 'r') as file:
        concatenated_json = file.read()

    print('file read')
    # Initialize variables
    parsed_objects = []
    decoder = json.JSONDecoder()
    chunk_size = 1000000
    window = 65536
    chunk_count = 0
    # Loop to parse each JSON object
    pos = 0
    while pos < len(concatenated_json):
        # Use JSONDecoder.raw_decode() to parse the next JSON object
        try:
            obj, end_pos = decoder.raw_decode(concatenated_json[pos:pos+window])
            # Add the parsed object to the list
            parsed_objects.append(obj)
            
            # Remove the parsed JSON object from the concatenated string
            pos += end_pos
        except Exception as e:
            print(pos,len(concatenated_json))
            print(concatenated_json[pos:pos+20])
            break
        if len(parsed_objects) == chunk_size:
            chunk_count += 1
            chunk_filename = os.path.join(output_dir, f'{year}_chunk_{chunk_count}.json')
            
            # Write the 100 parsed objects to the output JSON file
            with open(chunk_filename, 'w') as chunk_file:
                json.dump(parsed_objects, chunk_file)
            print('write to',chunk_filename)
            # Clear the list for the next chunk
            parsed_objects = []
    # If there are remaining objects, write them to the last file
    if parsed_objects:
        chunk_count += 1
        chunk_filename = os.path.join(output_dir, f'{year}_chunk_{chunk_count}.json')
        
        # Write the remaining parsed objects to the output JSON file
        with open(chunk_filename, 'w') as chunk_file:
            json.dump(parsed_objects, chunk_file)
        print('write to',chunk_filename)
raw_decode(input_file)