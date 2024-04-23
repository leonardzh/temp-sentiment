#!/bin/bash

# Directory containing CSV files
DIRECTORY=./nldas_weather

# Output file
OUTPUT_FILE=nldas2.csv

# Check if the output file already exists and remove it to start fresh
if [ -f $OUTPUT_FILE ]; then
    rm $OUTPUT_FILE
fi

# Variable to keep track of the first file
FIRST_FILE=1

# Loop through all CSV files in the directory
for FILE in $DIRECTORY/*.csv; do
    if [ $FIRST_FILE -eq 1 ]; then
        # If it's the first file, include the header
        cat $FILE > $OUTPUT_FILE
        FIRST_FILE=0
    else
        # If it's not the first file, skip the header (assuming header is 1 line)
        tail -n +2 $FILE >> $OUTPUT_FILE
    fi
done

echo "All CSV files have been merged into $OUTPUT_FILE."
