prefix="usa_tweets_"

# Iterate over the files you want to rename
for file in *.csv; do
    # Check if the file exists and is a regular file
    if [ -f "$file" ]; then
        # Construct the new filename with the prefix
        new_name="${prefix}${file}"

        # Rename the file
        mv "$file" "$new_name"
        echo "Renamed '$file' to '$new_name'"
    fi
done