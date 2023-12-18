# New in 2023

## prerequisite cleaning steps
off-record cleaning is needed is tweets are not completed.

`clean_tweets.py` would convert concatenated JSON to valid json format by wrapping it into list of dicts. This is done by replacing `}{` to `},{`

`split_tweets.py` would split the large json to 1-2GB pieces to leverage the multiple processing of converting json to CSV.

## Convert tweets in json to CSV

`Convert_Tweets_CSV.py` extract geolocation and convert json to data tables in CSV.