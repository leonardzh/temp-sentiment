"""
This script assigns sentiment scores to tweets.

Each tweet is assigned a set of sentiment scores based on its content, including afinn, hedonometer, and vader scores.
The sentiment scores indicate the sentiment polarity of the tweet, such as positive, negative, or neutral.

Author: Zheng Liu
Date: 2024-01-29
"""
import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import csv
import glob

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import nltk
import emoji
import preprocessor as p
import re
import math
import os
import json
import pandas as pd
from multiprocessing import Pool,Process
nltk.download('punkt')
p.set_options(p.OPT.URL, p.OPT.EMOJI)
TEXT_FIELD = 'pure_text'
PROJECT_ROOT = '/gpfs/data1/oshangp/liuz/sesync'
READ_PATH = PROJECT_ROOT+'/data/tweets/csv'
SAVE_PATH = PROJECT_ROOT+'/data/processed/sen'
AFINN_PATH = PROJECT_ROOT+'/codes/temp-sentiment/sentiment/AFINN-111.txt'
HEDONO_PATH = PROJECT_ROOT+'/codes/temp-sentiment/sentiment/Data_Set_S1.txt'
str_weather_terms = '''aerovane air airstream altocumulus altostratus anemometer anemometers anticyclone anticyclones \
arctic arid aridity atmosphere atmospheric autumn autumnal balmy baroclinic barometer barometers \
barometric blizzard blizzards blustering blustery blustery breeze breezes breezy brisk calm \
celsius chill chilled chillier chilliest chilly chinook cirrocumulus cirrostratus cirrus climate climates \
cloud cloudburst cloudbursts cloudier cloudiest clouds cloudy cold colder coldest condensation \
contrail contrails cool cooled cooling cools cumulonimbus cumulus cyclone cyclones damp damp \
damper damper dampest dampest degree degrees deluge dew dews dewy doppler downburst \
downbursts downdraft downdrafts downpour downpours dried drier dries driest drizzle drizzled \
drizzles drizzly drought droughts dry dryline fall farenheit flood flooded flooding floods flurries \
flurry fog fogbow fogbows fogged fogging foggy fogs forecast forecasted forecasting forecasts freeze \
freezes freezing frigid frost frostier frostiest frosts frosty froze frozen gale gales galoshes gust \
gusting gusts gusty haboob haboobs hail hailed hailing hails haze hazes hazy heat heated heating \
heats hoarfrost hot hotter hottest humid humidity hurricane hurricanes ice iced ices icing icy \
inclement landspout landspouts lightning lightnings macroburst macrobursts maelstrom mercury \
meteorologic meteorologist meteorologists meteorology microburst microbursts microclimate \
microclimates millibar millibars mist misted mists misty moist moisture monsoon monsoons \
mugginess muggy nexrad nippy NOAA nor’easter nor’easters noreaster noreasters overcast ozone \
parched parching pollen precipitate precipitated precipitates precipitating precipitation psychrometer \
radar rain rainboots rainbow rainbows raincoat raincoats rained rainfall rainier rainiest \
raining rains rainy sandstorm sandstorms scorcher scorching searing shower showering showers \
skiff sleet slicker slickers slush slushy smog smoggier smoggiest smoggy snow snowed snowier \
snowiest snowing snowmageddon snowpocalypse snows snowy spring sprinkle sprinkles sprinkling \
squall squalls squally storm stormed stormier stormiest storming storms stormy stratocumulus \
stratus subtropical summer summery sun sunnier sunniest sunny temperate temperature tempest \
thaw thawed thawing thaws thermometer thunder thundered thundering thunders thunderstorm \
thunderstorms tornadic tornado tornadoes tropical troposphere tsunami turbulent twister twisters \
typhoon typhoons umbrella umbrellas vane warm warmed warming warms warmth waterspout \
waterspouts weather wet wetter wettest wind windchill windchills windier windiest windspeed \
windy winter wintery wintry'''
LST_WEATHER_TERMS = str_weather_terms.split(' ')
DICT_WEATHER_TERMS = {LST_WEATHER_TERMS[i]: 1 for i in range(len(LST_WEATHER_TERMS))}

def CheckWeatherTerm(text):
    '''
    Return 1 or 0 for whether input contains any weather term
    '''
    words = nltk.word_tokenize(text)
    for w in words:
        if w in DICT_WEATHER_TERMS:
            return 1
    return 0

# def keepemoji_clean(text):
#     '''
#     clean text with tweet-preprocessor
#     '''
#     text = emoji.demojize(text)
#     return p.clean(text)
#https://github.com/s/preprocessor/issues/50
class TimeoutException(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutException

def keepemoji_clean(text):
    '''
    clean text with tweet-preprocessor
    '''
    text = emoji.demojize(text)
    import signal
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(2)
    try:
        r = p.clean(text)
    except TimeoutException:
        print(f"Could not handle the {text}")
        r = text
    else:
        signal.alarm(0)

    return r

def afinn_sentiment(text,afinn,pattern_split):
    """
    Returns a float for sentiment strength based on the input text.
    Positive values are positive valence, negative value are negative valence. 
    """
    words = pattern_split.split(text.lower())
    sentiments = list(map(lambda word: afinn.get(word, 0), words))
    leng = len(sentiments)
    if leng > 0:
        # How should you weight the individual word sentiments? 
        # You could do N, sqrt(N) or 1 for example. Here I use sqrt(N)
        sentiment = float(sum(list(sentiments)))/math.sqrt(len(list(sentiments)))
    else:
        sentiment = 0
    return sentiment

def add_afinn(df):
    filenameAFINN = AFINN_PATH
    afinn = dict(map(lambda ws: (ws[0], int(ws[1])), [ 
            ws.strip().split('\t') for ws in open(filenameAFINN) ]))
    pattern_split = re.compile(r"\W+")
    df['afinn'] = df[TEXT_FIELD].map(lambda x:afinn_sentiment(x,afinn,pattern_split))
    return df

def load_scores(filename):
    """Takes a file from the Dodd research paper and returns a dict of
    wordscores. Note this function is tailored to the file provided
    by the Dodd paper. For other sets of word scores, a dict can be
    passed directly to HMeter."""
    doddfile = csv.reader(open(filename, "r"), delimiter='\t')
    for x in range(4):  # strip header info
        next(doddfile)
    return {row[0]: float(row[2]) for row in doddfile}

class HMeter(object):
    """HMeter is the main class to prepare a text sample for scores. It
    expects a list of individual words, such as those provided by 
    nltk.word_tokenize, as wordlist. It expects a dict of words as k and
    floating point wordscores as v for wordscores. deltah allows us to 
    filter out the most neutral words as stop words."""
    def __init__(self, wordlist, wordscores, deltah=0.0):
        self.wordlist = wordlist
        self.wordscores = wordscores
        self.deltah = deltah
    _deltah = None
    @property
    def deltah(self):
        """Deltah determines stop words. The higher deltah the more neutral 
        words are are discarded from the matchlist."""
        return self._deltah
    @deltah.setter
    def deltah(self, deltah):
        """Each time deltah is set we need to regenerate the matchlist."""
        self._deltah = deltah
        # TODO Should probably raise a range error if deltah is nonsensical
        # first we take every word that matches labMT 1.0
        labmtmatches = (word for word in self.wordlist
                        if word in self.wordscores)
        # then we strip out stop words as described by Dodd paper
        self.matchlist = []
        for word in labmtmatches:
            score = self.wordscores[word]
            if score >= 5.0 + self.deltah or score <= 5.0 - self.deltah:
                self.matchlist.append(word)
    def fractional_abundance(self, word):
        """Takes a word and return its fractional abundance within
        self.matchlist"""
        frac_abund = self.matchlist.count(word) / len(self.matchlist)
        return frac_abund
    def word_shift(self, comp):
        """Produces data necessary to create a word shift graph. Returns a list 
        of tuples that contain each word's contribution to happiness score shift 
        between two samples. So for example, assigned to a variable 'output_data'
        output_data[n] represents the data for one word where:
            
        output_data[n][0] the word
        output_data[n][1] the proportional contribution the word gives to overall
                          word shift
        output_data[n][2] The relative abundance of word between the two samples
        output_data[n][3] The word's happiness relative to the refernce sample
        
        Using this data, we can construct word shift graphs as described here:
        http://www.hedonometer.org/shifts.html"""
        # initialize variables for potentially large loop.
        # create our comparison object. self is the reference object.
        tcomp = HMeter(comp, self.deltah)
        # we want a list of all potential words, but only need each word once.
        word_shift_list = set(tcomp.matchlist + self.matchlist)
        output_data = []
        ref_happiness_score = self.happiness_score()
        comp_happiness_score = tcomp.happiness_score()
        happy_diff = comp_happiness_score - ref_happiness_score
        for word in word_shift_list:
            abundance = (tcomp.fractional_abundance(word) -
                         self.fractional_abundance(word))
            happiness_shift = self.wordscores[word] - ref_happiness_score
            paper_score = (happiness_shift * abundance * 100) / happy_diff
            output_data.append((word, paper_score, abundance, happiness_shift))
        # sort words by absolute value of individual word shift
        output_data.sort(key=lambda word: abs(word[1]))
        return output_data
    def happiness_score(self):
        """Takes a list made up of individual words and returns the happiness
        score."""
        happysum = 0
        count = len(self.matchlist)
        for word in self.matchlist:
            happysum += self.wordscores[word]
        if count != 0:  # divide by zero errors are sad.
            return happysum / count
        else:
            pass  # empty lists have no score



def hmeter_sentiment(text,pattern_split,scores):
    """
    Returns a float for sentiment strength based on the input text.
    Positive values are positive valence, negative value are negative valence. 
    """
    words = pattern_split.split(text.lower())
    h = HMeter(words,scores)
    return h.happiness_score()

def add_hedono(df):
    scores = load_scores(HEDONO_PATH)
    pattern_split = re.compile(r"\W+")
    df['hedono'] = df[TEXT_FIELD].map(lambda x:hmeter_sentiment(x,pattern_split,scores))
    return df

def add_vader(df):
    analyzer = SentimentIntensityAnalyzer()
    df['vader'] = df[TEXT_FIELD].map(lambda x:analyzer.polarity_scores(x)['compound'])
    return df
def add_all_sentiment(df):
    '''
    calculate sentiment scores for field TEXT_FIELD
    '''
    df = add_afinn(df)
    df = add_hedono(df)
    df = add_vader(df)
    return df

def sentiment(df):
    df = df[df['pure_text'].notna()]
    df['tweet_created_at_int'] = df['tweet_created_at'].apply(lambda x: json.loads(x.replace("'", '"'))['$date'])
    df = df[['id','pure_text','tweet_created_at_int','fastText_lang','lat','lon']]
    df['clean_text'] = df['pure_text'].map(lambda x:keepemoji_clean(x))
    df['weather_term'] = df['clean_text'].map(lambda x:CheckWeatherTerm(x))
    df = add_all_sentiment(df)
    df = df.drop(columns=['pure_text','clean_text'])
    return df


CPU_NUMBER = 40

def process_file(file):
    # Read the CSV file
    try:
        
        df = pd.read_csv(file,lineterminator='\n',dtype={'id':str,'tweet_created_at':str})
        print(file,df.shape)
        # Apply the sentiment function to the data frame
        df = sentiment(df)
        
        # Save the modified data frame with the same file name in the SAVE_PATH
        save_file = os.path.join(SAVE_PATH, os.path.basename(file))
        df.to_csv(save_file, index=False)
        print(file,'done')
    except Exception as e:
        print(file,e)
if __name__ == '__main__':
    # Get the list of CSV files in the READ_PATH
    donefiles = os.listdir(SAVE_PATH)
    tweetfiles = [os.path.join(READ_PATH, f) for f in os.listdir(READ_PATH) if f.endswith('.csv') and f not in donefiles]
    print(tweetfiles)
    # for file in tweetfiles:
    #     process_file(file)
    #Create a pool of workers
    with Pool(CPU_NUMBER) as pool:
        # Process the files in parallel
        pool.map(process_file, tweetfiles)
    processes = []
    for f in tweetfiles:
        proc = Process(target=process_file, args=(f, ))
        processes.append(proc)
        proc.start()

    for process in processes:
        process.join()

