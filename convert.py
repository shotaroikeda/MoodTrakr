import pandas as pd
import numpy as np
import collections
import copy

#######################
# COMMONLY USED REGEX #
#######################
user_regex = re.compile('@\w+')
url_regex = re.compile('(https?:\/\/(?:www\.|(?!www))[^\s\.]+\.[^\s]{2,}|www\.[^\s]+\.[^\s]{2,})')

def formatted_tweet(tweet):
    return url_regex.sub('URL', user_regex.sub('USER', tweet)).split()

def convert_data(df):
    lower = np.vectorize(lambda x: x.lower()) # Vectorized function to capitalize string
    print("Searching for all words in database")
    words       = set([word for tweet in lower(df['text'])
                       for word in formatted_tweet(tweet)])       # Obtain all words

    print("Creating new headers and processing structure")
    # Construct headers to be used, along with a map of the word to the index
    new_headers = list(words)
    new_headers.append('POLARITY')
    mapper = {k: n for n, k in enumerate(new_headers)} # Map word to index
    print("Detected %d headers" % (len(new_headers)))

    new_dataframe = pd.DataFrame([], columns=new_headers)
    for n, tweet, polarity in enumerate(zip(lower(df['text']), df['polarity'])):
        if n % 100 == 0:
            print("Processing %d th element" % (n))

        # Generate zeros
        new_frame = np.zeros(len(new_headers))
        new_frame[mapper['POLARITY']] = polarity

        for word in formatted_tweet(tweet):
            new_frame[mapper(word)] += 1

        # Done with counting words
        new_dataframe = new_dataframe.append(new_frame, ignore_index=True)

    return new_dataframe

def main():
    # Preprocessing data
    training_dir = "twitter_sentiment_data/training.1600000.processed.noemoticon.csv"
    testing_dir = "twitter_sentiment_data/testdata.manual.2009.06.14.csv"
    row_headers = ["polarity", "id", "date", "query", "user", "text"]

    # Data to convert here
    print("Preparing existing data")
    training_data = pd.pandas.read_csv(training_dir,
                                       names=row_headers,
                                       usecols=[0, 4, 5], encoding='ISO-8859-1')

    test_data = pd.pandas.read_csv(testing_dir,
                                   names=row_headers,
                                   usecols=[0, 4, 5], encoding='ISO-8859-1')

    print("Converting data")
    # Convert data
    print("Converting training data")
    new_training = convert_data(training_data)
    print("Converting testing data")
    new_testing  = convert_data(test_data)

    # Save to disk
    new_training.to_hdf('twitter_sentiment_data/training_data.hdf', 'training')
    new_testing.to_hdf('twitter_sentiment_data/testing_data.hdf', 'testing')


if __name__ == '__main__':
    main()
