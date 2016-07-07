import pandas as pd
import numpy as np
import collections
import copy
import re
import ansiterm as Color
import threading

#######################
# COMMONLY USED REGEX #
#######################
user_regex = re.compile('@\w+')
url_regex = re.compile('(https?:\/\/(?:www\.|(?!www))[^\s\.]+\.[^\s]{2,}|www\.[^\s]+\.[^\s]{2,})')

def process_df(global_df, fname, start, end, div):
    # Function that gets run per thread
    print(Color.red("Thread-%d: Processing %d to %d with %d" % (threading.get_ident(), start, end, div)))
    # Loop data sets to create smaller datasets
    directory = 'twitter_sentiment_data/'

    part = (start // div) + 1
    print(Color.yellow('Thread-%d: Starting with part %d') % (threading.get_ident(), part))
    while end > start:
        print(Color.yellow('Thread-%d: Converting data set items %d~%d' %
                           (threading.get_ident(), start, start+div)))
        df = convert_data(global_df[n:n+div])
        f = directory + fname + '.%d.%d.hdf' % (part, len(df))
        print(Color.yellow('Thread-%d: Saving dataset %s') % (threading.get_ident(), f))
        df.to_hdf(f, fname)
        # Post
        part+=1
        start+=div
    

def formatted_tweet(tweet):
    return url_regex.sub('URL', user_regex.sub('USER', tweet)).split()


def convert_data(df):
    lower = np.vectorize(lambda x: x.lower()) # Vectorized function to capitalize string

    print("Searching for all words in database")
    words       = set([word for tweet in lower(df['text'])
                       for word in formatted_tweet(tweet)])       # Obtain all words

    # Construct headers to be used, along with a map of the word to the index
    print("Creating new headers and processing structure")
    new_headers = list(words)
    new_headers.append('POLARITY')
    mapper = {k: n for n, k in enumerate(new_headers)} # Map word to index
    print("Detected %d headers" % (len(new_headers)))

    # Preallocate memory for the amount of data required
    print("Preallocating memory for dataframe")
    new_dataframe = pd.DataFrame(index=np.arange(0, len(df)), columns=new_headers)
    print("Processing data...")
    for n, data in enumerate(zip(lower(df['text']), df['polarity'])):
        tweet, polarity = data
        if n % 100 == 0:
            print("Processing %d th element" % (n))

        # Generate zeros
        new_frame = np.zeros(len(new_headers))
        new_frame[mapper['POLARITY']] = polarity

        for word in formatted_tweet(tweet):
            new_frame[mapper[word]] += 1

        # Done with counting words
        new_dataframe.iloc[n] = new_frame # add to dataframe

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
    threads = []

    start = 0
    end = len(training_data)
    div = 1000

    for i in range(4):
        proc = end // 4
        if i == 3:
            args = (training_data, 'training_data', start, end, div)
            thread = threading.Thread(target=process_df, args=args)
            threads.append(thread)
        else:
            args = (training_data, 'training_data', start, start+proc, div)
            thread = threading.Thread(target=process_df, args=args)
            threads.append(thread)

        threads[i].start()
        start += proc

    print("Converting testing data")
    new_testing  = convert_data(test_data)

    # Save to disk
    new_testing.to_hdf('twitter_sentiment_data/testing_data.hdf', 'testing')

    for t in threads:
        t.join()

    print("Finished Converting Test Data")


if __name__ == '__main__':
    main()
