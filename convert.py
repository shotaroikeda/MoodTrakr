import pandas as pd
import numpy as np
import collections
import copy
import re
import ansiterm as Color
from multiprocessing import Process
import os

#######################
# COMMONLY USED REGEX #
#######################
user_regex = re.compile('@\w+')
url_regex = re.compile('(https?:\/\/(?:www\.|(?!www))[^\s\.]+\.[^\s]{2,}|www\.[^\s]+\.[^\s]{2,})')
palette = [Color.red, Color.green, Color.yellow, Color.light_purple, Color.purple, Color.cyan, Color.light_gray, Color.black]

def process_df(global_df, fname, start, end, div, color):
    # Function that gets run per thread
    print(color("PID-%d: Processing %d to %d with %d" % (os.getpid(), start, end, div)))
    # Loop data sets to create smaller datasets
    directory = 'twitter_sentiment_data/'

    part = (start // div) + 1
    print(color('PID-%d: Starting with part %d') % (os.getpid(), part))
    while end > start:
        print(color('PID-%d: Converting data set items %d~%d' %
                    (os.getpid(), start, start+div)))
        df = convert_data(global_df[start:start+div], color)
        f = directory + 'fname + .%04d.%04d.csv' % (part, len(df))
        print(color('PID-%d: Saving dataset %s') % (os.getpid(), f))
        df.to_csv(path_or_buf=f, encoding='utf-8')
        # Post
        part+=1
        start+=div
    

def formatted_tweet(tweet):
    return url_regex.sub('URL', user_regex.sub('USER', tweet)).split()


def convert_data(df, color):
    lower = np.vectorize(lambda x: x.lower()) # Vectorized function to capitalize string

    print(color("PID-%d: Searching for all words in database" % (os.getpid())))
    words       = set([word for tweet in lower(df['text'])
                       for word in formatted_tweet(tweet)])       # Obtain all words

    # Construct headers to be used, along with a map of the word to the index
    print(color("PID-%d: Creating new headers and processing structure" % (os.getpid())))
    new_headers = list(words)
    new_headers.append('POLARITY')
    mapper = {k: n for n, k in enumerate(new_headers)} # Map word to index
    print(color("PID-%d: Detected %d headers" % (os.getpid(), len(new_headers))))

    # Preallocate memory for the amount of data required
    print(color("PID-%d: Preallocating memory for dataframe" % (os.getpid())))
    new_dataframe = pd.DataFrame(index=np.arange(0, len(df)), columns=new_headers)
    print(color("PID-%d: Processing data..." % (os.getpid())))
    for n, data in enumerate(zip(lower(df['text']), df['polarity'])):
        tweet, polarity = data
        if n % 100 == 0:
            print(color("PID-%d: %10.2f%% done." % (os.getpid(), (n / len(df['text'])) * 100)))

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

    print("Shuffling data")
    # Shuffle the training data since all the values are aligned
    training_data = training_data.iloc[np.random.permutation(len(training_data))]
    training_data = training_data.reset_index(drop=True)

    test_data = pd.pandas.read_csv(testing_dir,
                                   names=row_headers,
                                   usecols=[0, 4, 5], encoding='ISO-8859-1')

    # Convert data
    print("Converting training data")
    processes = []

    start = 0
    end = len(training_data)
    div = 1000

    for i in range(4):
        proc = end // 4
        if i == 3:
            args = (training_data, 'training_data', start, end, div, palette[i])
            process = Process(target=process_df, args=args)
            processes.append(process)
        else:
            args = (training_data, 'training_data', start, start+proc, div, palette[i])
            process = Process(target=process_df, args=args)
            processes.append(process)

        processes[i].start()
        start += proc

    print("Converting testing data")
    # Training data is already taken care of
    new_testing = convert_data(test_data)

    # Save to disk
    new_testing.to_csv(path_or_buf='twitter_sentiment_data/testing_data.csv', encoding='utf-8')

    for p in processes:
        p.join()

    print("Finished Converting Test Data")


if __name__ == '__main__':
    main()
