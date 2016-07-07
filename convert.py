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
palette = [Color.red, Color.green, Color.yellow, Color.light_purple, Color.purple, Color.cyan, Color.light_gray, Color.black]

def process_df(global_df, fname, start, end, div):
    color = palette[threading.get_ident() % len(palette)]
    # Function that gets run per thread
    print(color("Thread-%d: Processing %d to %d with %d" % (threading.get_ident(), start, end, div)))
    # Loop data sets to create smaller datasets
    directory = 'twitter_sentiment_data/'

    part = (start // div) + 1
    print(color('Thread-%d: Starting with part %d') % (threading.get_ident(), part))
    while end > start:
        print(color('Thread-%d: Converting data set items %d~%d' %
                    (threading.get_ident(), start, start+div)))
        df = convert_data(global_df[start:start+div])
        f = directory + fname + '.%d.%d.hdf' % (part, len(df))
        print(color('Thread-%d: Saving dataset %s') % (threading.get_ident(), f))
        df.to_csv(path_or_buf=f, encoding='utf-8')
        # Post
        part+=1
        start+=div
    

def formatted_tweet(tweet):
    return url_regex.sub('URL', user_regex.sub('USER', tweet)).split()


def convert_data(df):
    color = palette[threading.get_ident() % len(palette)]
    lower = np.vectorize(lambda x: x.lower()) # Vectorized function to capitalize string

    print(color("Thread-%d: Searching for all words in database" % (threading.get_ident())))
    words       = set([word for tweet in lower(df['text'])
                       for word in formatted_tweet(tweet)])       # Obtain all words

    # Construct headers to be used, along with a map of the word to the index
    print(color("Thread-%d: Creating new headers and processing structure" % (threading.get_ident())))
    new_headers = list(words)
    new_headers.append('POLARITY')
    mapper = {k: n for n, k in enumerate(new_headers)} # Map word to index
    print(color("Thread-%d: Detected %d headers" % (threading.get_ident(), len(new_headers))))

    # Preallocate memory for the amount of data required
    print(color("Thread-%d: Preallocating memory for dataframe" % (threading.get_ident())))
    new_dataframe = pd.DataFrame(index=np.arange(0, len(df)), columns=new_headers)
    print(color("Thread-%d: Processing data..." % (threading.get_ident())))
    for n, data in enumerate(zip(lower(df['text']), df['polarity'])):
        tweet, polarity = data
        if n % 100 == 0:
            print(color("Thread-%d: %10.2f\% done." % (threading.get_ident(), n / len(tweet) * 100)))

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
    training_data = training_data[np.random.permutation(len(training_data))]
    training_data.reset_index(drop=True)
    print(training_data)

    # test_data = pd.pandas.read_csv(testing_dir,
    #                                names=row_headers,
    #                                usecols=[0, 4, 5], encoding='ISO-8859-1')

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
    # Training data is already taken care of
    # new_testing  = convert_data(test_data)

    # # Save to disk
    # new_testing.to_csv(path_or_buf='twitter_sentiment_data/testing_data.hdf', encoding='utf-8')

    for t in threads:
        t.join()

    print("Finished Converting Test Data")


if __name__ == '__main__':
    main()
