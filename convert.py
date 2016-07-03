import pandas as pd
import numpy as np
import collections
import copy

def convert_data(df):
    capital     = np.vectorize(lambda x: x.upper()) # Vectorized function to capitalize string
    print("Searching for all words in database")
    words       = [word for tweet in capital(df['text'])
                   for word in tweet.split()]       # Obtain all words
    print("Creating new headers and processing structure")
    new_headers = list(set(words))                  # this is going to be the new headers that will be used
    zeros = [0 for i in new_headers]
    zeroed_dict = collections.OrderedDict(zip(new_headers, zeros))
    zeroed_dict.update({'polarity': 0})

    new_dataframe = pd.DataFrame([], columns=new_headers + ['polarity'])
    for n, tweet, polarity in enumerate(zip(capital(df['text']), df['polarity'])):
        if n % 100 == 0:
            print("Processing %d th element" % (n))
        frame = copy.deepcopy(zeroed_dict)
        frame['polarity'] = polarity
        for word in tweet.split():
            frame[word] += 1

        # Done with counting words
        new_dataframe = new_dataframe.append(frame, ignore_index=True)

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
