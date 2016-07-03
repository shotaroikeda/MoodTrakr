import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re
from collections import Counter

print("reading files")
row_headers = ["polarity", "id", "date", "query", "user", "text"]
df = pd.pandas.read_csv("twitter_sentiment_data/training.1600000.processed.noemoticon.csv", names=row_headers, usecols=[0, 4, 5], encoding='ISO-8859-1')

# Delete Irrelevant Headers
# del df['ID']
# del df['Date']
# del df['Query']
# del df['User']

def cap_str(msg):
    return msg.lower()

capital = np.vectorize(cap_str) # Vectorized version of capitalizing all strings

word_dump = {'!': 0, '?': 0, '.': 0}

user_regex = re.compile('@\w+')
url_regex = re.compile('(https?:\/\/(?:www\.|(?!www))[^\s\.]+\.[^\s]{2,}|www\.[^\s]+\.[^\s]{2,})')
punc_regex = re.compile('(\!|\?|\.)')
print("Creating a dump of all the individual words")
for sentence in capital(df['text']):
    sentence = user_regex.sub('USER', sentence)
    sentence = url_regex.sub('URL', sentence)
    cc = Counter(sentence) # Contains puncuation
    sentence = punc_regex.sub(' ', sentence)
    for word in sentence.split():
        # Find puncutation

        if cc.get('!') is not None: word_dump['!'] += cc.get('!')
        if cc.get('?') is not None: word_dump['?'] += cc.get('?')
        if cc.get('.') is not None: word_dump['.'] += cc.get('.')

        try:
            word_dump[word] += 1
        except KeyError:
            word_dump[word] = 1

print("Filtering end results")
word_dump = {k: v for k, v in word_dump.items()}

print("%d words used" % (len(word_dump.items())))

sorted_items = sorted(word_dump.items(), key=lambda x: x[1], reverse=False)
words, counts = zip(*(sorted_items[:20]))

print("plotting graphs")
plt.figure(figsize=(30, 15))
# Plot graphs
plt.bar(range(len(counts)), counts, width=0.8, color="rgbkymc")
plt.xticks(np.arange(len(words)) + 0.5, words, rotation='vertical')
plt.savefig("graph.png", bbox_inches='tight')
