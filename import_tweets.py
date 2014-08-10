import twitter
from pymongo import MongoClient

auth=twitter.oauth.OAuth('xx','xx','xx','xx')
twitter_api = twitter.Twitter(auth=auth)

client = MongoClient()
db = client.tweets
dbtweets=db.tweets

def chunks(l, n):
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

from pandas import read_csv
df = read_csv('Tweets_Chemo.csv')

for tweets in chunks(df['url'].str.split('/').apply(lambda x: x[5]).tolist(),100):
    ids=twitter_api.statuses.lookup(_id=','.join(tweets))
    for tweet in ids:
        dbtweets.insert(tweet)
