from __future__ import division
from twython import Twython
from twython import TwythonStreamer
from collections import Iterable
from textblob import TextBlob
from urlparse import urlparse
from directory import directory
import threading
import json
import shapefile
import fdb
import datetime
import colorsys
import sys
import traceback
import os
import re

# FoundationDB setup
fdb.api_version(100)
db = fdb.open()
twitter_mood = directory.create_or_open(db, ('twitter_mood'))
tweet_by_state = twitter_mood['tweet_state']
tweet_by_time = twitter_mood['tweet_time']
tweet = twitter_mood['tweet']
statistic = twitter_mood['statistic']

# Twitter OAUTH Credentials
APP_KEY = os.environ['APP_KEY']
APP_SECRET = os.environ['APP_SECRET']
OAUTH_TOKEN = os.environ['OAUTH_TOKEN']
OAUTH_TOKEN_SECRET = os.environ['OAUTH_TOKEN_SECRET']

# Parse arguments
save_lag = int(sys.argv[1])
if len(sys.argv) > 2:
	track_word = sys.argv[3]
else:
	track_word = None

# Load shapefile
sf = shapefile.Reader("states.shp")

# Initialize start time
starttime = datetime.datetime.now()
stattime = datetime.datetime.now()

@fdb.transactional
def add_tweet(tr, id, state, sentiment):
	t = datetime.datetime.now()
	tr[tweet_by_time.pack((t.year, t.month, t.day, t.hour, t.minute, id))] = ''
	tr[tweet_by_state.pack((state, id))] = str(sentiment)
	tr[tweet.pack((id, t.year, t.month, t.day, t.hour, t.minute))] = str(state)
	print str(sentiment) + " - " + state

@fdb.transactional
def delete_tweet(tr, id):
	t = tr[tweet.range((id,))]
	for k, v in t:
		keys = fdb.tuple.unpack(k)
		del tr[tweet_by_state.range((v, id))]
		del tr[tweet_by_time.range((keys[1], keys[2], keys[3], keys[4], keys[5], id))]
	del t

def clean_database():
	lag = datetime.datetime.now() - datetime.timedelta(minutes=save_lag)
	old_tweets = db[tweet_by_time.pack(()):tweet_by_time.pack((lag.year, lag.month, lag.day, lag.hour, lag.minute))]
	old_tweet_ids = [fdb.tuple.unpack(k)[5] for k, v in old_tweets]
	for t in old_tweet_ids: delete_tweet(db, t)
	print "Cleaned!"

# Take average sentiment for all tweets currently in db
@fdb.transactional
def take_average(tr):
	all_tweets = [float(v) for k, v in tr[tweet_by_state.range(())]]
	avg = sum(all_tweets)/len(all_tweets)
	t = datetime.datetime.now()
	tr[statistic.pack(('avg', t.year, t.month, t.day, t.hour, t.minute))] = str(avg)
	print 'Average taken!: ' + str(avg)
	print 'Current tweets: ' + str(len(all_tweets))

# Point in polygon
def pip(x,y,poly):
    n = len(poly)
    inside = False
    p1x,p1y = poly[0]
    for i in range(n+1):
        p2x,p2y = poly[i % n]
        if y > min(p1y,p2y):
            if y <= max(p1y,p2y):
                if x <= max(p1x,p2x):
                    if p1y != p2y:
                        xints = (y-p1y)*(p2x-p1x)/(p2y-p1y)+p1x
                    if p1x == p2x or x <= xints:
                        inside = not inside
        p1x,p1y = p2x,p2y
    return inside

# Process tweet to make it more machine readable
def process_tweet(t):
    #Convert to lower case
    t = t.lower()
    #Convert www.* or https?://* to URL
    t = re.sub('((www\.[\s]+)|(https?://[^\s]+))','URL',t)
    #Convert @username to AT_USER
    t = re.sub('@[^\s]+','AT_USER',t)    
    #Remove additional white spaces
    t = re.sub('[\s]+', ' ', t)
    #Replace #word with word
    t = re.sub(r'#([^\s]+)', r'\1', t)
    #trim
    t = t.strip('\'"')
    return t

class MyStreamer(TwythonStreamer):

	def on_success(self, data):
		global starttime
		global stattime
		global db
		if 'coordinates' in data:
			state = None
			try:
				if isinstance(data['coordinates'], Iterable):
					if 'coordinates' in data['coordinates']:
							lat = data['coordinates']['coordinates'][0]
							lon = data['coordinates']['coordinates'][1]
							
							for st in sf.shapeRecords():
								if pip(lat, lon, st.shape.points):
									state = st.record[31]
									break

				elif 'place' in data and isinstance(data['place'], Iterable):
					if data['place']['country_code'] == 'US':
						state = data['place']['full_name'][-2:]

				if ('text' in data) and (state != None):
					text = process_tweet(data['text'])
					analysis = TextBlob(text)
					sentiment = analysis.sentiment.polarity
					id = data['id_str']
					if  sentiment != 0:
						add_tweet(db, id, state, sentiment)
					
				if (datetime.datetime.now() - starttime).total_seconds() > 60:
					thread = threading.Thread(target=clean_database)
					thread.start()
					starttime = datetime.datetime.now()

				if (datetime.datetime.now() - stattime).total_seconds() > 300:
					take_average(db)
					stattime = datetime.datetime.now()

			except Exception as e:
				print e

	def on_error(self, status_code, data):
		print status_code

def stream():
	stream = MyStreamer(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
	if track_word is None:
		stream.statuses.filter(locations='-124.7625,24.5210,-66.9326,49.3845,-171.7911,54.4041,-129.9799,71.3577,-159.8005,18.9161,-154.8074,22.2361')
	else:
		stream.statuses.filter(track=track_word)

stream()
