from __future__ import division
from urlparse import urlparse
from flask import Flask, render_template
from collections import Iterable
from flask import make_response, request, current_app
from functools import update_wrapper
from directory import directory
import fdb
import json
import os
import colorsys
import datetime
import shapefile

# Flask setup
app = Flask(__name__)
app.debug = True

# FoundationDB setup
fdb.api_version(100)
db = fdb.open()
twitter_mood = directory.create_or_open(db, ('twitter_mood'))
tweet_by_state = twitter_mood['tweet_state']
tweet_by_time = twitter_mood['tweet_time']
tweet = twitter_mood['tweet']
statistic = twitter_mood['statistic']

sf = shapefile.Reader("states.shp")

# Decorator to allow crossdomain access to functions
def crossdomain(origin=None, methods=None, headers=None,
                max_age=21600, attach_to_all=True,
                automatic_options=True):
    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))
    if headers is not None and not isinstance(headers, basestring):
        headers = ', '.join(x.upper() for x in headers)
    if not isinstance(origin, basestring):
        origin = ', '.join(origin)
    if isinstance(max_age, datetime.timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        if methods is not None:
            return methods

        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers
            h['Content-Type'] = "application/json"
            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            return resp

        f.provide_automatic_options = False
        return update_wrapper(wrapped_function, f)
    return decorator

def hue2hex(hue):
	orgb = colorsys.hsv_to_rgb(hue, 1, 1)
	rgb = []
	for c in orgb:
		test = int(c * 256)
		if test == 256:
			test = test - 1
		rgb.append(test)
	hx = '#%02x%02x%02x' % (rgb[0], rgb[1], rgb[2])
	return hx

@fdb.transactional
def get_state_sentiment(tr, state):
	state_tweets = [float(v) for k, v in tr[tweet_by_state.range((state,))]]
	avg = sum(state_tweets)/len(state_tweets)
	variances = [ (i - avg) ** 2 for i in state_tweets]
	std = (sum(variances)/len(variances))**(1/2)
	return (avg, len(state_tweets), std)

@fdb.transactional
def get_running_average(tr):
	all_stats = [float(v) for k, v in tr[statistic.range(('avg',))]]
	avg = sum(all_stats)/len(all_stats)
	return avg

def calculateMood():
	mood = dict()
	avgStat = get_running_average(db)

	for st in sf.shapeRecords():
		state = st.record[31]
		mood[state] = { }
		sentiment, count, std = get_state_sentiment(db, state)
		mood[state]['sentiment'] = sentiment
		mood[state]['count'] = count
		mood[state]['std'] = std
		if std != 0:
			mood[state]['mood_score'] = (mood[state]['sentiment'] - avgStat)/std
			if mood[state]['mood_score'] > .6:
				hue = 120/360
			elif mood[state]['mood_score'] < -.6:
				hue = 0
			else:
				hue = (60 + (100*mood[state]['mood_score']))/360
		else:
			mood[state]['mood_score'] = 0
			hue = 60/360
		mood[state]['color'] = hue2hex(hue)
	
	return json.dumps(mood)

@app.route('/')
@crossdomain(origin='*')
def mood():
	return calculateMood()

@app.route('/example')
def exampleMap():
	return render_template('example.html')
	
