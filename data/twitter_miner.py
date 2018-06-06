#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Twitter

Usage:
	twitter_miner.py [--k=<keywords>] [--l=<languages>]

	--k=<keywords>		Filter tweets based on keywords
	--l=<languages>		Filter tweets based on language
	-h, --help			Print this help message
"""

import os
import time
from docopt import docopt

import tweepy as tp
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

from stop_words import get_stop_words

import gzip
import json

# Parse the command line
arguments= docopt(__doc__)
keywords = arguments["--k"]
languages = arguments["--l"]

# Keys for Twitter API
consumer_key = os.environ["CONSUMER_KEY"]
consumer_secret = os.environ["CONSUMER_SECRET"]
access_token = os.environ["ACCESS_TOKEN"]
access_secret = os.environ["ACCESS_SECRET"]

# Get the authentication handler
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

# Get the API handler
api = tp.API(auth)

# Generate file name
timestr = time.strftime("%Y%m%d-%H%M%S")
filename = "twitter-"+timestr+".json.gz"

# Get italian stop-words (this is done to overcome the weakness of the
# free Twitter API which do not permit to filter tweets for language only)
it_stop_words = get_stop_words("it")

# Custom listener to filter the Twitter stream
class TwitterFilter(StreamListener):

	def on_data(self, raw_data):
		try:
			with gzip.GzipFile(filename, "ab") as log:
				print("[*] Writing tweet")
				log.write(raw_data.encode())
				return True
		except BaseException as e:
			print(str(e))

	def on_error(self, status_code):
		print("Error "+ str(status_code))
		# If we get rate limited we close the connection
		if (status_code == 420):
			return False
		return True

# Get the stream
twitter_stream = Stream(auth, TwitterFilter())
twitter_stream.filter(track=it_stop_words, languages=['it'])



