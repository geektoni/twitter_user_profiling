#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Twitter

Usage:
	twitter_miner.py [--k=<keywords>] [--l=<languages>] [--max=<max_tweets>] [--verbose]

	--k=<keywords>		Filter tweets based on keywords.
	--l=<languages>		Filter tweets based on language.
    --max=<max_tweets>  Set the maximum number of tweets we want to mine.
    --verbose           Verbose behaviour.
	-h, --help			Print this help message.
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

import boto3

# Parse the command line
arguments= docopt(__doc__)
keywords = arguments["--k"]
languages = arguments["--l"]
max_tweets = arguments["--max"] if arguments["--max"] else -1

# Print verbose definition
def print_verbose(text, arg=arguments):
    if arg["--verbose"]:
        print(text)

# Keys for Twitter API
consumer_key = os.environ["CONSUMER_KEY"]
consumer_secret = os.environ["CONSUMER_SECRET"]
access_token = os.environ["ACCESS_TOKEN"]
access_secret = os.environ["ACCESS_SECRET"]

# Get the authentication handler
print_verbose("[*] Logging in into Twitter API")
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

# Get the API handler
api = tp.API(auth)

# Get italian stop-words (this is done to overcome the weakness of the
# free Twitter API which do not permit to filter tweets for language only)
it_stop_words = get_stop_words("it")

# Open Amazon S3 connection to save the dataset
print_verbose("[*] Logging into Amazon S3")
aws_s3 = boto3.client('s3')
bucket_name = "twitter100days"

# Custom listener to filter the Twitter stream
class TwitterFilter(StreamListener):

	def __init__(self):
		start_time = time.strftime("%Y%m%d-%H%M%S")
		self.filename = "twitter-" + start_time
		self.tweets=0
		super(TwitterFilter, self).__init__(StreamListener)


	# Custom method to check file size and upload it to amazon s3
	def check_size(self):
		size = os.path.getsize(self.filename)
        # Create a file and upload it only if we reach the dimension of 50MB
        # or if we reach the maximum number of tweets for that file.
		if size >= 52428800 or (max_tweets != 1 and self.tweets >= max_tweets):
			end_time = time.strftime("%H%M%S")
			print_verbose("[*] Uploading the datafile to Amazon S3.")
			aws_s3.upload_file(self.filename, bucket_name,  self.filename + "-" + end_time + ".json.gz")
			print_verbose("[*] Generate new filename.")
			start_time = time.strftime("%Y%m%d-%H%M%S")
			self.tweets=0
			os.remove(self.filename)
			return "twitter-" + start_time
		return self.filename

	def on_data(self, raw_data):
		try:
			if self.tweets%10 == 0:
				print_verbose("[*] "+str(self.tweets)+": Writing tweets")
			with gzip.GzipFile(self.filename, "ab") as log:
				log.write(raw_data.encode())
			self.tweets += 1
			self.filename = self.check_size()
			return True
		except BaseException as e:
			print(str(e))

	def on_error(self, status_code):
		print("Error "+ str(status_code))
		# If we get rate limited we close the connection
		if status_code == 420:
			return False
		return True


# Get the stream
print_verbose("[*] Get Twitter Stream and start listerning.")
twitter_stream = Stream(auth, TwitterFilter())
twitter_stream.filter(track=it_stop_words, languages=['it'])
