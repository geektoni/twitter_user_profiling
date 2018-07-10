#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Twitter Miner

Script to mine tweets from Twitter API and then upload them to an AWS S3 bucket.
To work with the Twitter API you need to set these enviromental variables:
* CONSUMER_KEY, CONSUMER_SECRET;
* ACCESS_TOKEN, ACCESS_SECRET;
Those information can be requested from the Twitter Developer website.

Usage:
	twitter_miner.py [--k=<keywords>] [--l=<languages>] [--max=<max_tweets>] [--verbose] [--no-upload]

	--k=<keywords>     Filter tweets based on keywords.
	--l=<languages>    Filter tweets based on language.
	--max=<max_tweets> Set the maximum number of tweets we want to mine.
	--verbose          Verbose behaviour.
	--no-upload        Do not upload the dataset files to Amazon AWS (keep them on disk).
	-h, --help         Print this help message.
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
if not arguments["--no-upload"]:
	print_verbose("[*] Logging into Amazon S3")
	aws_s3 = boto3.client('s3')
	bucket_name = "twitter100days"
else:
	print_verbose("[*] Skipping logging into Amazon S3")

# Custom listener to filter the Twitter stream
class TwitterFilter(StreamListener):

	def __init__(self):
		start_time = time.strftime("%Y%m%d-%H%M%S")
		self.filename = "twitter-" + start_time
		self.filename_info = "twitter-" + start_time + "_info.csv"
		self.tweets=0
		super(TwitterFilter, self).__init__(StreamListener)


	# Custom method to check file size and upload it to amazon s3
	def check_size(self):
		size = os.path.getsize(self.filename)
		# Create a file and upload it only if we reach the dimension of 50MB
		# or if we reach the maximum number of tweets for that file.
		if size >= 52428800 or (max_tweets != -1 and self.tweets >= max_tweets):
			end_time = time.strftime("%H%M%S")
			final_filename = self.filename + "-" + end_time + ".json.gz"
			if not arguments["--no-upload"]:
				print_verbose("[*] Uploading the datafile to Amazon S3.")
				aws_s3.upload_file(self.filename, bucket_name,  final_filename)
			print_verbose("[*] Write information to file.")
			self.write_file_info(final_filename, self.tweets, size)
			print_verbose("[*] Generate new filename.")
			start_time = time.strftime("%Y%m%d-%H%M%S")
			self.tweets=0
			if not arguments["--no-upload"]:
				os.remove(self.filename)
			return "twitter-" + start_time
		return self.filename

	# Custom method to save informations about the number of tweets present in
	# each of the files
	def write_file_info(filename, tweet_number, memory):
		with open(self.filename_info, "w+") as file_info:
			file_info.write("%s,%i,%i" % filename, tweet_number, memory)

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
