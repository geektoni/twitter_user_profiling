#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Classify users script.

Gives the top 10 clusters to which the user belongs.

Usage:
	classify_users.py <dataset_path> <user_id> [--aws] [--aws-token=<aws_token>] [--aws-secret=<aws_secret>] [--app-name=<app_name>]

	<dataset_path>		Location of the clustered dataset;
	<user_id>			Twitter's user id;
	--aws				Enable AWS reading;
	--app-name			Specify Spark's app name.

"""
import os

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from docopt import docopt

if __name__ == "__main__":

	# Parse the command line
	arguments= docopt(__doc__)

	data_path = arguments["<dataset_path>"]
	user_id = int(arguments["<user_id>"])
	app_name = arguments["--app-name"] if arguments["--app-name"] else "twitter-clustering"

	conf = SparkConf().setAppName(app_name)
	conf = (conf.set('spark.executor.memory', '20G')
						.set('spark.driver.memory', '20G')
						.set('spark.driver.maxResultSize', '10G').set('spark.executor.cores', 5).set('spark.executor.instances', 4).set('spark.default.parallelism', 20))
	spark = SparkSession.builder.config(conf=conf).getOrCreate()

	if arguments["--aws"]:
		aws_token = arguments["--aws-token"] if arguments["--aws-token"] else os.environ["ACCESS_TOKEN"]
		aws_secret = arguments["--aws-secret"] if arguments["--aws-secret"] else os.environ["ACCESS_SECRET"]
		spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key",  aws_token)
		spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret)
		spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-west-2.amazonaws.com")
	dataset = spark.read.parquet(data_path)

	# Get the top 10 cluster for the user.
	try:
		dataset.filter("_c1={}".format(user_id)).groupBy("prediction").count().sort("count", ascending=False).show(10)
	except Exception as error:
		print(error)

	# Stop the Spark session
	spark.stop()
