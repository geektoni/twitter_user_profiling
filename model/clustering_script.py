#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Clustering Spark Script.

Possible way to clustering: k-means and bisecting k-means.

Usage:
	clustering_script.py <algorithm> <dataset_path> <output_path> [--c=<cluster_number>] [--i=<max_iter>] [--find-k] [--verbose] [--aws] [--aws-token=<aws_token>] [--aws-secret=<aws_secret>] [--app-name=<app_name>]

	<algorithm>				The name of the clustering algorithm we want to use (kmeans, LDA, GMM, B-kmeans);
	<dataset_path>			The path to the dataset we want to use;
	<output_path>			Output destination for predictions
	--c=<cluster_number>	Fix the number of clusters to specific number (do not work with all the algos);
	--i=<max_iter>			Max number of iterations
	--find-k				Automatically find the best k value.
	--verbose				Verbose mode. Print more information to screen.
	--aws					Enable Amazon S3 search for the dataset (urls as s3a://...)
	--app-name				Specify the Spark's app name.
	-h, --help				Print this help message.
"""
import os

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

from docopt import docopt

import helpers


# FIXME Ubuntu 16.04
# Remember to export the two local variables:
# export PYSPARK_PYTHON="python3"
# export PYSPARK_DRIVER_PYTHON="python3"
# Otherwise it won't work (because of a conflict between the driver's python version
# and the worker's python version)

if __name__ == "__main__":

	# Parse the command line
	arguments= docopt(__doc__)

	# Get the options in a more usable fashion.
	algorithm_type = arguments["<algorithm>"]
	data_path = arguments["<dataset_path>"]
	output_path = arguments["<output_path>"]
	verbose = arguments["--verbose"]
	max_clusters = arguments["--c"]
	max_iter = arguments["--i"]
	app_name = arguments["--app-name"] if arguments["--app-name"] else "twitter-clustering"

	# We use directly the SparkSession here instead of SparkConf and SparkContext,
	# since now the SparkSession is the entry point for all functionatilies of pyspark.
	# See stackoverflow.com/questions/43802809/difference-between-sparkcontext-javasparkcontext-sqlcontext-sparksession
	conf = SparkConf().setAppName(app_name)
	conf = (conf.set('spark.executor.memory', '20G')
						.set('spark.driver.memory', '20G')
						.set('spark.driver.maxResultSize', '10G').set('spark.executor.cores', 5).set('spark.executor.instances', 4).set('spark.default.parallelism', 20))
	spark = SparkSession.builder.config(conf=conf).getOrCreate()


	# Get the dataset.
	if arguments["--aws"]:
		aws_token = arguments["--aws-token"] if arguments["--aws-token"] else os.environ["ACCESS_TOKEN"]
		aws_secret = arguments["--aws-secret"] if arguments["--aws-secret"] else os.environ["ACCESS_SECRET"]
		spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key",  aws_token)
		spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret)
		spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-west-2.amazonaws.com")
	dataset = spark.read.parquet(data_path)

	# Get the correct clustering algorithm based on the string passed by
	# the user.
	try:

		def experiment(_max_clusters):
			helpers.print_verbose("[*] Getting the correct algorithm", verbose)
			algorithm = helpers.return_correct_clustering_algorithm(algorithm_type, _max_clusters, max_iter)

			helpers.print_verbose("[*] Fitting the clustering algorithm {}".format(algorithm_type))
			model = algorithm.fit(dataset)

			helpers.print_verbose("[*] Transforming the dataset.")
			predictions = model.transform(dataset)

			return model, predictions

		# Decide if we want to find K automatically or not
		if arguments["--find-k"]:
			max_clusters, predictions, sil, wss = helpers.repeat_experiment(10, 100, 10, experiment)

			print(sil)
			print(wss)
			print(max_clusters)
		else:
			model, predictions = experiment(max_clusters)
			predictions.write.parquet(output_path+"/predictions")

		# Save more informations to disk.
		if arguments["--verbose"]:
			helpers.get_information_from_model(predictions, "filtered_words_2", max_clusters, algorithm_type, output_path)

	except Exception as error:
		print(error)

	# Stop the Spark session
	spark.stop()
