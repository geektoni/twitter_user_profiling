#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Clustering Spark Script

This is an alpha version. Please use this script from the model directory, otherwise
it won't find the data.csv file which location is hard-wired into the code.

Possible way to clustering:
- GMM (Gaussian Mixture Model): For high-dimensional data (with many features), this algorithm may perform poorly.
This is due to high-dimensional data (a) making it difficult to cluster at all
(based on statistical/theoretical arguments) and (b) numerical issues with Gaussian distributions.
- LDA (Latent Dirichlet Allocation): topic model designed for text documents.

Usage:
	clustering_script.py <algorithm> <dataset_path> [--c=<cluster_number>] [--i=<max_iter>] [--find-k] [--verbose] [--custom-hadoop] [--aws] [--aws-token=<aws_token>] [--aws-secret=<aws_secret>]

	<algorithm>				The name of the clustering algorithm we want to use (kmeans, LDA, GMM, B-kmeans);
	<dataset_path>			The path to the dataset we want to use;
	--c=<cluster_number>	Fix the number of clusters to specific number (do not work with all the algos);
	--i=<max_iter>			Max number of iterations
	--verbose				Verbose mode. Print more information to screen.
	--custom-hadoop			Specify custom location form hadoop's AWS libraries.
	--aws					Enable Amazon S3 search for the dataset (urls as s3a://...)
	-h, --help				Print this help message.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

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
	verbose = arguments["--verbose"]
	max_clusters = arguments["--c"]
	max_iter = arguments["--i"]

	# FIXME
	# Options to work correctly with hadoop and AWS (this will be removed soon)
	if arguments["--custom-hadoop"]:
		os.environ['PYSPARK_SUBMIT_ARGS'] = "--jars=/opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-1.7.4.jar," \
									 "/opt/hadoop/share/hadoop/tools/lib/hadoop-aws-2.7.7.jar" \
									 " pyspark-shell"


	# We use directly the SparkSession here instead of SparkConf and SparkContext,
	# since now the SparkSession is the entrypoint for all functionatilies of pyspark.
	# The Master will be set by the spark-submit command.
	# See stackoverflow.com/questions/43802809/difference-between-sparkcontext-javasparkcontext-sqlcontext-sparksession
	spark = SparkSession \
			.builder \
			.appName("twitter-clustering") \
			.getOrCreate()

	# TODO:
	# Get the dataset.
	# For now we are using a custom  method to generate
	# a sample dataset on the fly, later on we will give the user the ability to
	# select which dataset to use (dataset = spark.read.csv(arguments["<dataset_path>"]))
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
			algorithm = helpers.return_correct_clustering_algorithm(algorithm_type, max_clusters, max_iter)

			helpers.print_verbose("[*] Fitting the clustering algorithm {}".format(algorithm_type))
			model = algorithm.fit(dataset)

			helpers.print_verbose("[*] Transforming the dataset.")
			predictions = model.transform(dataset)

			return predictions

		# Decide if we want to find K automatically or not
		if arguments["--find-k"]:
			max_clusters, predictions = helpers.repeat_experiment(10, 100, 10, experiment)
			print(max_clusters)
		else:
			predictions = experiment(max_clusters)

		# Save more informations to disk.
		if arguments["--verbose"]:
			helpers.get_information_from_model(predictions, "filtered_words_2", max_clusters, algorithm_type)

	except Exception as error:
		print(error)

	# Stop the Spark session
	spark.stop()
