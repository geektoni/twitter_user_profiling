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
	clustering_script.py <algorithm> <dataset_path> [--c=<cluster_number>] [--i=<max_iter>] [--verbose]

	<algorithm>				The name of the clustering algorithm we want to use (kmeans, LDA, GMM, B-kmeans);
	<dataset_path>			The path to the dataset we want to use;
	--c=<cluster_number>	Fix the number of clusters to specific number (do not work with all the algos);
	--i=<max_iter>			Max number of iterations
	--verbose				Verbose mode. Print more information to screen.
	-h, --help				Print this help message.
"""

from pyspark.sql import SparkSession
from pyspark.ml.evaluation import ClusteringEvaluator

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
	verbose = arguments["--verbose"]
	max_clusters = arguments["--c"]
	max_iter = arguments["--i"]

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
	dataset = helpers.get_sample_features(spark)

	# Get the correct clustering algorithm based on the string passed by
	# the user.
	try:

		helpers.print_verbose("[*] Getting the correct algorithm", verbose)
		algorithm = helpers.return_correct_clustering_algorithm(algorithm_type, max_clusters, max_iter)

		helpers.print_verbose("[*] Fitting the clustering algorithm {}".format(algorithm_type))
		model = algorithm.fit(dataset)

		helpers.print_verbose("[*] Transforming the dataset.")
		predictions = model.transform(dataset)

		# Evaluate how good are our clusters' centers.
		evaluator = ClusteringEvaluator()
		silhouette = evaluator.evaluate(predictions)
		print("Silhouette with squared euclidean distance = " + str(silhouette))

		# Shows the cluster centers.
		#centers = model.clusterCenters()
		#print("Cluster Centers: ")
		#for center in centers:
		#	print(center)

	except Exception as error:
		print(error)

	# Stop the Spark session
	spark.stop()