#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Local K-mean Script

Usage:
    cleandata.py <dataset_location> [--aws] [--c=<cluster_number>] [--aws-token=<token>] [--aws-secret=<secret>] [--app-name=<name>]

    <dataset_location>      Location of the dataset we want to clean;
    --c=<cluster_number>    Specify how many clusters we want to find;
    --aws                   Enable AWS saving/reading of the datasets.
"""
import os

import numpy as np
import pandas as pd
from sklearn import cluster

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf

from docopt import docopt

def stringify(array):
    if not array:
        return None
    return '[' + ','.join([str(elem) for elem in array]) + ']'

if __name__ == "__main__":
    # Parse the command line
    arguments = docopt(__doc__)


    data_path = arguments["<dataset_path>"]
    max_clusters = arguments["--c"]
    app_name = arguments["--app-name"] if arguments["--app-name"] else "twitter-clustering-sklearn"

    spark = SparkSession.builder.appName(app_name).getOrCreate()

    if arguments["--aws"]:
        aws_token = arguments["--aws-token"] if arguments["--aws-token"] else os.environ["ACCESS_TOKEN"]
        aws_secret = arguments["--aws-secret"] if arguments["--aws-secret"] else os.environ["ACCESS_SECRET"]
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_token)
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret)
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-west-2.amazonaws.com")

    tostring_udf = udf(stringify,StringType())

    print("reading data")
    df = spark.read.format("parquet") \
        .option("inferSchema", True) \
        .load(data_path)
    print("done\n\n")

    dfpandas = df.toPandas()
    spark.stop()
    print("stopped pyspark")

    # Convert features into Pandas column.
    print("feats inline")
    series = dfpandas['features'].apply(lambda x : np.array(x.toArray())).as_matrix().reshape(-1,1)
    features = np.apply_along_axis(lambda x : x[0], 1, series)
    feats = pd.DataFrame(features)

    # Generate the final dataset (which can be saved to disk.)
    print("drop column and merge datasets")
    dfpandas = dfpandas.drop(columns=["features"])
    final_df = pd.merge(dfpandas, feats, left_index=True, right_index=True)

    print("starting k-means")

    def clustering_algo(_max_clusters):
        k_means = cluster.KMeans(n_clusters=_max_clusters, max_iter=20)
        k_means.fit(feats)

    # Run the k-mean algorithm and get its execution time
    import timeit
    setup = "from __main__ import clustering_algo"
    print(timeit.timeit("clustering_algo({})".format(max_clusters), setup=setup, number=1))
    