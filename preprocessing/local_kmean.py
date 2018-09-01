from __future__ import print_function

import string
import numpy as np
import pandas as pd
from sklearn import cluster

from pyspark.sql import SparkSession
from pyspark.sql import Row, Column
from pyspark.sql.types import *

from pyspark.sql.functions import udf


from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover

import os
import sys

import re
import numpy

from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tokenize import RegexpTokenizer

def stringify(array):
    if not array:
        return None
    return '[' + ','.join([str(elem) for elem in array]) + ']'

if __name__ == "__main__":
    spark = SparkSession.builder.appName("data-cleaning").getOrCreate()
    tostring_udf = udf(stringify,StringType())

    print("reading data")
    df = spark.read.format("parquet").option("header", True) \
        .option("inferSchema", True) \
        .load("data/mock_data/polished_data/slice_1000/")
    print("done\n\n")

    dfpandas = df.toPandas()
    spark.stop()

    # dfpandas.info()
    # print(dfpandas)
    print("stopped pyspark")
    print("feats inline")
    series = dfpandas['features'].apply(lambda x : np.array(x.toArray())).as_matrix().reshape(-1,1)
    # print(series)
    features = np.apply_along_axis(lambda x : x[0], 1, series)
    # print(features)
    feats = pd.DataFrame(features)
    # print(feats)
    print("drop column and merge datasets")
    dfpandas = dfpandas.drop(columns=["features"])
    final_df = pd.merge(dfpandas, feats, left_index=True, right_index=True)
    # print(final_df)
    
    print("starting k-means")
    k_means = cluster.KMeans(n_clusters=10, max_iter=1000)
    k_means.fit(feats)
    print(k_means.cluster_centers_)
    # print("writing to file csv")

    # final_df.to_csv("pippo.csv", index=False)
    