#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Preprocessing Script

Usage:
    cleandata.py <dataset_location> <output_location> <num_features> [--aws] [--custom-hadoop]
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, size
from pyspark.ml.feature import HashingTF, IDF
from pyspark.ml.feature import RegexTokenizer, Tokenizer
from pyspark.ml.feature import StopWordsRemover

import re
import os
import string

from docopt import docopt

punct = str.maketrans('', '', string.punctuation)
words = open("./../data/twitter-stopwords.txt")
sp_wrd = []
for w in words:
    sp_wrd = w.split(",")

def preprocess(text):
    sentence = re.sub('http[s]*:\/\/[a-zA-Z0-9\.]+\/[a-zA-Z0-9]+', '', text)    # remove urls
    sentence = re.sub('\s([@][\w_-]+)', '', sentence)                           # remove @mentions
    sentence = sentence.lower()                                                 # lower case
    return sentence.translate(punct)                                            # remove punctuation


def remove_numbers_single_words(sentence):
    min_size = 4
    return [w for w in sentence if len(w) >= min_size and not re.match(r'\d', w)]  # remove small words and numbers


def stringify(array):
    if not array:
        return None
    return '[' + ','.join([str(elem) for elem in array]) + ']'


def createFeats(spark, input, output, num_feat):
    preproc_udf = udf(preprocess, StringType())
    remove_udf = udf(remove_numbers_single_words, ArrayType(StringType()))

    print("loading file")
    df = spark.read.format("csv").option("header", False) \
        .option("delimiter", ",").option("inferSchema", True) \
        .load(input)

    print("------------------------------------------------")

    # Remove urls, punctuation and set everything as lower case
    df = df.filter(df._c2.isNotNull())
    df = df.withColumn("text", preproc_udf(df["_c2"]))
    df = df.filter(df.text.isNotNull())

    # Tokenize words
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    df = tokenizer.transform(df)

    # Remove stopwords
    all_stopwords = StopWordsRemover.loadDefaultStopWords("english") + sp_wrd + StopWordsRemover.loadDefaultStopWords("italian")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=all_stopwords)
    df = remover.transform(df)
    df = df.filter(size(df.filtered_words) > 0)

    # Remove words smaller that 5 letters and numbers
    df = df.withColumn("filtered_words_2", remove_udf(df["filtered_words"]))
    df = df.filter(size(df.filtered_words_2) > 0)

    hashingTF = HashingTF(inputCol="filtered_words_2", outputCol="rawFeatures", numFeatures=num_feat)
    featurizedData = hashingTF.transform(df)

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    
    rescaledData = idfModel.transform(featurizedData)
    rescaledData = rescaledData.select("_c0", "filtered_words_2", "features")
    rescaledData.write.parquet(output)

def f(row):
    print(row)


if __name__ == "__main__":

    arguments = docopt(__doc__)

    dataset_input = arguments["<dataset_location>"]
    dataset_output = arguments["<output_location>"]
    features = int(arguments["<num_features>"])

    if arguments["--custom-hadoop"]:
        os.environ[
            'PYSPARK_SUBMIT_ARGS'] = "--jars=/opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-1.7.4.jar," \
                                 "/opt/hadoop/share/hadoop/tools/lib/hadoop-aws-2.7.7.jar" \
                                 " pyspark-shell"

    spark = SparkSession.builder.appName("data-cleaning").getOrCreate()

    # Set up access for Amazon AWS
    if arguments["--aws"]:
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", os.environ["ACCESS_TOKEN"])
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", os.environ["ACCESS_SECRET"])
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.eu-west-2.amazonaws.com")
        spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    # Polish and create the features
    createFeats(spark, dataset_input, dataset_output, features)

    spark.stop()