<<<<<<< HEAD
from __future__ import print_function

import string

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


stop = set(stopwords.words('english'))
lemma = WordNetLemmatizer()
direct = "file:///mnt/c/Users/Annalisa/Documents/Dataset/"
f_name = "data_02/text_20_wh.csv"


def preprocess(text):    
    sentence = re.sub('http[s]*:\/\/[a-zA-Z0-9\.]+\/[a-zA-Z0-9]+', '', text)    # remove urls
    sentence = sentence.lower()                                                     # lower case

    tokenizer = RegexpTokenizer(r'\w+')                                             # tokenize
    tokens = tokenizer.tokenize(sentence)                                           # tokenize
    filtered_words = [w for w in tokens if len(w)> 1 and not re.match(r'\d', w) and not w in stop]  # remove letters and only numbers
    lemmas = [lemma.lemmatize(w) for w in filtered_words]          # lemmatize   
    
    if not len(lemmas):
        return None
    return lemmas


def stringify(array):
    if not array:
        return None
    return '[' + ','.join([str(elem) for elem in array]) + ']'

def createFeats(spark):
    preproc_udf = udf(preprocess, ArrayType(StringType()))
    tostring_udf = udf(stringify,StringType())

    print("loading file")
    df = spark.read.format("csv").option("header", True) \
        .option("delimiter", ",").option("inferSchema", True) \
        .load(direct + f_name)

    print("------------------------------------------------")
    
    df = df.withColumn("text", preproc_udf(df["text"]))
    df = df.filter(df.text.isNotNull())

    hashingTF = HashingTF(inputCol="text", outputCol="rawFeatures", numFeatures=20)
    featurizedData = hashingTF.transform(df)

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    
    rescaledData = idfModel.transform(featurizedData)
    rescaledData.write.parquet("tdfIF")

    # read=spark.read.parquet("tdfIF")
    # read.show(truncate=False)
    # read.printSchema()

def f(row):
    print(row)
    
if __name__ == "__main__":
    spark = SparkSession.builder.appName("data-cleaning").getOrCreate()
    createFeats(spark)
=======
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Preprocessing Script

Usage:
    cleandata.py <dataset_location> <output_location> <num_features> [--aws]
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.ml.feature import HashingTF, IDF
from pyspark.ml.feature import RegexTokenizer, Tokenizer
from pyspark.ml.feature import StopWordsRemover

import re
import os
import string

from docopt import docopt

punct = str.maketrans('', '', string.punctuation)

def preprocess(text):
    sentence = re.sub('http[s]*:\/\/[a-zA-Z0-9\.]+\/[a-zA-Z0-9]+', '', text)    # remove urls
    sentence = sentence.lower()                                                 # lower case
    return sentence.translate(punct)                                        # remove punctuation

    #lemmas = [lemma.lemmatize(w) for w in filtered_words]          # lemmatize
    
    #if not len(lemmas):
    #    return None

    #print(lemmas)
    #return lemmas

def remove_numbers_single_words(sentence):
    min_size = 5
    return [w for w in sentence if len(w) >= min_size and not re.match(r'\d', w)]  # remove letters and only numbers


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
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=StopWordsRemover.loadDefaultStopWords("english"))
    df = remover.transform(df)
    df = df.filter(df.text.isNotNull())

    # Remove words smaller that 5 letters and numbers
    df = df.withColumn("filtered_words_2", remove_udf(df["filtered_words"]))
    df = df.filter(df.text.isNotNull())

    hashingTF = HashingTF(inputCol="filtered_words_2", outputCol="rawFeatures", numFeatures=num_feat)
    featurizedData = hashingTF.transform(df)

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    
    rescaledData = idfModel.transform(featurizedData)
    rescaledData.write.parquet(output)

def f(row):
    print(row)


if __name__ == "__main__":

    arguments = docopt(__doc__)

    dataset_input = arguments["<dataset_location>"]
    dataset_output = arguments["<output_location>"]
    features = arguments["<num_features>"]

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

>>>>>>> c6d22a7e58d67bb13338c96739aac2f015b2646c
    spark.stop()