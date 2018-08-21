#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Preprocessing Script

Usage:
    cleandata.py <dataset_location> <output_location>
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.ml.feature import HashingTF, IDF

import re
import string

from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tokenize import RegexpTokenizer

from docopt import docopt

stop_en = set(stopwords.words('english'))
stop_it = set(stopwords.words('italian'))
lemma = WordNetLemmatizer()

def preprocess(text):    
    sentence = re.sub('http[s]*:\/\/[a-zA-Z0-9\.]+\/[a-zA-Z0-9]+', '', text)    # remove urls
    sentence = sentence.lower()                                                     # lower case
    sentence = sentence.translate(None, string.punctuation)                     # remove punctuation

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


def createFeats(spark, input, output, num_feat):
    preproc_udf = udf(preprocess, ArrayType(StringType()))
    tostring_udf = udf(stringify,StringType())

    print("loading file")
    df = spark.read.format("csv").option("header", True) \
        .option("delimiter", ",").option("inferSchema", True) \
        .load(input)

    print("------------------------------------------------")
    
    df = df.withColumn("text", preproc_udf(df["text"]))
    df = df.filter(df.text.isNotNull())

    hashingTF = HashingTF(inputCol="text", outputCol="rawFeatures", numFeatures=num_feat)
    featurizedData = hashingTF.transform(df)

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    
    rescaledData = idfModel.transform(featurizedData)
    rescaledData.write.parquet(output+"/tdfIF")

    # DEBUG
    # read=spark.read.parquet("tdfIF")
    # read.show(truncate=False)
    # read.printSchema()

def f(row):
    print(row)
    
if __name__ == "__main__":

    arguments = docopt(__doc__)

    dataset_input = arguments["<dataset_location>"]
    dataset_output = arguments["<output_location>"]

    spark = SparkSession.builder.appName("data-cleaning").getOrCreate()
    createFeats(spark, dataset_input, dataset_output, 20)
    spark.stop()