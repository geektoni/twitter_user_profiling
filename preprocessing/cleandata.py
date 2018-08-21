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
    spark.stop()