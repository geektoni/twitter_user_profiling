# import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
from __future__ import print_function

from typing import List
from docopt import docopt
import string

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.ml.linalg import Vectors

from pyspark.ml.feature import StopWordsRemover

import os
import sys

# import pandas
# import csv
# import sys
import string
import re
import numpy

from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tokenize import RegexpTokenizer

# import gensim
# from gensim import corpora

# sentenceData = spark.createDataFrame([
#     (0, ["I", "saw", "the", "red", "balloon"]),
#     (1, ["Mary", "had", "a", "little", "lamb"])
# ], ["id", "raw"])

stop = set(stopwords.words('english'))
lemma = WordNetLemmatizer()
direct = "file:///mnt/c/Users/Annalisa/Documents/Dataset/"
f_name = "data_02/text_20_wh.csv"
# path = direct + f_name

def preprocess(row):
    sentence = re.sub('http[s]*:\/\/[a-zA-Z0-9\.]+\/[a-zA-Z0-9]+', '', row.text)    # remove urls
    sentence = sentence.lower()                                                     # lower case
    tokenizer = RegexpTokenizer(r'\w+')                                             # tokenize
    tokens = tokenizer.tokenize(sentence)                                           # tokenize
    filtered_words = [lemma.lemmatize(w) for w in tokens if not w in stop]          # lemmatize
    for w in filtered_words:
        r = StringType()
        # w = w.cast(StringType())
        r.toInternal(w)
    arr = ArrayType(StringType(), True)
    filtered_words = arr.toInternal(filtered_words)
    # print(filtered_words)
    print(type(filtered_words))
    return filtered_words

# spark.read.csv(
#     "/mnt/c/Users/Annalisa/Documents/Dataset Twitter/data_02/some_input_file.csv", header=False
# )
def read_data(spark):
    print("loading file")
    df = spark.read.format("csv").option("header", True) \
        .option("delimiter", ",").option("inferSchema", True) \
        .load(direct + f_name)
    
    print("file read")
    # print("--------------------------------")
    # print("# of lines and columns names")
    # print(df.count(), df.columns)
    # print("--------------------------------")
    # print("show first 5 of col text")
    # print(df.select('text').show(5))
    # print("--------------------------------")
    # print("show schema")
    # print(df.printSchema())
    print("--------------------------------")
    print("apply preprocessing to text")
    df3 = df.select('text')
    def f(row):
        print(row.text)
    # df3.foreach(f)

    df2 = (df.rdd.map(lambda x: preprocess(x)))
    df2 = df2.collect()
    print(df2)
    print(type(df2))
    print(type(df2[0]))

    schema = StructType([
        StructField("text", ArrayType(StringType(), True), True)
    ])

    dfp = spark.createDataFrame(df2, schema)
    dfp.foreach(f)
    # df4 = 
    dfp.printSchema()
    
    # print(df2)

    # dfp=dfp.withColumn('row_index', f.monotonically_increasing_id())
    # df5=df.withColumn('row_index', f.monotonically_increasing_id())

    # df4 = df5.drop('text').join(df2, on["row_index"]).sort("row_index").drop("row_index")
    # df4.show()
    print("--------------------------------")



    # l = sc.parallelize([1, 2, 3])
    # index = sc.parallelize(range(0, l.count()))
    # z = index.zip(l)

    # rdd = sc.parallelize([['p1', 'a'], ['p2', 'b'], ['p3', 'c']])
    # rdd_index = index.zip(rdd)

    # # just in case!
    # assert(rdd.count() == l.count())
    # # perform an inner join on the index we generated above, then map it to look pretty.
    # new_rdd = rdd_index.join(z).map(lambda (x, y): [y[0][0], y[0][1], y[1]])
    # new_df = new_rdd.toDF(["product", 'name', 'new_col'])



    # df.write.partitionBy("user_id").format("parquet").save(direct +"prova.parquet")


if __name__ == "__main__":
    # arguments = docopt(__doc__)

    # path = arguments["<path>"]
    spark = SparkSession.builder.appName("data-cleaning").getOrCreate()
    read_data(spark)
    spark.stop()
