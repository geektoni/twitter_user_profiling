from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.clustering import KMeans
from pyspark.ml.clustering import BisectingKMeans
from pyspark.ml.clustering import GaussianMixture
from pyspark.ml.clustering import LDA


def return_correct_clustering_algorithm(_type, _cluster_number, _max_iter):
	"""
	This method returns an instance of the clustering algorithm
	selected by the user.
	:param _type: the name of the algorithm we want to use.
	:param _cluster_number: the number of clusters.
	:param _max_iter: the maximum number of iterations.
	:return: an _type instance or it raises an execption if _type is not valid.
	"""

	cluster_number = int(_cluster_number) if _cluster_number else 10
	max_iter = int(_max_iter) if _max_iter else 1000

	if _type == "kmeans":
		return KMeans().setK(cluster_number).setMaxIter(max_iter).setSeed(1)
	elif _type == "b-kmeans":
		return BisectingKMeans().setK(cluster_number).setMaxIter(max_iter).setSeed(1)
	elif _type == "gmm":
		return GaussianMixture().setK(cluster_number).setMaxIter(max_iter).setSeed(1)
	elif _type == "lda":
		return LDA().setK(cluster_number).setMaxIter(max_iter).setSeed(1)
	else:
		raise Exception("The clustering algorithm requested {} is not available".format(_type))


def get_sample_features(spark, _data_path, _is_header=False):
	"""
	Factory method. It will return a DataFrame usable by the clustering algorithms.
	:param spark: the spark session variable.
	:param _data_path: path where the data are located (CSV only).
	:param _is_header: if the file contains an header.
	:return: a dataframe containing ids and the features.
	"""
	sentenceData = spark.read.csv(_data_path, header=_is_header)

	tokenizer = Tokenizer(inputCol="_c2", outputCol="words")
	wordsData = tokenizer.transform(sentenceData.na.drop(subset=["_c2"]))

	# CountVectorizer can also be used to get term frequency vectors
	hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
	featurizedData = hashingTF.transform(wordsData)

	idf = IDF(inputCol="rawFeatures", outputCol="features")
	idfModel = idf.fit(featurizedData)
	rescaledData = idfModel.transform(featurizedData)
	return rescaledData.select("_c0", "features")


def print_verbose(string, verbose=False):
	"""
	Basic logger method. It will print only if we want it to.
	:param string: the string we want to print on screen.
	:param verbose: flag that enable the print on screen
	:return: nothing
	"""
	if verbose:
		print(string)