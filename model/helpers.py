from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.clustering import KMeans


def return_correct_clustering_algorithm(_type, cluster_number):
	"""
	This method returns an instance of the clustering algorithm
	selected by the user.
	:param _type: the name of the algorithm we want to use.
	:param cluster_number: the number of clusters.
	:return: an _type instance or it raises an execption if _type is not valid.
	"""
	if _type == "kmeans":
		if not cluster_number:
			return KMeans().setK(10).setSeed(1)
		else:
			return KMeans().setK(int(cluster_number)).setSeed(1)
	else:
		raise Exception("The clustering algorithm requested {} is not available".format(_type))


def get_sample_features(spark):
	"""
	Factory method. It will return a DataFrame usable by the clustering algorithms.
	:param spark: the spark session variable.
	:return: a dataframe containing ids and the features.
	"""
	sentenceData = spark.read.csv("../data/mock_data/data.csv")

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