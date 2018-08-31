from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.clustering import KMeans
from pyspark.ml.clustering import BisectingKMeans
from pyspark.ml.clustering import GaussianMixture
from pyspark.ml.clustering import LDA
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.functions import explode

import matplotlib.pyplot as plt


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


def get_information_from_model(dataframe, column, _cluster_number, _type):
	"""
	Get information from the model and the predictions.
	:param dataframe:
	:param column:
	:param n_clusters:
	:param _type:
	:return:
	"""

	# Check if the cluster number was supplied
	cluster_number = int(_cluster_number) if _cluster_number else 10

	if _type == "kmeans":

		# Get all the top words contained into each of the clusters
		# and save them to file.
		for i in range(0, cluster_number):
			dataframe.withColumn("tokens", explode(column))\
			.filter("prediction={}".format(str(i)))\
			.groupBy("tokens")\
			.count()\
			.orderBy("count", ascending=False)\
			.select("tokens", "count")\
			.show()
			#.write.csv("cluster_{}_most_common_words.csv".format(str(i)))


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


def repeat_experiment(start_range, end_range, step, fn_to_repeat):
	"""
	Repeat an experiment and take the model with the largest Silhouette
	score.
	:param start_range: start number of clusters
	:param end_range: end number of clusters
	:param step: range's step
	:param fn_to_repeat: clustering function that needs to be evaluated
	:return: the predicted number of clusters and the dataframe predicted
	"""
	evaluator = ClusteringEvaluator()
	model, max_df = fn_to_repeat(start_range)
	max_k = start_range
	max_sil = evaluator.evaluate(max_df)

	# Append values
	silhouette_value = []
	wss_value =[]
	silhouette_value.append(max_sil)
	wss_value.append(model.computeCost(max_df))

	for k in range(start_range+step, end_range+step, step):
		model_tmp, df = fn_to_repeat(k)
		silhouette = evaluator.evaluate(df)

		# Append values
		silhouette_value.append(silhouette)
		wss_value.append(model_tmp.computeCost(df))

		# Take the maximum
		if silhouette >= max_sil:
			max_k = k
			max_df = df

	return max_k, max_df, silhouette_value, wss_value


def print_plot(data, range):
	plt.plot(data, range)
	plt.show()

def print_verbose(string, verbose=False):
	"""
	Basic logger method. It will print only if we want it to.
	:param string: the string we want to print on screen.
	:param verbose: flag that enable the print on screen
	:return: nothing
	"""
	if verbose:
		print(string)