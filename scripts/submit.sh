#!/usr/bin/env bash
# Azure HDInsight set up script

# Set up strict bash
# See http://redsymbol.net/articles/unofficial-bash-strict-mode/
set -euo pipefail
IFS=$'\n\t'

# Install pip and requirements (fix also locale problem)
sudo apt-get install -y python3-pip unzip
export LC_ALL="en_US.UTF-8"
export LC_CTYPE="en_US.UTF-8"

# Get repository and start from the right branch
git clone https://github.com/geektoni/twitter100days
cd twitter100days
git checkout preprocessing_without_nltk

# Install project requirements
pip3 install --user -r requirements.txt

# IDs of the datasets
FILEID_A="1VhG5m7xVfvjJbL2_0hHEcYx7o9urAMYP"
FILEID_B="1kk_jQtCYIKbaw7ZJLxrIhlLsgn0pScmB"

# Download first dataset
# https://drive.google.com/open?id=1VhG5m7xVfvjJbL2_0hHEcYx7o9urAMYP
wget --load-cookies /tmp/cookies.txt "https://docs.google.com/uc?export=download&confirm=$(wget --quiet --save-cookies /tmp/cookies.txt --keep-session-cookies --no-check-certificate "https://docs.google.com/uc?export=download&id=$FILEID_A" -O- | sed -rn 's/.*confirm=([0-9A-Za-z_]+).*/\1\n/p')&id=$FILEID_A" -O data_01.csv.tgz && rm -rf /tmp/cookies.txt

# Download second dataset
# https://drive.google.com/open?id=1kk_jQtCYIKbaw7ZJLxrIhlLsgn0pScmB
wget --load-cookies /tmp/cookies.txt "https://docs.google.com/uc?export=download&confirm=$(wget --quiet --save-cookies /tmp/cookies.txt --keep-session-cookies --no-check-certificate "https://docs.google.com/uc?export=download&id=$FILEID_B" -O- | sed -rn 's/.*confirm=([0-9A-Za-z_]+).*/\1\n/p')&id=$FILEID_B" -O data_02.csv.tgz && rm -rf /tmp/cookies.txt

# Unpack files
tar -xvzf data_01.csv.tgz
tar -xvzf data_02.csv.tgz

# Run spark once to enable hadoop (??)
touch test.py && echo "exit()" >> test.py
spark-submit test.py
rm -rf test.py

# Copy files on hadoop HDFS
hadoop fs -copyFromLocal tweet_text_01.csv /tweets_01.csv
hadoop fs -copyFromLocal tweet_text_02.csv /tweets_02.csv

# Export python version for the sake of Spark
export PYSPARK_PYTHON="python3"
export PYSPARK_DRIVER_PYTHON="python3"

# Run the cleaning operations
# Remember to set manually the ACCESS_TOKEN and ACCESS_SECRET
cd preprocessing/
python3 cleandata.py wasb:///tweets_01.csv s3a://bigdataprojecttwitter2018/data_01 1024 --aws
python3 cleandata.py wasb:///tweets_02.csv s3a://bigdataprojecttwitter2018/data_02 1024 --aws

# Run clustering algorithms
cd ../model
python3 clustering_script.py kmeans s3a://bigdataprojecttwitter2018/data_01
python3 clustering_script.py kmeans s3a://bigdataprojecttwitter2018/data_02