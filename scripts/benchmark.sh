#!/usr/bin/env bash

# Set up strict bash
# See http://redsymbol.net/articles/unofficial-bash-strict-mode/
set -euo pipefail
IFS=$'\n\t'

# AWS token
AWS_TOKEN=""
AWS_SECRET=""

# Install pip and requirements (fix also locale problem)
sudo apt-get install -y python3-pip unzip
export LC_ALL="en_US.UTF-8"
export LC_CTYPE="en_US.UTF-8"

# Get repository and start from the right branch
# and install project requirements
if [ ! -d twitter100days ]; then
    git clone https://github.com/geektoni/twitter100days
    cd twitter100days
    pip3 install --user -r requirements.txt
else
    cd twitter100days
fi

# Change to the correct branch
git checkout preprocessing_without_nltk

# Move to the model dir
cd model

# Export Python
export PYSPARK_PYTHON="python3"
export PYSPARK_DRIVER_PYTHON="python3"

# For each of the slices compute the time, given 100 clusters
for e in 1000 10000 100000 1000000;
do
    python3 clustering_script.py kmeans s3a://bigdataprojecttwitter2018/data_02/slice_$e --aws --c=100 --aws-token=$AWS_TOKEN --aws-secret=$AWS_SECRET --app-name=data_02_slice_$e
done

python3 clustering_script.py kmeans s3a://bigdataprojecttwitter2018/data_02/complete --aws --c=100 --aws-token=$AWS_TOKEN --aws-secret=$AWS_SECRET --app-name=data_02_complete
python3 clustering_script.py kmeans s3a://bigdataprojecttwitter2018/data_01/complete --aws --c=100 --aws-token=$AWS_TOKEN --aws-secret=$AWS_SECRET --app-name=data_01_complete

# Compute the time varying the kernel size
for k in `seq 10 10 100`
do
    python3 clustering_script.py kmeans s3a://bigdataprojecttwitter2018/data_02/complete --aws --c=$k --aws-token=$AWS_TOKEN --aws-secret=$AWS_SECRET --app-name=data_02_kernels_complete_$k
done