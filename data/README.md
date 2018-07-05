# Data

This directory contains the script used to analyze and mine the tweets using Amazon AWS and the Twitter's API. To run
these scripts you will need the following components:
  * **Python 3.0+**;
  * **Docopt**;
  * **Tweepy**;
  * **Stop_words**;
  * **Boto3**;

Moreover, the `twitter_miner.py` will require some environmental variables to be set (see `twitter_miner.py --help`). They are needed to work with the Twitter API.
As a final note, to upload the files to a S3 bucket on Amazon AWS you will need to run `aws configure` locally and set up
a proper user with the necessary rights to access your bucket.
