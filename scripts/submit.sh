#!/usr/bin/env bash
# Azure HDInsight set up script

# Set up strict bash
# See http://redsymbol.net/articles/unofficial-bash-strict-mode/
set -euo pipefail
IFS=$'\n\t'

# Install pip and requirements (fix also locale problem)
sudo apt-get install -y python3-pip
export LC_ALL="en_US.UTF-8"
export LC_CTYPE="en_US.UTF-8"
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
tar -zxvf data_01.csv.tgz
tar -zxvf data_02.csv.tgz