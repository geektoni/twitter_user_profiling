#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Twitter

Usage:
	tweets_analyzer.py <file>

	--k=<keywords>		Filter tweets based on keywords
	--l=<languages>		Filter tweets based on language
	-h, --help			Print this help message
"""

import json
from docopt import docopt

arguments = docopt(__doc__)
file = arguments["<file>"]

with open(file, 'r') as f:
	for line in f:
		line = f.readline() # read only the first tweet/line
		tweet = json.loads(line) # load it as Python dict
		print(tweet["user"]["name"] + ":: "+tweet["text"]+"\n") # pretty-print
