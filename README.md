# Tweets Clustering for User Profiling

Main Authors:
- [Giovanni De Toni](mailto:giovanni.det@gmail.com)
- [Annalisa Congiu](mailto:annalisa.congiu@studenti.unitn.it)

This repository contains a Spark pipeline which can be used to parse and analyze
a Twitter dataset in order to perform clustering of the tweets such to extract
user profiles and insight about the different possible communities which are
present on the social Network

## Installation

This project requires Python3 and pip3. The installation of the required packages
can be done automatically by running this line on your terminal:

```bash
pip3 install -r requirements.txt
```

## Usage

In each directory you can find the following:
 * data: contains a Twitter miner which can be used to mine tweets directly from Twitter API and
   some mock twitter data;
 * model: contains the model used for this work (scikit-learn and spark);
 * preprocessing: contains the utilities employed to clean the dataset and perform feauture extraction;
 * script: contains some utilities used to test everything.

## Contributing

This project is not maintained anymore. It serve just as a showcase about several
features of Apache Spark and MLlib.

## License
[MIT](https://choosealicense.com/licenses/mit/)
