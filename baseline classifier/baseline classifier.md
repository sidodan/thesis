### Baseline Classifier

This section is a baseline classifier for subpopulations in Twitter. 
It used to perform as a baseline for our method, which is base on personalized pagerank. 

## Steps

In order to use it, please perform the following steps:

1. Extract data using "CollectUsersTweets.py" - can be run using pyspark.
This file extract users specific users data from twitter stream.

2. Run GenerateUserFielsSPClassifier.py
This file generates data in a format compatible with the classifier. Positive, negative and Test twitter streams are required.
  
3. Run BaselineSPClassifierStructed.py
This file is the classifier itself, which builds a feature vector for each user base on its used words.
In order to run this step, please make sure you already have some positive examples under SEED folder, negative examples under RANDOM, and users you wish to classify under TEST.

## Results

The resutls are basically a labeled for each user appears in the TEST folder. 