### Baseline Classifier ###
# Run this code after you have positive an d negative examples under SEED and RANDOM folders, and some users you wish to classify under TEST
# Three versions of the baseline classifier
# 
def SplitLine(line, FLAG):
    line = line.strip()
    line = line.lower()
    # Words + Hashtags + Mentions
    if FLAG == 'W_H_M':    
        words = re.split(r'\W+', line)
        return words
    # Hashtags only
    if FLAG == 'H':
        words = re.split('#', line)
        words.pop(0)
        return words
    # words only
    if FLAG == 'W':
        words = re.split(' ', line)
        return words

import re
import shutil
import os

import datetime
from sklearn import svm

FLAG = 'H' # Words + Hashtags + Mentions

result_path = 'RESULTS'
word_index_file = open(result_path + '/' + 'word_index.txt', 'w')
test_file = open(result_path + '/' + 'test_index.txt', 'w')
train_file = open(result_path + '/' + 'train_index.txt', 'w')
acc_file = open(result_path + '/' + 'test_inference.txt', 'w')

__stop_words__ = set(line.strip() for line in open('300StopWordsForclassifier_list.txt'))

current_time = datetime.datetime.now().time()
print 'Start build dictionary: ' + current_time.isoformat()

dirnames = ['SEED', 'RANDOM']
counts = {}
n = 0
for dirname in dirnames:
    filenames = os.listdir(dirname)
    for filename in filenames:
        file = open(dirname + '/' + filename, 'r')
        seen = {}
        for line in file:
            words = SplitLine(line, FLAG)
            if FLAG == 'W_H_M':
                for word in words:         
                    if len(word) > 0 and word not in seen and word not in __stop_words__:
                        counts[word] = counts.get(word, 0) + 1
                    seen[word] = True
            if FLAG == 'W' or FLAG == 'H': 
                for chunk in words:
                    match = re.search('^(\w+)(\W.*)?$', chunk)
                    if match:
                        word = match.group(1)                           
                        if len(word) > 0 and word not in seen and word not in __stop_words__:
                            counts[word] = counts.get(word, 0) + 1
                seen[word] = True
        file.close()

word_to_index = {}

for word in sorted(counts, key=counts.get, reverse=True):
    if counts[word] > 0 and word not in __stop_words__:
        word_to_index[word] = n
        n += 1
        word_index_file.write(str(n) + '\t' + str(word) + '\n')

word_index_file.close()
current_time = datetime.datetime.now().time()
print 'End build dictionary: ' + current_time.isoformat()
print 'A Dictionary with ' + str(len(word_to_index)) + ' unique words was generated'

# Train the model
x_train = []
y_train = []
x_test = []
y_test = []

dirnames = ['SEED', 'RANDOM', 'TEST']
test_filenames = [] # Add Filenames to a list, to print it during inference
c = 0 # Add a counter index for the training examples
for dirname in dirnames:
    filenames = os.listdir(dirname)
    length = len(filenames)
    for filename in filenames:
        file = open(dirname + '/' + filename, 'r')
        feature_vector = [0 for i in range(len(word_to_index))]
        for line in file:
            words = SplitLine(line, FLAG)
            if FLAG == 'W_H_M':
                for word in words:
                    if word in word_to_index:
                        index = word_to_index[word]
                        feature_vector[index] += 1
            if FLAG == 'W' or FLAG == 'H': 
                for chunk in words:
                    match = re.search('^(\w+)(\W.*)?$', chunk)
                    if match:
                        word = match.group(1)
                        if word in word_to_index:
                            index = word_to_index[word]
                            feature_vector[index] += 1
        file.close()
        if dirname == 'SEED' or dirname == 'RANDOM':
            x_train.append(feature_vector)
            y_train.append(dirname)
            train_file.write(filename.split('.')[0] + '\t' + dirname + '\n')
        else:
            x_test.append(feature_vector)
			# comment out the below code in case you have labeled data, i.e. y_test labels.
            # y_test.append(dirname)
			test_file.write(str(c) + '\t' + filename.split('.')[0] + '\t' + dirname + '\n')
            test_filenames.append(filename.split('.')[0])
            c = c + 1

current_time = datetime.datetime.now().time()
print 'Start Classification: ' + current_time.isoformat()

# Generate linear SVM
clf_svm = svm.SVC(kernel = 'linear')
clf_svm = clf_svm.fit(x_train, y_train)
z_test = clf_svm.predict(x_test)
    
for i in range(len(z_test)):
    acc_file.write(str(i) + '\t' + str(test_filenames[i]) + '\t' + str(z_test[i]) + '\n')

#4 Close Result files
train_file.close()
test_file.close()
acc_file.close()

current_time = datetime.datetime.now().time()
print 'End Classification: ' + current_time.isoformat()

# comment out the below code in case you have labeled data, i.e. y_test labels. 
'''
acc = 0
TotalNoTestSamples = len(z_test)
# Seeds are positive examples, while Random are negative
TP = 0  # True Seeds
TN = 0  # True Random
FP = 0  # False Seeds
FN = 0  # False Random
for i in range(len(z_test)):
    # S means Seed ,R means Random
    if z_test[i][:1] == y_test[i][:1]:
        acc += 1
    if z_test[i][:1] == y_test[i][:1] and y_test[i][:1] == 'S':
        TP += 1
    if z_test[i][:1] == y_test[i][:1] and y_test[i][:1] == 'R':
        TN += 1
    if z_test[i][:1] != y_test[i][:1] and y_test[i][:1] == 'R':
        FP += 1
    if z_test[i][:1] != y_test[i][:1] and y_test[i][:1] == 'S':
        FN += 1
    acc_file.write(str(i) + '\t' + str(y_test[i][:1]) + '\t' + str(z_test[i][:1]) + '\n')
    
acc /= float(len(z_test))
Precision = float(float(TP) / (float(TP + FP)))
Recall = float(float(TP) / (float(TP + FN)))

print 'Accuracy: ' + str(acc)
print 'Precision: ' + str(Precision)
print 'Recall: ' + str(Recall)
print 'True Random: ' + str(TN)
print 'False Random: ' + str(FN)
print 'True Seeds: ' + str(TP)
print 'False Seeds: ' + str(FP)
# Close Result files
test_file.close()
acc_file.close()

current_time = datetime.datetime.now().time()
print 'End Classification: ' + current_time.isoformat()

'''
