# Preprocess - Covert S3 data into files, having each user in a separate file
# Repeat this step to generate labeled positive and negative examples
import os

path = 'RAW_DATA/S3_DATA/'
output_path = 'RAW_DATA/OUT'
final_path = 'TEST'

# out_file contains a concatinated file for all tweets.
out_file = open(output_path + '/' + 'out.txt', 'w')
# users_file is basically a user column contains all users we wish to classify - the Test data
users_file = open(output_path + '/' + 'users.txt', 'w')
# same users, in a list format
users_file_list = open(output_path + '/' + 'users_list.txt', 'w')

file_names = os.listdir(path)
seen = {}
users_list = []
for filename in file_names:
    file = open(path + '/' + filename, 'r')
    for line in file:
        line = line.strip()
        line = line.lower()
        name = line.split('\t')[0]
        tweet = line.split('\t')[1]
        out_file.write(name + '\t' + tweet + '\n')
        if name not in seen:
            users_list.append(name)
            seen[name] = True
    file.close()
out_file.close()

# write the list of users to a file
users_file_list.write("%s\n" % users_list)

for item in users_list:
    # write the list of users to a file in a column format
    users_file.write("%s\n" % item)

users_file.close()
users_file_list.close()

# generate a file for each user with its tweets.
for user in users_list:
    big_file = open(output_path + '/' + 'out.txt', 'r')
    user_file = open(final_path + '/' + user + '.txt', 'w')
    for line in big_file:
        line = line.strip()
        line = line.lower()
        name = line.split('\t')[0]
        tweet = line.split('\t')[1]
        if name == user:
            user_file.write(tweet + '\n')
    user_file.close()
    big_file.close()

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

