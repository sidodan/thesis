# Preprocess - Covert S3 data into files, having each user in a separate file
# Repeat this step to generate labeled positive and negative examples
# Need to generate data in the same format for SEED (positive), RANDOM (negative) and TEST (users to be evaluated)

import os

path = 'RAW_DATA/S3_DATA/'
output_path = 'RAW_DATA/OUT'

# Need to repeat this process for some positive and negative examples
final_path = 'TEST' 
# final_path = 'SEED'
# final_path = 'RANDOM'

# out_file contains a concatinated file for all tweets.
out_file = open(output_path + '/' + 'out.txt', 'w')
# users_file is basically a user column contains all users we with to classify
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
