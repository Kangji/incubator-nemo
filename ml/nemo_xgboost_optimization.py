#!/usr/bin/python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import getopt
import sys
from pathlib import Path
import pprint

import numpy as np
import xgboost as xgb

from tree import *
from inout import *

import matplotlib.pyplot as plt

# ########################################################
# MAIN FUNCTION
# ########################################################
try:
  opts, args = getopt.getopt(sys.argv[1:], "ht:d:r:i:", ["tablename=", "dagdirectory=", "resourceinfo=", "inputsize="])
except getopt.GetoptError:
  print('nemo_xgboost_optimization.py -t <tablename>')
  sys.exit(2)
for opt, arg in opts:
  if opt == '-h':
    print('nemo_xgboost_optimization.py -t <tablename>')
    sys.exit()
  elif opt in ("-t", "--tablename"):
    tablename = arg
  elif opt in ("-d", "--dagdirectory"):
    dagdirectory = arg
  elif opt in ("-r", "--resourceinfo"):
    resourceinfo = arg
  elif opt in ("-i", "--inputsize"):
    inputsize = arg

modelname = tablename + "_bst.model"
data = Data()
encoded_rows = data.load_data_from_db(tablename)
# write_to_file('process_test', processed_rows)

write_rows_to_file('nemo_optimization.out', encoded_rows)
# write_to_file('decode_test', decode_rows(encoded_rows, id_to_col, value_is_digit, id_to_value))
ddata = xgb.DMatrix('nemo_optimization.out')

avg_20_duration = np.mean(ddata.get_label()[:20])
print("average job duration: ", avg_20_duration)
allowance = avg_20_duration // 25  # 4%

row_size = len(encoded_rows)
print("total_rows: ", row_size)

## TRAIN THE MODEL (REGRESSION)
dtrain = ddata.slice([i for i in range(0, row_size) if i % 7 != 6])  # mod is not 6
print("train_rows: ", dtrain.num_row())
dtest = ddata.slice([i for i in range(0, row_size) if i % 7 == 6])  # mod is 6
print("test_rows: ", dtest.num_row())
labels = dtest.get_label()

## Load existing booster, if it exists
bst_opt = xgb.Booster(model_file=modelname) if Path(modelname).is_file() else None
preds_opt = bst_opt.predict(dtest) if bst_opt is not None else None
error_opt = (sum(1 for i in range(len(preds_opt)) if abs(preds_opt[i] - labels[i]) > allowance) / float(
  len(preds_opt))) if preds_opt is not None else 1
print('opt_error=%f' % error_opt)
min_error = error_opt

learning_rates = [0.01, 0.05, 0.1, 0.25, 0.5, 0.8]
for lr in learning_rates:
  param = {'max_depth': 6, 'eta': lr, 'verbosity': 0, 'objective': 'reg:linear'}

  watchlist = [(dtest, 'eval'), (dtrain, 'train')]
  num_round = row_size // 10
  bst = xgb.train(param, dtrain, num_round, watchlist, early_stopping_rounds=5)

  preds = bst.predict(dtest)
  error = (sum(1 for i in range(len(preds)) if abs(preds[i] - labels[i]) > allowance) / float(len(preds))) if len(
    preds) > 0 else 1.0
  print('error=%f' % error)

  ## Better booster
  if error <= error_opt:
    bst_opt = bst
    bst.save_model(modelname)
    min_error = error

print('minimum error=%f' % min_error)

## Let's now use bst_opt
## Check out the histogram by uncommenting the lines below
# fscore = bst_opt.get_fscore()
# sorted_fscore = sorted(fscore.items(), key=lambda kv: kv[1])
# for i in range(len(sorted_fscore)):
#   print("\nSplit Value Histogram:")
#   feature = sorted_fscore.pop()[0]
#   print(feature, "=", id_to_col[int(feature[1:])])
#   hg = bst_opt.get_split_value_histogram(feature)
#   print(hg)

df = bst_opt.trees_to_dataframe()
# print("Trees to dataframe")
# print(df)

trees = {}
for index, row in df.iterrows():
  if row['Tree'] not in trees:  # Tree number = index
    trees[row['Tree']] = Tree()

  # translated_feature = data.transform_id_to_key(int(row['Feature'][1:])) if row['Feature'].startswith('f') else row['Feature']
  # print(translated_feature)
  trees[row['Tree']].addNode(row['ID'], row['Feature'], row['Split'], row['Yes'], row['No'], row['Missing'],
                             row['Gain'])


# Let's process the data now.
dag_json = read_dag_json(dagdirectory, 'ir-0-initial.json')
print(dag_json)

results = {}
print("\nGenerated Trees:")
for t in trees.values():
  results = dict_union(results, t.importanceDict())
  print(t)

print("\nImportanceDict")
print(json.dumps(results, indent=2))

print("\nSummary")
resultsJson = []
for k, v in results.items():
  for kk, vv in v.items():
    # k = feature, kk = split, vv = val
    i, key, tpe = data.transform_id_to_keypair(int(k[1:]))
    how = 'greater' if vv > 0 else 'smaller'
    # result_string = f'{key} should be {vv} ({how}) than {kk}'
    # print(result_string)
    classes = key.split('/')
    key_class = classes[0]
    value_class = classes[1]
    value = data.transform_id_to_value(key, data.derive_value_from(key, kk, vv))
    resultsJson.append({'type': tpe, 'ID': i, 'EPKeyClass': key_class, 'EPValueClass': value_class, 'EPValue': value})

resultsJson = [item for item in resultsJson if not item['EPKeyClass'].endswith('ScheduleGroupProperty')]  # We don't want to fix schedule group property

# Question: Manually use this resource information in the optimization?
# cluster_information = read_resource_info(resourceinfo)
# print("CLUSTER:\n", cluster_information)

print("RESULT:")
pprint.pprint(resultsJson)

with open("results.out", "w") as file:
  file.write(json.dumps(resultsJson, indent=2))

# Visualize tree
# xgb.plot_tree(bst_opt)
# plt.show()
