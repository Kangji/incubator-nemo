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
  opts, args = getopt.getopt(sys.argv[1:], "hs:d:r:i:", ["dagsummary=", "dagpropertydir=", "resourceinfo=", "inputsize="])
except getopt.GetoptError:
  print('nemo_xgboost_property_optimization.py -s <dagsummary> -d <dagpropertydir> -r <resourceinfo>')
  sys.exit(2)
dagsummary = None
dagpropertydir = None
resourceinfo = None
inputsize = None
for opt, arg in opts:
  if opt == '-h':
    print('nemo_xgboost_property_optimization.py -s <dagsummary> -d <dagpropertydir> -r <resourceinfo>')
    sys.exit()
  elif opt in ("-s", "--dagsummary"):
    dagsummary = arg
  elif opt in ("-d", "--dagpropertydir"):
    dagpropertydir = arg
  elif opt in ("-r", "--resourceinfo"):
    resourceinfo = arg
  elif opt in ("-i", "--inputsize"):
    inputsize = arg

modelname = "nemo_rule_bst.model"

conn = None

try:
  host = "nemo-optimization.cabbufr3evny.us-west-2.rds.amazonaws.com"
  dbname = "nemo_optimization"
  dbuser = "postgres"
  dbpwd = "fake_password"
  conn = pg.connect(host=host, dbname=dbname, user=dbuser, password=dbpwd)
  print("Connected to the PostgreSQL DB.")
except:
  try:
    sqlite_file = "./optimization_db.sqlite"
    conn = sq.connect(sqlite_file)
    print("Connected to the SQLite DB.")
  except:
    print("I am unable to connect to the database. Try running the script with `./bin/xgboost_rule_optimization.sh`")

sql = "SELECT * from nemo_data"
cur = conn.cursor()
try:
  cur.execute(sql)
  print("Loaded data from the DB.")
except:
  print("I can't run " + sql)

rows = cur.fetchall()

keypairs = []
for row in rows:
  properties = row[6]
  rules = properties['rules']

  for rule in rules:
    key = f'{rule["name"]}'
    keypairs.append(key)
# print("Pre-processing properties..")

keyLE = preprocessing.LabelEncoder()
keyLE.fit(keypairs)
# print("KEYS:", list(self.keyLE.classes_))

encoded_rows = [f'{int(row[1]) // 1000} {":1 ".join(keyLE.transform([rule["name"] for rule in row[6]["rules"]]))}:1 {":0 ".join(keyLE.transform([key for key in keypairs if key not in row[6]["rules"]]))}:0' for row in rows]
cur.close()
conn.close()
print("Pre-processing complete")

write_rows_to_file('nemo_rule_optimization.out', encoded_rows)
ddata = xgb.DMatrix('nemo_rule_optimization.out')

avg_20_duration = np.mean(ddata.get_label()[:20])
print("average job duration: ", avg_20_duration)
allowance = avg_20_duration // 25  # 4%

row_size = len(encoded_rows)
print("total_rows: ", row_size)


## TRAIN THE MODEL (LOGISTIC REGRESSION)
dtrain = ddata.slice([i for i in range(0, row_size) if i % 7 != 6])  # mod is not 6
print("train_rows: ", dtrain.num_row())
dtest = ddata.slice([i for i in range(0, row_size) if i % 7 == 6])  # mod is 6
print("test_rows: ", dtest.num_row())
labels = dtest.get_label()

## Load existing booster, if it exists
bst_opt = xgb.Booster(model_file=modelname) if Path(modelname).is_file() else None
preds_opt = bst_opt.predict(dtest) if bst_opt is not None else None
error_opt = (sum(1 for i in range(len(preds_opt)) if abs(preds_opt[i] - labels[i]) > allowance) / float(
  len(preds_opt))) if preds_opt is not None and len(preds_opt) != 0 else 1
print('opt_error=%f' % error_opt)
min_error = error_opt

learning_rates = [0.01, 0.05, 0.1, 0.25, 0.5, 0.8]
for lr in learning_rates:
  param = {'max_depth': 6, 'eta': lr, 'verbosity': 0, 'objective': 'binary:logistic'}

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
fscore = bst_opt.get_fscore()
sorted_fscore = sorted(fscore.items(), key=lambda kv: kv[1])
for i in range(len(sorted_fscore)):
  print("\nSplit Value Histogram:")
  feature = sorted_fscore.pop()[0]
  print(feature, "=", keyLE.inverse_transform([int(feature[1:])]))
  hg = bst_opt.get_split_value_histogram(feature)
  print(hg)

df = bst_opt.trees_to_dataframe()
# print("Trees to dataframe")
# print(df)

# Visualize tree
# xgb.plot_tree(bst_opt, num_trees=0)
# plt.show()

