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

import xgboost as xgb
import os

# simple example
# load file from text file, also binary buffer generated by xgboost
dtrain = xgb.DMatrix(os.path.join('dataset.txt.train'))
dtest = xgb.DMatrix(os.path.join('dataset.txt.test'))

# specify parameters via map, definition are same as c++ version
param = {'booster': 'gbtree', 'max_depth': 6, 'eta': 0.8, 'objective': 'reg:squarederror', "gamma": 0.8}

# specify validations set to watch performance
watchlist = [(dtest, 'eval'), (dtrain, 'train')]
num_round = 5
bst = xgb.train(param, dtrain, num_round, watchlist)

# this is prediction
preds = bst.predict(dtest)
labels = dtest.get_label()
for i in range(len(preds)):
	print('compare {} with {} = {}'.format(preds[i], labels[i], abs(preds[i] - labels[i]) < abs(labels[i]/2) or abs(preds[i] - labels[i]) < abs(preds[i]/2)))
print('error=%f' %
      (sum(1 for i in range(len(preds)) if abs(preds[i] - labels[i]) > abs(labels[i]/2) and abs(preds[i] - labels[i]) > abs(preds[i]/2)) /
       float(len(preds))))
bst.save_model('0001.model')
# dump model
bst.dump_model('dump.raw.txt')
# dump model with feature map
# bst.dump_model('dump.nice.txt', 'featmap.txt')
