#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import random
from pathlib import Path

import numpy as np
import xgboost as xgb

from inout import *


def train(data, dagpropertydir=None):
    modelname = "nemo_bst.model"
    row_size = data.load_data_from_db('nemo_optimization.out', dagpropertydir) if dagpropertydir else data.load_data_from_db('nemo_optimization.out')
    ddata = xgb.DMatrix('nemo_optimization.out')

    print("total_rows: ", row_size)

    sample_avg_duration = np.mean(random.sample(ddata.get_label(), max(20, row_size // 20)))  # max of 20 or 5% of the data
    print("average job duration: ", sample_avg_duration)
    allowance = sample_avg_duration // 25  # 4%

    # TRAIN THE MODEL (REGRESSION)
    dtrain = ddata.slice([i for i in range(0, row_size) if i % 7 != 6])  # mod is not 6
    print("train_rows: ", dtrain.num_row())
    dtest = ddata.slice([i for i in range(0, row_size) if i % 7 == 6])  # mod is 6
    print("test_rows: ", dtest.num_row())
    labels = dtest.get_label()

    # Load existing booster, if it exists
    bst_opt = xgb.Booster(model_file=modelname) if Path(modelname).is_file() else None
    preds_opt = bst_opt.predict(dtest) if bst_opt is not None else None
    error_opt = (sum(1 for i in range(len(preds_opt)) if abs(preds_opt[i] - labels[i]) > allowance) / float(
      len(preds_opt))) if preds_opt is not None and len(preds_opt) != 0 else 1
    print('opt_error=%f' % error_opt)
    min_error = error_opt

    learning_rates = [0.01, 0.05, 0.1, 0.25, 0.5, 0.8]
    for lr in learning_rates:
        param = {'max_depth': 6, 'eta': lr, 'verbosity': 0, 'objective': 'reg:squarederror'}

        watchlist = [(dtest, 'eval'), (dtrain, 'train')]
        num_round = row_size // 10
        bst = xgb.train(param, dtrain, num_round, watchlist, early_stopping_rounds=5)

        preds = bst.predict(dtest)
        error = (sum(1 for i in range(len(preds)) if abs(preds[i] - labels[i]) > allowance) / float(len(preds))) if len(
          preds) > 0 else 1.0
        print('error=%f' % error)

        # Better booster
        if error <= error_opt:
            bst_opt = bst
            bst.save_model(modelname)
            min_error = error

    print('minimum error=%f' % min_error)

    # Check out the histogram by uncommenting the lines below
    fscore = bst_opt.get_fscore()
    sorted_fscore = sorted(fscore.items(), key=lambda kv: kv[1])
    for i in range(len(sorted_fscore)):
        print("\nSplit Value Histogram:")
        feature = sorted_fscore.pop()[0]
        print(feature, "=", data.transform_ids_to_keypairs([int(feature[1:])]))
        hg = bst_opt.get_split_value_histogram(feature)
        print(hg)

    df = bst_opt.trees_to_dataframe()
    print("Trees to dataframe")
    print(df)

    # Visualize tree
    # xgb.plot_tree(bst_opt, num_trees=0)
    # plt.show()

    return bst_opt


if __name__ == "__main__":
    train(Data())
