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

from pathlib import Path
import random
from math import sqrt

from sklearn.metrics import mean_squared_error
import numpy as np
import xgboost as xgb
import matplotlib.pyplot as plt

from inout import *
from tree import *


def train(data, dagsummary):
    row_size = data.count_rows_from_db(dagsummary)
    print(f'row_size = {row_size}')

    identity = '{}.{}'.format(dagsummary, row_size)
    modelname = "nemo_bst.{}.model".format(identity)
    filename = 'nemo_optimization.{}.out'.format(identity)
    keyfile_name = 'key.{}.pickle'.format(identity)
    valuefile_name = 'value.{}.pickle'.format(identity)
    if Path(filename).is_file() and Path(keyfile_name).is_file() and Path(valuefile_name).is_file():
        data.load_data_from_file(keyfile_name, valuefile_name)
    else:
        row_size = data.load_data_from_db(filename, keyfile_name, valuefile_name, dagsummary)
        print(f'preprocessed {row_size} rows')

    print(f'reading from {filename}')
    ddata = xgb.DMatrix(filename)

    print("total_rows: ", row_size)

    # TRAIN THE MODEL (REGRESSION)
    test_indices = sorted(random.sample(range(0, row_size), row_size // 6))
    dtrain = ddata.slice([i for i in range(0, row_size) if i not in test_indices])  # mod is not 6
    print("train_rows: ", dtrain.num_row())
    dtest = ddata.slice(test_indices)  # mod is 6
    print("test_rows: ", dtest.num_row())
    labels = dtest.get_label()

    # Load existing booster, if it exists
    bst_opt = xgb.Booster(model_file=modelname) if Path(modelname).is_file() else None
    preds_opt = bst_opt.predict(dtest) if bst_opt is not None else None
    error_opt = sqrt(mean_squared_error(preds_opt, labels))
    print('optimal rmse error=%f' % error_opt)
    min_error = error_opt

    learning_rates = [0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.7, 0.85]
    for lr in learning_rates:
        param = {'max_depth': 6, 'eta': lr, 'gamma': 0.01, 'verbosity': 0, 'objective': 'reg:squarederror'}

        watchlist = [(dtest, 'eval'), (dtrain, 'train')]
        num_round = max(row_size // 2, 10)
        bst = xgb.train(param, dtrain, num_round, watchlist, early_stopping_rounds=max(num_round // 10, 5))

        preds = bst.predict(dtest)
        error = sqrt(mean_squared_error(preds, labels))
        print(f'rmse error for learning rate {lr} is {error}')

        # Better booster
        if error <= min_error:
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
        print(feature, "=", data.transform_id_to_keypair(int(feature[1:])))
        hg = bst_opt.get_split_value_histogram(feature)
        print(hg)

    df = bst_opt.trees_to_dataframe()
    print("Trees to dataframe")
    print(df)

    # Visualize tree
    # xgb.plot_tree(bst_opt, num_trees=0)
    # plt.show()

    # Let's now use bst_opt
    # Build the tree ourselves
    trees = {}
    for index, row in df.iterrows():
        if row['Tree'] not in trees:  # Tree number = index
            trees[row['Tree']] = Tree(data)  # Simply has a reference to the data

        # translated_feature = data.transform_id_to_key(int(row['Feature'][1:])) if row['Feature'].startswith('f') else row['Feature']
        # print(translated_feature)
        trees[row['Tree']].add_node(row['ID'], row['Feature'], row['Split'], row['Yes'], row['No'], row['Missing'],
                                    row['Gain'])

    return trees


if __name__ == "__main__":
    dag_summary = input("Enter the DAG you want to train for : ")
    train(Data(), dagsummary=dag_summary)
