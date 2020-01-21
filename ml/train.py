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

import numpy as np
import xgboost as xgb
import matplotlib.pyplot as plt

from inout import *
from tree import *


def train(data):
    modelname = "nemo_bst.model"

    row_size = data.count_rows_from_db()
    print(f'row_size = {row_size}')

    filename = 'nemo_optimization.{}.out'.format(row_size)
    keyfile_name = 'key.{}.pickle'.format(row_size)
    valuefile_name = 'value.{}.pickle'.format(row_size)
    if Path(filename).is_file() and Path(keyfile_name).is_file() and Path(valuefile_name).is_file():
        data.load_data_from_file(keyfile_name, valuefile_name)
    else:
        row_size = data.load_data_from_db('nemo_optimization')
        print(f'row_size = {row_size}')
    ddata = xgb.DMatrix(filename)

    print("total_rows: ", row_size)

    sample_avg_duration = np.mean(np.random.choice(ddata.get_label(), max(20, row_size // 20)))  # max of 20 or 5% of the data
    print(f"average job duration: {sample_avg_duration}")
    allowance = sample_avg_duration // 20  # 5%
    print(f"allowance: {allowance} seconds")

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
        param = {'max_depth': 10, 'eta': lr, 'verbosity': 0, 'objective': 'reg:squarederror'}

        watchlist = [(dtest, 'eval'), (dtrain, 'train')]
        num_round = row_size // 2
        bst = xgb.train(param, dtrain, num_round, watchlist, early_stopping_rounds=max(num_round // 10, 5))

        preds = bst.predict(dtest)
        error = (sum(1 for i in range(len(preds)) if abs(preds[i] - labels[i]) > allowance) / float(len(preds))) if len(
          preds) > 0 else 1.0
        print(f'error for learning rate {lr} is {error}')

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
        print(feature, "=", data.transform_id_to_keypair(int(feature[1:])))
        hg = bst_opt.get_split_value_histogram(feature)
        print(hg)

    df = bst_opt.trees_to_dataframe()
    print("Trees to dataframe")
    print(df)

    # Visualize tree
    xgb.plot_tree(bst_opt, num_trees=0)
    plt.show()

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

    # Handle the generated trees
    results = {}
    print("\nGenerated Trees:")
    for t in trees.values():
        results = dict_union(results, t.importance_dict())
        print(t)

    print("\nImportanceDict")
    print(json.dumps(results, indent=2))

    return trees


if __name__ == "__main__":
    train(Data())
