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

import json


def stringify_num(num):
  return str(round(num, 2))


def dict_union(d1, d2):
  for k, v in d2.items():
    if k in d1:
      if type(d1[k]) is dict and type(v) is dict:  # When same 'feature'
        d1[k] = dict_union(d1[k], v)
      else:  # When same 'split'
        d1[k] = d1[k] + v
    elif type(v) is dict:  # When no initial data
      d1[k] = v
    else:  # k = split, v = diff. include if it does not violate.
      if v > 0 > max(d1.values()) and k < max(d1.keys()):  # If no positive values yet
        d1[k] = v
      elif v > max(d1.values()) > 0:  # Update if greater value
        max_key = max(d1, key=lambda key: d1[key])
        del d1[max_key]
        d1[k] = v
      elif v < 0 < min(d1.values()) and min(d1.keys()) < k:  # If no negative values yet
        d1[k] = v
      elif v < min(d1.values()) < 0:  # Update if smaller value
        min_key = min(d1, key=lambda key: d1[key])
        del d1[min_key]
        d1[k] = v
  return d1


class Tree:
  root = None
  idx_to_node = {}

  def append_to_dict_if_not_exists(self, idx, node):
    if idx not in self.idx_to_node:
      self.idx_to_node[idx] = node

  def addNode(self, index, feature_id, split, yes, no, missing, value):
    n = None
    if self.root == None:
      self.root = Node(None)
      n = self.root
      self.append_to_dict_if_not_exists(index, n)
    else:
      n = self.idx_to_node[index]

    self.append_to_dict_if_not_exists(yes, Node(n))
    self.append_to_dict_if_not_exists(no, Node(n))
    self.append_to_dict_if_not_exists(missing, Node(n))
    n.addAttributes(index, feature_id, split, yes, no, missing, value, self.idx_to_node)

  def importanceDict(self):
    return self.root.importanceDict()

  def __str__(self):
    return json.dumps(json.loads(str(self.root)), indent=4)


class Node:
  parent = None
  index = None

  feature = None
  split = None
  left = None
  right = None
  missing = None

  value = None

  reachable = None  # Dynamic programming

  def __init__(self, parent):
    self.parent = parent

  def addAttributes(self, index, feature_id, split, yes, no, missing, value, idx_to_node):
    self.index = index
    if feature_id == 'Leaf':
      self.value = value
    else:
      self.feature = feature_id
      self.split = split
      self.left = idx_to_node[yes]
      self.right = idx_to_node[no]
      self.missing = idx_to_node[missing]

  def isLeaf(self):
    return self.value != None

  def isRoot(self):
    return self.parent == None

  def getIndex(self):
    return self.index

  def getLeft(self):
    return self.left

  def getRight(self):
    return self.right

  def getMissing(self):
    return self.missing

  def isReachable(self):  # Is the node reachable from the root, following the given conditions?
    if self.reachable is not None:  # dynamic programming
      return self.reachable
    elif self.isRoot():
      self.reachable = True
      return self.reachable
    elif not self.parent.isReachable():
      self.reachable = False
      return self.reachable
    else:
      feature = self.parent.feature  # feature to look at
      conditions = []
      p = self.parent
      while not p.isRoot():  # aggregate the conditions of the parents for the given feature
        if p.parent.feature is feature:
          is_less_than = False if p is p.parent.getRight() else True
          conditions.append((is_less_than, p.parent.split))
        p = p.parent
      must_be_greater_than = None
      must_be_less_than = None
      for condition in conditions:  # check conditions of the parents for the given feature
        if condition[0] and (not must_be_less_than or condition[1] < must_be_less_than):  # less than split: find min
          must_be_less_than = condition[1]
        elif not condition[1] and (not must_be_greater_than or condition[1] > must_be_greater_than):  # gt split: find max
          must_be_greater_than = condition[1]
      is_less_than = False if self is self.parent.getRight() else True  # see if it meets the given condition
      if (is_less_than and must_be_greater_than and self.parent.split < must_be_greater_than) or (not is_less_than and must_be_less_than and self.parent.split > must_be_less_than) or (must_be_less_than and must_be_greater_than and must_be_greater_than > must_be_less_than):
        self.reachable = False
        return self.reachable
      else:
        self.reachable = True
        return self.reachable

  def getApprox(self):
    if self.isLeaf():
      return self.value
    else:
      lapprox = self.left.getApprox()
      rapprox = self.right.getApprox()
      if not self.left.isReachable():
        return rapprox
      elif not self.right.isReachable():
        return lapprox
      else:
        return (lapprox + rapprox) / 2

  def getDiff(self):
    lapprox = self.left.getApprox()
    rapprox = self.right.getApprox()
    if (rapprox != 0 and abs(lapprox / rapprox) < 0.04) or (lapprox != 0 and abs(rapprox / lapprox) < 0.04):
      return 0  # ignore: too much difference (4%) - dangerous for skews
    return lapprox - rapprox

  def importanceDict(self):
    if self.isLeaf():
      return {}
    else:
      d = {}
      d[self.feature] = {self.split: self.getDiff()}
      return dict_union(d, dict_union(self.left.importanceDict(), self.right.importanceDict()))

  def __str__(self):
    if self.isLeaf():
      return f'{stringify_num(self.value)}'
    else:
      left = str(self.left) if self.left.isLeaf() else json.loads(str(self.left))
      right = str(self.right) if self.right.isLeaf() else json.loads(str(self.right))
      return json.dumps({self.index: f'{self.feature}' + '{' + stringify_num(self.getApprox()) + ',' + stringify_num(
        self.getDiff()) + '}', 'L' + self.left.getIndex(): left, 'R' + self.right.getIndex(): right})
