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
import psycopg2 as pg
import sqlite3 as sq
from sklearn import preprocessing
import xml.etree.ElementTree as ET


def preprocess_properties(properties, keypairs, values):
  vertex_properties = properties['vertex']
  edge_properties = properties['edge']
  tpe = properties['type']

  for vp in vertex_properties:
    i = f'{vp["ID"]}'
    key = f'{vp["EPKeyClass"]}/{vp["EPValueClass"]}'
    value = f'{vp["EPValue"]}'
    keypairs.append(f'{i},{key},{tpe}')
    if key not in values:
      values[key] = {'data': []}
    values[key]['isdigit'] = value.isdigit()
    if not value.isdigit():
      values[key]['data'].append(value)
  for ep in edge_properties:
    i = f'{ep["ID"]}'
    key = f'{ep["EPKeyClass"]}/{ep["EPValueClass"]}'
    value = f'{ep["EPValue"]}'
    keypairs.append(f'{i},{key},{tpe}')
    if key not in values:
      values[key] = {'data': []}
    values[key]['isdigit'] = value.isdigit()
    if not value.isdigit():
      values[key]['data'].append(value)
  return tpe


class Data:
  keyLE = None
  valueLE = {}

  def process_json(self, properties):
    vertex_properties = properties['vertex']
    edge_properties = properties['edge']
    tpe = properties['type']

    digit_keypairs = []
    digit_values = []
    keypairs_to_translate = []
    values_to_translate = {}

    for vp in vertex_properties:
      i = f'{vp["ID"]}'
      key = f'{vp["EPKeyClass"]}/{vp["EPValueClass"]}'
      value = f'{vp["EPValue"]}'
      if value.isdigit():
        digit_keypairs.append(f'{i},{key},{tpe}')
        digit_values.append(value)
      else:
        keypairs_to_translate.append(f'{i},{key},{tpe}')
        values_to_translate[key] = value

    for ep in edge_properties:
      i = f'{ep["ID"]}'
      key = f'{ep["EPKeyClass"]}/{ep["EPValueClass"]}'
      value = f'{ep["EPValue"]}'
      if value.isdigit():
        digit_keypairs.append(f'{i},{key},{tpe}')
        digit_values.append(value)
      else:
        keypairs_to_translate.append(f'{i},{key},{tpe}')
        values_to_translate[key] = value

    digit_key_ids = self.transform_keypairs_to_ids(digit_keypairs)
    digit_value_ids = digit_values
    translated_key_ids = self.transform_keypairs_to_ids(keypairs_to_translate)
    translated_value_ids = [self.transform_value_to_id(k, v) for k, v in values_to_translate.items()]

    properties_string = ""
    for ek, ev in zip(digit_key_ids, digit_value_ids):
      properties_string = properties_string + f' {ek}:{ev}'

    for ek, ev in zip(translated_key_ids, translated_value_ids):
      properties_string = properties_string + f' {ek}:{ev}'

    return properties_string.strip()


  def format_row(self, duration, inputsize, jvmmemsize, totalmemsize, properties):
    duration_in_sec = int(duration) // 1000
    inputsize_id = self.transform_keypair_to_id("env,inputsize,ignore")
    inputsize_in_10kb = int(inputsize) // 10240  # capable of expressing upto around 20TB with int range
    jvmmemsize_id = self.transform_keypair_to_id("env,jvmmemsize,ignore")
    jvmmemsize_in_mb = int(jvmmemsize) // 1048576
    totalmemsize_id = self.transform_keypair_to_id("env,totalmemsize,ignore")
    totalmemsize_in_mb = int(totalmemsize) // 1048576
    processed_properties = self.process_json(properties)
    return f'{duration_in_sec} {inputsize_id}:{inputsize_in_10kb} {jvmmemsize_id}:{jvmmemsize_in_mb} {totalmemsize_id}:{totalmemsize_in_mb} {processed_properties}'


  # ########################################################
  def load_data_from_db(self, tablename, dagpropertydir=None):
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
        print("I am unable to connect to the database. Try running the script with `./bin/xgboost_optimization.sh`")

    sql = "SELECT * from " + tablename
    cur = conn.cursor()
    try:
      cur.execute(sql)
      print("Loaded data from the DB.")
    except:
      print("I can't run " + sql)

    rows = cur.fetchall()

    keypairs = ["env,inputsize,ignore", "env,jvmmemsize,ignore", "env,totalmemsize,ignore"]  # 0 is the id for the row-wide variables
    values = {}
    for row in rows:
      preprocess_properties(row[5], keypairs, values)
    if dagpropertydir:
      preprocess_properties(self.load_property_json(dagpropertydir), keypairs, values)
    # print("Pre-processing properties..")

    self.keyLE = preprocessing.LabelEncoder()
    self.keyLE.fit(keypairs)
    # print("KEYS:", list(self.keyLE.classes_))

    for k, v in values.items():
      self.valueLE[k] = {}
      self.valueLE[k]['isdigit'] = v['isdigit']
      if not v['isdigit']:
        self.valueLE[k]['le'] = preprocessing.LabelEncoder()
        self.valueLE[k]['le'].fit(v['data'])
        # print("VALUE FOR ", k, ":", list(self.valueLE[k]['le'].classes_))

    processed_rows = [self.format_row(row[1], row[2], row[3], row[4], row[5]) for row in rows]
    cur.close()
    conn.close()
    print("Pre-processing complete")

    return processed_rows


  def transform_keypair_to_id(self, keypair):
    return self.transform_keypairs_to_ids([keypair])[0]


  def transform_keypairs_to_ids(self, keypairs):
    return self.keyLE.transform(keypairs)


  def transform_id_to_keypair(self, i):
    return self.transform_ids_to_keypairs([i])[0]


  def transform_ids_to_keypairs(self, ids):
    return [i.split(',') for i in self.keyLE.inverse_transform(ids)]


  def transform_value_to_id(self, epkey, value):
    return value if self.valueLE[epkey]['isdigit'] else self.valueLE[epkey]['le'].transform([value])[0]


  def transform_id_to_value(self, epkey, i):
    return i if self.valueLE[epkey]['isdigit'] else self.valueLE[epkey]['le'].inverse_transform([i])[0]


  def value_of_key_isdigit(self, epkey):
    return self.valueLE[epkey]['isdigit']


  def get_value_candidate(self, epkey):
    return None if self.value_of_key_isdigit(epkey) else self.valueLE[epkey]['le'].classes_


  def derive_value_from(self, epkey, split, tweak):
    value_candidates = self.get_value_candidate(epkey)
    max_value = len(value_candidates) - 1 if value_candidates is not None else None
    min_value = 0
    initial = int(split) if abs(split - int(split)) < 0.01 else split
    correction = tweak / 5 if abs(tweak / 5) > 1 else \
      (tweak / 2 if abs(tweak / 2) > 1 else
       (tweak if abs(tweak) > 1 else
        (0 if abs(tweak) < 0.5 else
         (1 if tweak > 0 else -1))))
    value = int(initial + correction)
    return max_value if max_value is not None and value > max_value else (min_value if value < min_value else value)


  def process_property_json(self, dagdirectory):
    return self.process_json(self.load_property_json(dagdirectory))


  def load_property_json(self, dagdirectory):
    jsonfile = 'ir-initial-properties.json'
    if jsonfile.startswith("/") and dagdirectory.endswith("/"):
      path = '{}{}'.format(dagdirectory, jsonfile[1:])
    elif jsonfile.startswith("/") or dagdirectory.endswith("/"):
      path = '{}{}'.format(dagdirectory, jsonfile)
    else:
      path = '{}/{}'.format(dagdirectory, jsonfile)
    with open(path) as data_file:
      return json.load(data_file)


# ########################################################
def read_resource_info(resource_info_path):
  data = []
  if resource_info_path.endswith("json"):
    with open(resource_info_path) as data_file:
      data.append(json.load(data_file))
  elif resource_info_path.endswith("xml"):
    root = ET.parse(resource_info_path).getroot()
    for cluster in root:
      nodes = []
      for node in cluster:
        attr = {}
        for a in node:
          attr[a.tag] = int(a.text) if a.text.isdigit() else a.text
        nodes.append(attr)
      data.append(nodes)
  return data


def write_rows_to_file(filename, rows):
  f = open(filename, 'w')
  for row in rows:
    f.write(row + "\n")
  f.close()
