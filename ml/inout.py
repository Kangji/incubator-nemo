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


configuration_space = [
  'ClonedSchedulingProperty',
  'ParallelismProperty',
  'ResourceAntiAffinityProperty',
  'ResourceLocalityProperty',
  'ResourcePriorityProperty',
  'ResourceSiteProperty',
  'ResourceSlotProperty',
  'ResourceTypeProperty',

  'CompressionProperty',
  'DataFlowProperty',
  'DataPersistenceProperty',
  'DataStoreProperty',
  'PartitionerProperty',
  'PartitionSetProperty',
]


def preprocess_properties(properties, keypairs, values):
  tpe = properties['type']
  if tpe in ['id', 'pattern']:
    vertex_properties = properties['vertex']
    edge_properties = properties['edge']

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
  elif tpe == 'rule':
    props = properties['rules']
    key =
  return tpe


class Data:
  keyLE = None
  valueLE = {}
  loaded_properties = {}  # dictionary of key_id:value_ids (int:int)
  finalized_properties = []  # list of key_ids (int) - values can be accessed from loaded_properties

  def process_json(self, properties):
    vertex_properties = properties['vertex']
    edge_properties = properties['edge']
    tpe = properties['type']

    digit_keypairs = []
    digit_values = []
    digit_finalized = []
    keypairs_to_translate = []
    values_to_translate = []
    finalized = []

    for vp in vertex_properties:
      i = f'{vp["ID"]}'
      key = f'{vp["EPKeyClass"]}/{vp["EPValueClass"]}'
      value = f'{vp["EPValue"]}'
      is_finalized = f'{vp["isFinalized"]}'
      if value.isdigit():
        digit_keypairs.append(f'{i},{key},{tpe}')
        digit_values.append(value)
        if is_finalized and is_finalized == 'true':
          digit_finalized.append(True)
        else:
          digit_finalized.append(False)
      else:
        keypairs_to_translate.append(f'{i},{key},{tpe}')
        values_to_translate.append((key, value))
        if is_finalized and is_finalized == 'true':
          finalized.append(True)
        else:
          finalized.append(False)


    for ep in edge_properties:
      i = f'{ep["ID"]}'
      key = f'{ep["EPKeyClass"]}/{ep["EPValueClass"]}'
      value = f'{ep["EPValue"]}'
      is_finalized = f'{vp["isFinalized"]}'
      if value.isdigit():
        digit_keypairs.append(f'{i},{key},{tpe}')
        digit_values.append(value)
        if is_finalized and is_finalized == 'true':
          digit_finalized.append(True)
        else:
          digit_finalized.append(False)
      else:
        keypairs_to_translate.append(f'{i},{key},{tpe}')
        values_to_translate.append((key, value))
        if is_finalized and is_finalized == 'true':
          finalized.append(True)
        else:
          finalized.append(False)

    digit_key_ids = self.transform_keypairs_to_ids(digit_keypairs)
    digit_value_ids = digit_values
    translated_key_ids = self.transform_keypairs_to_ids(keypairs_to_translate)
    translated_value_ids = [self.transform_value_to_id(k, v) for k, v in values_to_translate]

    properties_string = ""
    for ek, ev, ef in zip(digit_key_ids, digit_value_ids, digit_finalized):
      properties_string = properties_string + f' {ek}:{ev}'
      if ef:
        self.finalized_properties.append(int(ek))

    for ek, ev, ef in zip(translated_key_ids, translated_value_ids, finalized):
      properties_string = properties_string + f' {ek}:{ev}'
      if ef:
        self.finalized_properties.append(int(ek))

    return properties_string.strip()


  def format_row(self, duration, inputsize, jvmmemsize, totalmemsize, dagsummary, properties):
    duration_in_sec = int(duration) // 1000
    inputsize_id = self.transform_keypair_to_id("env,inputsize,ignore")
    inputsize_in_10kb = int(inputsize) // 10240  # capable of expressing upto around 20TB with int range
    jvmmemsize_id = self.transform_keypair_to_id("env,jvmmemsize,ignore")
    jvmmemsize_in_mb = int(jvmmemsize) // 1048576
    totalmemsize_id = self.transform_keypair_to_id("env,totalmemsize,ignore")
    totalmemsize_in_mb = int(totalmemsize) // 1048576
    dagsummary_id = self.transform_keypair_to_id("env,dagsummary,ignore")
    dagsummary_value_id = self.transform_value_to_id('dagsummary',dagsummary)
    processed_properties = self.process_json(properties)
    return f'{duration_in_sec} {inputsize_id}:{inputsize_in_10kb} {jvmmemsize_id}:{jvmmemsize_in_mb} {totalmemsize_id}:{totalmemsize_in_mb} {dagsummary_id}:{dagsummary_value_id} {processed_properties}'


  # ########################################################
  def load_data_from_db(self, dagsummary, dagpropertydir=None):
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

    sql = "SELECT * from nemo_data"
    cur = conn.cursor()
    try:
      cur.execute(sql)
      print("Loaded data from the DB.")
    except:
      print("I can't run " + sql)

    rows = cur.fetchall()

    keypairs = ["env,inputsize,ignore", "env,jvmmemsize,ignore", "env,totalmemsize,ignore"]  # 0 is the id for the row-wide variables
    values = {}
    if dagsummary:
      key = 'dagsummary'
      keypairs.append('env,{},ignore'.format(key))
      if key not in values:
        values[key] = {'data': []}
      values[key]['isdigit'] = False
      for row in rows:
        values[key]['data'].append(row[5])
    for row in rows:
      preprocess_properties(row[6], keypairs, values)
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

    processed_rows = [self.format_row(row[1], row[2], row[3], row[4], row[5], row[6]) for row in rows]
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


  def get_value_candidates(self, epkey):
    return None if self.value_of_key_isdigit(epkey) else self.valueLE[epkey]['le'].classes_


  def derive_value_from(self, keyid, epkey, split, tweak):
    if not self.is_in_configuration_space(keyid):  # ex. ParallelismProperty or finalized property
      return None

    value_candidates = self.get_value_candidates(epkey)
    max_value = len(value_candidates) - 1 if value_candidates else None
    min_value = 0
    initial = int(round(split)) if abs(split - int(split)) < 0.01 else split
    correction = tweak / 3 if abs(tweak / 3) >= 1 else (tweak / 2 if abs(tweak / 2) >= 1 else tweak)
    loaded_property_value = self.get_loaded_property(keyid)
    if loaded_property_value:
      if (loaded_property_value < split and tweak < 0) or (loaded_property_value > split and tweak > 0):
        value = loaded_property_value
      else:
        value = int(round(initial + correction))
    else:
      value = int(round(initial + correction))
    return max_value if max_value and value > max_value else (min_value if value < min_value else value)


  def is_in_configuration_space(self, key):  #key is recommended to be the feature id, but it could also be the name (e.g. ParallelismProperty)
    if key and isinstance(key, int):
      i, k, tpe = self.transform_id_to_keypair(key)
      return self.is_in_configuration_space(k) and not self.is_finalized_property(key)
    elif key and isinstance(key, str):
      if key.startswith('f') and key[1:].isdigit():  # ex. f55
        return self.is_in_configuration_space(int(key[1:]))
      elif key.startswith('org'):  # ex. org.apache.nemo.common.ir.vertex.executionproperty.ParallelismProperty/java.lang.Integer
        return self.is_in_configuration_space(key.split('/')[0].split('.')[-1])
      return key in configuration_space
    else:
      return False


  def get_loaded_property(self, key):
    if isinstance(key, str) and key.startswith('f') and key[1:].isdigit():
      return self.get_loaded_property(int(key[1:]))
    elif isinstance(key, int) and key in self.loaded_properties:
      return self.loaded_properties[key]
    else:
      return None


  def is_finalized_property(self, key):
    if isinstance(key, str) and key.startswith('f') and key[1:].isdigit():
      return self.is_finalized_property(int(key[1:]))
    elif isinstance(key, int):
      return key in self.finalized_properties
    else:
      return False


  def process_property_json(self, dagdirectory):
    property_json = self.load_property_json(dagdirectory)
    processed_json_string = self.process_json(property_json)

    inputsize_id = self.transform_keypair_to_id("env,inputsize,ignore")
    inputsize_in_10kb = int(property_json['inputsize']) // 10240  # capable of expressing upto around 20TB with int range
    jvmmemsize_id = self.transform_keypair_to_id("env,jvmmemsize,ignore")
    jvmmemsize_in_mb = int(property_json['jvmmemsize']) // 1048576
    totalmemsize_id = self.transform_keypair_to_id("env,totalmemsize,ignore")
    totalmemsize_in_mb = int(property_json['totalmemsize']) // 1048576
    dagsummary_id = self.transform_keypair_to_id("env,dagsummary,ignore")
    dagsummary_value_id = self.transform_value_to_id('dagsummary',property_json['dagsummary'])

    processed_env = f'{inputsize_id}:{inputsize_in_10kb} {jvmmemsize_id}:{jvmmemsize_in_mb} {totalmemsize_id}:{totalmemsize_in_mb} {dagsummary_id}:{dagsummary_value_id}'
    for e in processed_env.split():
      e = e.split(':')
      self.finalized_properties.append(int(e[0]))

    processed = f'{processed_env} {processed_json_string}'
    for e in processed.split():
      e = e.split(':')
      self.loaded_properties[int(e[0])] = int(e[1])

    return processed

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
