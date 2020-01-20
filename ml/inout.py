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

import json, os.path, pickle
import psycopg2 as pg
import sqlite3 as sq
from tqdm import tqdm
import xml.etree.ElementTree as ET


configuration_space = [
  # 'ClonedSchedulingProperty',
  'ParallelismProperty',
  # 'ResourceAntiAffinityProperty',
  # 'ResourceLocalityProperty',
  # 'ResourcePriorityProperty',
  # 'ResourceSiteProperty',
  # 'ResourceSlotProperty',
  # 'ResourceTypeProperty',

  # 'CompressionProperty',
  # 'DataFlowProperty',
  # 'DataPersistenceProperty',
  # 'DataStoreProperty',
  # 'PartitionerProperty',
  # 'PartitionSetProperty',
]


# As a result of the method, keypairs are filled with (id,EPKey,Type) tuples and
# values are filled with the corresponding values for each key tuple and whether it is a digit or not
def aggregate_dict_properties_json(properties, keypairs, values):
    vertex_properties = properties['vertex']
    edge_properties = properties['edge']
    tpe = properties['type']
    rules = properties['rules'] if properties['rules'] else []

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
    for rule in rules:
        key = f'{rule["name"]}'
        keypairs.append(f'rule,{key},ignore')
        if key not in values:
            values[key] = {'data': []}
        values[key]['isdigit'] = False
        values[key]['data'].append(True)  # true if it exists
    return tpe


class Data:
    idx_to_keypair = {}
    keypair_to_idx = {}
    idx_to_value_by_key = {}
    value_to_idx_by_key = {}
    loaded_properties = {}  # dictionary of key_id:value_ids (int:int)
    finalized_properties = []  # list of key_ids (int) - values can be accessed from loaded_properties

    host = "35.194.96.120"
    dbname = "nemo_optimization"
    dbuser = "postgres"
    dbpwd = "fake_password"

    def process_json(self, id, key, tpe, value, is_finalized):
        res = ''
        keypair = f'{id},{key},{tpe}'

        if keypair in self.keypair_to_idx:
            key_idx = self.keypair_to_idx[keypair]
        else:
            key_idx = len(self.idx_to_keypair)
            self.idx_to_keypair[key_idx] = keypair
            self.keypair_to_idx[keypair] = key_idx

        if key not in self.idx_to_value_by_key and key not in self.value_to_idx_by_key:
            self.idx_to_value_by_key[key] = {}
            self.value_to_idx_by_key[key] = {}

        isdigit = value.isdigit()
        self.idx_to_value_by_key[key]['isdigit'] = isdigit
        self.value_to_idx_by_key[key]['isdigit'] = isdigit
        if isdigit:
            res = f'{key_idx}:{value}'
        elif value in self.value_to_idx_by_key[key]:
            res = f'{key_idx}:{self.value_to_idx_by_key[key][value]}'
        else:
            value_idx = len(self.idx_to_value_by_key[key])
            self.idx_to_value_by_key[key][value_idx] = value
            self.value_to_idx_by_key[key][value] = value_idx
            res = f'{key_idx}:{value_idx}'

        if is_finalized and is_finalized == 'true':
            self.finalized_properties.append(key_idx)

        return res

    def format_row(self, duration, inputsize, jvmmemsize, totalmemsize, dagsummary, properties):
        vertex_properties = properties['vertex']
        edge_properties = properties['edge']
        tpe = properties['type']
        rules = properties['rules'] if properties['rules'] else []
        resource_data = properties['executor_info']

        properties_string = []

        duration_in_sec = int(duration) // 1000
        properties_string.append(str(duration_in_sec))
        inputsize_in_10kb = int(inputsize) // 10240  # capable of expressing upto around 20TB with int range
        properties_string.append(self.process_json('env', 'inputsize', 'ignore', str(inputsize_in_10kb), 'true'))
        jvmmemsize_in_mb = int(jvmmemsize) // 1048576
        properties_string.append(self.process_json('env', 'jvmmemsize', 'ignore', str(jvmmemsize_in_mb), 'true'))
        totalmemsize_in_mb = int(totalmemsize) // 1048576
        properties_string.append(self.process_json('env', 'totalmemsize', 'ignore', str(totalmemsize_in_mb), 'true'))
        properties_string.append(self.process_json('env', 'dagsummary', 'ignore', dagsummary, 'true'))

        if resource_data:
            total_executor_num = extract_total_executor_num(resource_data)
            properties_string.append(self.process_json('env', 'total_executor_num', 'ignore', str(total_executor_num), 'true'))

            total_cores = extract_total_cores(resource_data)
            properties_string.append(self.process_json('env', 'total_cores', 'ignore', str(total_cores), 'true'))

            avg_memory_mb_per_executor = extract_avg_memory_mb_per_executor(resource_data)
            properties_string.append(self.process_json('env', 'avg_memory_mb_per_executor', 'ignore', str(avg_memory_mb_per_executor), 'true'))

        for p in vertex_properties + edge_properties:
            i = f'{p["ID"]}'
            key = f'{p["EPKeyClass"]}/{p["EPValueClass"]}'
            value = f'{p["EPValue"]}'
            is_finalized = f'{p["isFinalized"]}'
            properties_string.append(self.process_json(i, key, tpe, value, is_finalized))

        for rule in rules:
            key = f'{rule["name"]}'
            properties_string.append(self.process_json('rule', key, 'ignore', 'true', 'false'))

        return ' '.join(properties_string).strip()

    # ########################################################
    def count_rows_from_db(self):
        conn = pg.connect(host=self.host, dbname=self.dbname, user=self.dbuser, password=self.dbpwd)
        print("Connected to the PostgreSQL DB.")
        sql = "SELECT count(*) from nemo_data"
        cur = conn.cursor()
        try:
            cur.execute(sql)
            print("Loaded data from the DB.")
        except:
            print("I can't run " + sql)

        return cur.fetchone()[0]

    def load_data_from_file(self, keyfile_name, valuefile_name):
        print("Loading pre-processed properties..")
        with open(keyfile_name, 'rb') as fp:
            self.idx_to_keypair = pickle.load(fp)
            self.keypair_to_idx = dict([reversed(i) for i in self.idx_to_keypair.items()])
            print(f'loaded {len(self.idx_to_keypair)} key pairs from {keyfile_name}')
        with open(valuefile_name, 'rb') as fp:
            self.idx_to_value_by_key = pickle.load(fp)
            for k, i_t_v in self.idx_to_value_by_key.items():
                self.value_to_idx_by_key[k] = dict([reversed(i) for i in i_t_v.items()])
                if 'isdigit' in self.idx_to_value_by_key[k]:
                    self.value_to_idx_by_key[k]['isdigit'] = self.idx_to_value_by_key[k]['isdigit']
            print(f'loaded values for {len(self.idx_to_value_by_key)} key pairs from {valuefile_name}')

    def load_data_from_db(self, destionation_file='nemo_optimization', dagpropertydir=None):
        conn = None

        try:
            conn = pg.connect(host=self.host, dbname=self.dbname, user=self.dbuser, password=self.dbpwd)
            print("Connected to the PostgreSQL DB.")
        except:
            try:
                sqlite_file = "./optimization_db.sqlite"
                conn = sq.connect(sqlite_file)
                print("Connected to the SQLite DB.")
            except:
                print("I am unable to connect to the database. Try running the script with `./bin/xgboost_property_optimization.sh`")

        sql = "SELECT id, duration, inputsize, jvmmemsize, memsize, dagsummary, properties from nemo_data"
        cur = conn.cursor()
        try:
            cur.execute(sql)
            print("Loaded data from the DB.")
        except:
            print("I can't run " + sql)

        row_size = cur.rowcount
        keyfile_name = 'key.{}.pickle'.format(row_size)
        valuefile_name = 'value.{}.pickle'.format(row_size)
        file_name = '{}.{}.out'.format(destionation_file, row_size)

        if os.path.isfile(keyfile_name) and os.path.isfile(valuefile_name):
            self.load_data_from_file(keyfile_name, valuefile_name)

        # add the info for the current one
        if dagpropertydir:
            properties = self.load_property_json(dagpropertydir)
            tpe = properties['type']
            rules = properties['rules'] if properties['rules'] else []
            for p in properties['vertex'] + properties['edge']:
                i = f'{p["ID"]}'
                key = f'{p["EPKeyClass"]}/{p["EPValueClass"]}'
                value = f'{p["EPValue"]}'
                is_finalized = f'{p["isFinalized"]}'
                self.process_json(i, key, tpe, value, is_finalized)
            for rule in rules:
                key = f'{rule["name"]}'
                self.process_json('rule', key, 'ignore', 'true', 'false')

        print("Pre-processing properties..")
        with open(file_name, 'w') as f:
            for row in tqdm(cur, total=row_size):
                f.write('{}\n'.format(self.format_row(row[1], row[2], row[3], row[4], row[5], row[6])))

        cur.close()
        conn.close()

        with open(keyfile_name, 'wb') as fp:
          pickle.dump(self.idx_to_keypair, fp, protocol=pickle.HIGHEST_PROTOCOL)
          print(f'dumped {len(self.idx_to_keypair)} keys to pickle to {keyfile_name}')
        with open(valuefile_name, 'wb') as fp:
          pickle.dump(self.idx_to_value_by_key, fp, protocol=pickle.HIGHEST_PROTOCOL)

        print("Pre-processing complete")

        return row_size

    def transform_keypair_to_id(self, keypair):
        return self.keypair_to_idx[keypair]

    def transform_keypairs_to_ids(self, keypairs):
        return [self.keypair_to_idx[keypair] for keypair in keypairs]

    def transform_id_to_keypair(self, i):
        return self.idx_to_keypair[i].split(',')

    def transform_ids_to_keypairs(self, ids):
        return [self.idx_to_keypair[idx].split(',') for idx in ids]

    def transform_value_to_id(self, epkey, value):
        return value if self.value_to_idx_by_key[epkey]['isdigit'] else self.value_to_idx_by_key[epkey][value]

    def transform_id_to_value(self, epkey, i):
        return i if self.idx_to_value_by_key[epkey]['isdigit'] else self.idx_to_value_by_key[epkey][i]

    def value_of_key_isdigit(self, epkey):
        return self.idx_to_value_by_key[epkey]['isdigit']

    def get_value_candidates(self, epkey):
        return None if self.value_of_key_isdigit(epkey) else [i for i in self.value_to_idx_by_key[epkey] if i != 'isdigit']

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

    def process_individual_property_json(self, dagdirectory):
        property_json = self.load_property_json(dagdirectory)
        processed_json_string = self.process_json_to_string(property_json)

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
def read_resource_info(resource_info):
    # resource_info can either be a path to a json/xml file or the json string itself
    data = []
    if resource_info.endswith("json"):
        with open(resource_info) as data_file:
            data = data + json.load(data_file)
    elif resource_info.endswith("xml"):
        root = ET.parse(resource_info).getroot()
        for cluster in root:
            for node in cluster:
                attr = {}
                for a in node:
                    attr[a.tag] = int(a.text) if a.text.isdigit() else a.text
                data.append(attr)
    else:
        data = data + json.loads(resource_info)
    return data


# resource_data is the result of the method above, read_resource_info
def extract_total_executor_num(resource_data):
    total_executor_num = 0
    for node in resource_data:
        total_executor_num = total_executor_num + node['num'] if 'num' in node else 1
    return total_executor_num


def extract_total_cores(resource_data):
    total_cores = 0
    for node in resource_data:
        total_cores = total_cores + int(node['capacity'] * (node['num'] if 'num' in node else 1))
    return total_cores


def extract_avg_memory_mb_per_executor(resource_data):
    total_memory = 0
    for node in resource_data:
        total_memory = total_memory + int(node['memory_mb'] * (node['num'] if 'num' in node else 1))
    return total_memory // extract_total_executor_num(resource_data)
