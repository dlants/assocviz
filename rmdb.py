#!/usr/bin/python
# This file parses a plain text dataset, and creates a sqlite db
# Created by Denis Lantsman
# denis.lantsman@gmail.com
import sys
import os
import time
import csv
import xml.dom.minidom as minidom
import sqlite3
import re
import argparse
import matplotlib.pyplot as plt
import numpy as np

class Objective:
  def __init__(self, obj_tuple):
    self.obj_id, self.obj_idx, self.name, self.short_name = obj_tuple 
    self.modules = []
    self.selected = False

  def __str__(self):
    return str(self.obj_idx) + '|' + self.obj_id

  def __repr__(self):
    return str(self.obj_idx) + '|' + self.obj_id

class Module:
  def __init__(self, module_tuple):
    self.module_id, self.obj_id, self.module_idx, self.name = module_tuple
    self.meta_ids = []
    self.selected = False

  def __str__(self):
    return str(self.module_idx) + " | " + self.module_id

  def __repr__(self):
    return str(self.module_idx) + " | " + self.module_id

class RMDB:
  def __init__(self, path):
    self.conn = sqlite3.connect(path)
    self.c = self.conn.cursor()
    self.curriculum = []
    self.meta_id_map = {}
    self.c.execute('''select * from objectives order by obj_idx''')
    # compose objectives
    objs = self.c.fetchall()
    for obj_tuple in objs:
      obj = Objective(obj_tuple)

      self.c.execute('''select * from modules where obj_id = ? order by module_idx''', (obj.obj_id,))
      modules = self.c.fetchall()
      # compose modules
      for module_tuple in modules:
        mod = Module(module_tuple)

        # compose metaids
        self.c.execute('''SELECT meta_id FROM problems where module_id = ? order by meta_idx desc''', 
          (mod.module_id,))

        metas = self.c.fetchall()

        for meta_id in metas:
          mod.meta_ids.append(meta_id[0])
          self.meta_id_map[meta_id[0]] = (obj.obj_id, mod.module_id)

        obj.modules.append(mod)
      self.curriculum.append(obj)

  def query_all(self):
    '''get all of the transactions.'''
    q_str = '''select * from transactions order by user_id, meta_id, start'''
    self.c.execute(q_str)

    user_id = None
    meta_id = None
    attempt = 0

    self.query = {}

    while True:
      transaction  = self.c.fetchone()
      if not transaction:
        break 
      if not transaction[0] == user_id:
        user_id = transaction[0]
        meta_id = None
        attempt = 0
        self.query[user_id] = []

      if not transaction[2] == meta_id:
        meta_id = transaction[2] 
        attempt = 0

      if attempt > 1:
        continue

      self.query[user_id].append(Event(transaction, attempt))
      attempt += 1

  def token(self, transaction, attempt):
    '''Generate a set token for the given transaction'''
    return transaction[2], attempt, transaction[6] == 'YES', transaction[7] == 'YES'

  def query(self, max_metaids = -1):
    '''Query the db by my selection.
    
    Returns a list of frozensets, and sets internal representations to translate
    them back to graph structure.'''

    meta_ids = []
    count = 0

    for obj in self.curriculum:
      for module in obj.modules:
        if obj.selected or module.selected:
          meta_ids.extend(module.meta_ids)

    if max_metaids > 0:
      meta_ids = meta_ids[:max_metaids]

    q_str = '''SELECT * FROM transactions WHERE meta_id IN ({seq}) order by 
      user_id, meta_id, start'''.format(seq=','.join(['?']*len(meta_ids)))
    self.c.execute(q_str, meta_ids)
    transactions = self.c.fetchall()

    user_id = None
    meta_id = None
    attempt = 0

    baskets = []
    tokens = set([])

    # scan through the output, creating tokens for the output and mappings for
    # the inverse relationship
    # output is sorted in user_id, meta_id, start order.
    for transaction in transactions:
      # new user (new basket)
      if not transaction[0] == user_id:
        user_id = transaction[0]
        meta_id = None
        attempt = 0
        baskets.append([])

      # new question (reset attempt)
      if not transaction[2] == meta_id:
        meta_id = transaction[2] 
        attempt = 0

      if attempt > 1:
        continue

      token = self.token(transaction, attempt)
      baskets[-1].append(token)
      tokens.add(token)
      attempt += 1

    print "{} baskets".format(len(baskets))
    print "{} unique tokens".format(len(tokens))

    lens = [len(x) for x in baskets]
    print "{} longest sequence".format(max([len(x) for x in baskets]))

    self.token_to_idx = {x[1]:x[0] for x in enumerate(tokens)}
    self.idx_to_token = {x[0]:x[1] for x in enumerate(tokens)}

    # shorten strings to integers to make comparisons faster.
    baskets_short = [[self.token_to_idx[token] for token in transaction] for transaction in baskets]
 
    return baskets_short

  def select(self, obj_idxs = None, module_id = None):
    if obj_idxs:
      for obj in self.curriculum:
        # if obj is selected, all modules are selected
        if obj_idxs == -1 or obj.obj_idx in obj_idxs:
          obj.selected = True
      return

    for obj in self.curriculum:
      for mod in obj.modules:
        if mod.module_id == module_id:
          # if module is selected, all metaids are selected
          mod.selected = True

  def summary(self):
    '''Print out summary statistics and provide plots of the database.'''
    print "db summary:"
    self.c.execute('select count(*) from objectives')
    print "{} objectives".format(self.c.fetchone()[0])
    self.c.execute('select count(*) from modules')
    print "{} modules".format(self.c.fetchone()[0])
    self.c.execute('select count(*) from problems')
    print "{} problems".format(self.c.fetchone()[0])
    self.c.execute('select count(*) from transactions')
    print "{} transactions".format(self.c.fetchone()[0])
    print "---------------------------------------------"
    self.c.execute('select count(distinct meta_id) from transactions')
    print "{} unique metaids".format(self.c.fetchone()[0])
    self.c.execute('select count(distinct user_id) from transactions')
    print "{} unique userids".format(self.c.fetchone()[0])
    print "---------------------------------------------"
    self.c.execute('select obj_id from objectives order by obj_idx')
    objs = self.c.fetchall()

    counts = {}
    for obj in objs:
      print "on obj {}".format(obj)
      counts[obj] = {}
      self.c.execute('select module_id from modules where obj_id = (?)', obj)
      modules = self.c.fetchall()
      for module in modules:
        counts[obj][module] = {}
        q_str = '''select count(*) from transactions where meta_id in 
        (select meta_id from problems where module_id = (?))'''
        self.c.execute(q_str, module)
        counts[obj][module]['count'] = self.c.fetchone()[0]
        q_str = '''select count(distinct user_id) from transactions where meta_id in 
        (select meta_id from problems where module_id = (?))'''
        self.c.execute(q_str, module)
        counts[obj][module]['users'] = self.c.fetchone()[0]

    obj_counts = np.zeros(len(objs))
    obj_users = np.zeros(len(objs))
    module_counts = []
    for idx, obj in enumerate(objs):
      c = 0
      u = 0
      for module in counts[obj].keys():
        c += counts[obj][module]['count']
        u += counts[obj][module]['users']
      obj_counts[idx] = c
      obj_users[idx] = u

    plt.subplot(211)
    plt.bar(range(len(obj_counts)), obj_counts)
    plt.xlabel('obj number')
    plt.ylabel('number of transactions')
    plt.subplot(212)
    plt.bar(range(len(obj_users)), obj_users)
    plt.xlabel('obj number')
    plt.ylabel('number of unique users')
    plt.show()
    import pdb; pdb.set_trace()

  def close(self):
    self.conn.close()

  def print_seq(self, seq):
    for idx in seq:
      token = self.idx_to_token[idx]
      context = self.meta_id_map[token[0]]
      print "obj: " + context[0] + " module: " + context[1] + " metaid: " +\
        token[0] + " attempt: " + str(token[1]) + " hint: " + str(token[2]) + " correct: "\
        + str(token[3])


if __name__ == "__main__":
  # test code
  rmdb = RMDB('data/rm.db')
  rmdb.summary()
  rmdb.select([0])
  rmdb.query()
