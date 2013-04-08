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

class Event:
  def __init__(self, transaction, attempt_idx):
    '''Stores the info for an event.
    '''
    self.attempt_idx = attempt_idx
    self.user_id, self.pid, self.meta_id, self.problem_id = transaction[0:4]
    self.start = time.localtime(transaction[4])
    self.duration = transaction[5]
    self.hint = (transaction[6] == u'YES')
    self.correct = (transaction[7] == u'YES')

  def token(self):
    '''Return a tuple representing an event token, to be used in association
    mining.'''
    out = ''
    out += str(self.meta_id)
    out += "|" + str(self.attempt_idx)
    if self.correct:
      out += '|correct'
    else:
      out += '|wrong'
    if self.hint:
      out += '|hint'
    else:
      out += '|nohint'
    
    return out

  def __str__(self):
    time_str = time.strftime("%H:%M:%S - %m/%d/%Y", self.start)

    if len(sys.argv) == 2:
    	return '{},{},{},{},{},{},{}'.format(time_str, self.duration, 
            self.attempt_idx, self.user_id, self.meta_id, self.correct, self.hint)
    else:
        return '[{} - {} s - attempt {} | user_id: {} meta_id: {} | correct: {} hint: {}]'\
            .format(time_str, self.duration, self.idx, self.user_id, self.meta_id, self.correct, self.hint)

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

  def query(self, max_metaids = -1):
    '''Query the db by my selection.
    
    sets self.query to a dictionary, mapping objective -> module -> a list of 
    events sorted by uid, metaid and start time'''


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

    self.query = {}

    for transaction in transactions:
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

  def close(self):
    self.conn.close()

if __name__ == "__main__":
  # test code
  rmdb = RMDB('data/rm.db')
  rmdb.select([0])

  out = rmdb.query()
  
  if len(sys.argv) == 2:
  	for key in out.keys():
  		for val in out[key]:
  			print '\t-{}'.format(val)
  else:
    	for key in out.keys():
			print key + ":"
			for val in out[key]:
				print '\t-{}'.format(val)
			print '\n'

  rmdb.close()
