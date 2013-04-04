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

class Event:
  def __init__(self, transaction, idx):
    '''Stores the info for an event.

    idx - the attempt of the student at the given problem.
    metaid - the metaid of the given problem
    uid - the uid of the student
    start - the sart time of the interaction
    duration - the duration of the interaction
    correct - was the interaction correct?
    hint - did the student use a hint?
    '''
    self.idx = idx
    self.uid, self.pid, self.metaid, self.problemuid = transaction[0:4]
    self.start = time.localtime(transaction[4])
    self.duration = transaction[5]
    self.hint = (transaction[6] == u'YES')
    self.correct = (transaction[7] == u'YES')

  def token(self):
    '''Return a tuple representing an event token, to be used in association
    mining.'''
    out = ''
    out += str(self.metaid)
    out += "|" + str(self.idx)
    if(self.correct):
      out += '|Y'
    else:
      out += '|N'
    if(self.hint):
      out += '|Y'
    else:
      out += '|N'
    
    return out

  def __str__(self):
    time_str = time.strftime("%H:%M:%S - %m/%d/%Y", self.start)
    return '[{} - {} s - attempt {} | uid: {} pid: {} | correct: {} hint: {}]'.format(
        time_str, self.duration, self.idx, self.uid, self.metaid, self.correct, self.hint)

class RMDB:
  def __init__(self, path):
    self.conn = sqlite3.connect(path)
    self.c = self.conn.cursor()
    self.selection = []

  def query(self):
    '''Query the db by my selection.
    
    Output is a dictionary, mapping a student uid to event objects.'''

    q_str = '''SELECT * FROM transactions WHERE metaid IN ({seq}) order by 
        uid, metaid, start'''.format(seq=','.join(['?']*len(self.selection)))

    self.c.execute(q_str, self.selection)
    out = {}

    uid = None
    metaid = None
    count = 0
    for transaction in self.c.fetchall():
      if not transaction[0] == uid:
        uid = transaction[0]
        out[uid] = []
        count = 0

      if not metaid == transaction[2]:
        metaid = transaction[2]
        count = 0

      if count < 2:
        out[uid].append(Event(transaction, count))
      count += 1

    return out

  def select_objective(self, obj_uid):
    self.c.execute('''select uid from modules where obj_uid = ?''', (obj_uid,))

    for module_uid in self.c.fetchall():
      self.select_module(str(module_uid[0]))

  def select_module(self, module_uid):
    self.c.execute('''SELECT metaid FROM problems where module_uid = ? order by idx desc''', 
        (module_uid,))

    metaids = self.c.fetchall()

    for metaid in metaids:
      self.selection.append(metaid[0])

  def select_metaid(self, metaid):
    self.selection.append(metaid)

  def close(self):
    self.conn.close()

if __name__ == "__main__":
  # test code
  rmdb = RMDB('rm.db')

  rmdb.select_objective('MT-ELM-DefWholNum-RM')
  out = rmdb.query()

  for key in out.keys():
    print key + ":"
    for val in out[key]:
      print '\t-{}'.format(val)
    print '\n'

  rmdb.close()
