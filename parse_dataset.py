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

parser = argparse.ArgumentParser(description = 'Parse an EDM dataset.')
parser.add_argument('path', type=str, help='transactions file')
parser.add_argument('moddir', type=str, help='module directory')
parser.add_argument('objpath', type=str, help='objective path')
parser.add_argument('currpath', type=str, help='curriculum path')
parser.add_argument('--dbpath', type=str, default='rm.db', 
    help='Target file (rm.db)')
args = parser.parse_args()

conn = sqlite3.connect(args.dbpath)
c = conn.cursor()
colsin = ['#userid', 'problemid', 'metaid', 'problemuid', 'first_time', 
  'submit_time', 'hint_used', 'answercorrect']

colsout = ['uid', 'pid', 'metaid', 'problemuid', 'start', 
  'duration', 'hint', 'correct']

colstr = ""
for col in colsout:
  colstr += col + ","
colstr = colstr[:-1]

# Create transaction table
print 'parsing transacitons'
c.execute("DROP TABLE IF EXISTS transactions;")
c.execute("CREATE TABLE transactions ({});".format(colstr))
timeformat = '%d.%m.%Y %H:%M:%S'
with open(args.path, 'rb') as f:
  dr = csv.DictReader(f)
  for r in dr:
    # skip if data missing
    if r['first_time'] == 'N' and r['submit_time'] == 'N':
      continue
    
    # convert times from strings to seconds
    r['first_time'] = time.mktime(time.strptime(r['first_time'], timeformat))
    r['submit_time'] = time.mktime(time.strptime(r['submit_time'], timeformat))
    r['submit_time'] = r['submit_time'] - r['first_time']

    # skip if duration too long (>15 minutes)
    if r['submit_time'] > 900 or r['submit_time'] < 0:
      continue

    c.execute("INSERT INTO transactions ({}) VALUES (?, ?, ?, ?, ?, ?, ?, ?)".format(colstr), 
        [r[col] for col in colsin])

conn.commit()

# Create curriculum table
print 'creating curriculum table'
curriculum_dom = minidom.parse(args.currpath)
curriculum = [x.childNodes[0].data for x in curriculum_dom.getElementsByTagName('uid')]

objectives_dom = minidom.parse(args.objpath)

colsobj = ['uid', 'curr_idx', 'name', 'short_name']

colstr = ""
for col in colsobj:
  colstr += col + ","
colstr = colstr[:-1]

c.execute("DROP TABLE IF EXISTS objectives;")
c.execute("CREATE TABLE objectives ({});".format(colstr))

for objective_idx, objective_uid in enumerate(curriculum):
  for objective_dom in objectives_dom.getElementsByTagName('objective'):
    if objective_dom.getAttribute('uid') == objective_uid:
      name = objective_dom.getElementsByTagName('name')[0].childNodes[0].data
      short_name = objective_dom.getElementsByTagName('shortName')[0].childNodes[0].data

      c.execute("INSERT INTO objectives ({}) VALUES (?, ?, ?, ?)".format(colstr), 
          (objective_uid, objective_idx, name, short_name))

conn.commit()
print 'creating module and problem tables'

colsmodule = ['uid', 'obj_uid', 'name']
mod_colstr = ""
for col in colsmodule:
  mod_colstr += col + ","
mod_colstr = mod_colstr[:-1]

c.execute("DROP TABLE IF EXISTS modules;")
c.execute("CREATE TABLE modules ({});".format(mod_colstr))

colsprob = ['metaid', 'module_uid', 'idx']
prob_colstr = ""
for col in colsprob:
  prob_colstr += col + ","
prob_colstr = prob_colstr[:-1]

c.execute("DROP TABLE IF EXISTS problems;")
c.execute("CREATE TABLE problems ({});".format(prob_colstr))

for root, dirs, files in os.walk(args.moddir):
  for filename in files:
    print 'traversing file {}'.format(filename)
    module_dom = minidom.parse(os.path.join(root, filename))
    name = module_dom.getElementsByTagName('name')[0].childNodes[0].data
    uid  = module_dom.getElementsByTagName('UID')[0].childNodes[0].data
    obj_uid = module_dom.getElementsByTagName('targetObjective')

    if len(obj_uid) == 0:
      continue

    obj_uid = obj_uid[0].childNodes[0].data

    c.execute("INSERT INTO modules ({}) VALUES (?, ?, ?)".format(mod_colstr), 
        (uid, obj_uid, name))

    # regular expression for finding metaids
    count = 1
    for entry in module_dom.getElementsByTagName('entries'):
      if entry.getElementsByTagName('type')[0].childNodes[0].data == '1':
        content = entry.getElementsByTagName('content')[0].childNodes[0].data
        
        matches = re.findall(r'metaID=[\w|-]+', content)
        for match in matches:
          metaid = match.split('=')[1]

          c.execute("INSERT INTO problems ({}) VALUES (?, ?, ?)".format(prob_colstr), 
              (metaid, uid, count))

          count+=1

print 'creating indexes into transactions table'
c.execute("CREATE INDEX metaid_index ON transactions (metaid)")
c.execute("CREATE INDEX uid_index ON transactions (uid)")
print "done"
conn.commit()
conn.close()
