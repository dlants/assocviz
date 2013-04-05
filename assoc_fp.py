#!/usr/bin/python
# This file parses a plain text dataset, and creates a sqlite db
# Created by Denis Lantsman
# denis.lantsman@gmail.com
import sys
import os
import argparse
from pymining import itemmining, assocrules
import rmdb

class Assoc_Learner:
  def __init__(self, dbpath):
    self.rmdb = rmdb.RMDB(dbpath)

  def find_rules(self):
    print "querying db"
    print "selection : {} metaids".format(len(self.rmdb.selection))
    self.rmdb.selection = self.rmdb.selection[:5]
    usr_dict = self.rmdb.query()

    baskets = [[x.token() for x in usr_dict[y]] for y in usr_dict.keys()]

    print "{} baskets".format(len(baskets))

    print "learning rules"
    generator = find_frequent_itemsets(baskets, len(baskets) * 0.1, True)
    print "iterating through rules"
    self.rules =  sorted([x for x in generator], key = lambda x: x[1])
    return self.rules



if __name__ == "__main__":
  # test code
  asl = Assoc_Learner('rm.db')
  #asl.rmdb.select_metaid('MT-ELM-DivdDivrQuot-u1-1-RM')
  asl.rmdb.select_module('MT-ELM-DefWholNum-Th-RM')
  rules = asl.find_rules()
  print "sup conf rule"
  for r in rules:
    print "{}: {}".format(r[1], r[0])
