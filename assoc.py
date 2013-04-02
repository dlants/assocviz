#!/usr/bin/python
# This file parses a plain text dataset, and creates a sqlite db
# Created by Denis Lantsman
# denis.lantsman@gmail.com
import sys
import os
import argparse
import Orange
from Orange import orange
from Orange import data
from Orange import feature
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
    tokens = reduce(set.union, [set(x) for x in baskets])

    print "constructiong domain"
    domain = data.Domain([])
    for token in tokens:
      domain.add_meta(feature.Descriptor.new_meta_id(), \
         feature.Continuous(token), 1) 

    table = data.Table(domain)

    print "populating table"
    for basket in baskets:
      instance = data.Instance(domain)
      for item in basket:
        instance[item] = 1.0
      table.append(instance)

    print "db size:{} rows, {} keys".format(len(baskets), len(tokens))

    print "learning rules"
    self.rules = Orange.associate.AssociationRulesSparseInducer(table, 
        support = 0.3, max_item_sets = 100000)
    Orange.associate.sort(self.rules, ms=['confidence'])
    return self.rules


if __name__ == "__main__":
  # test code
  asl = Assoc_Learner('rm.db')
  #asl.rmdb.select_metaid('MT-ELM-DivdDivrQuot-u1-1-RM')
  asl.rmdb.select_module('MT-ELM-DefWholNum-Th-RM')
  rules = asl.find_rules()
  print "sup conf rule"
  for r in rules:
    print "{:.2f} {:.2f} {}".format(r.support, r.confidence, r)
