#!/usr/bin/python
# This file parses a plain text dataset, and creates a sqlite db
# Created by Denis Lantsman
# denis.lantsman@gmail.com
import sys
import os
import argparse
from pymining import itemmining, assocrules, seqmining
import rmdb

class Assoc_Learner:
  def __init__(self, dbpath, min_support = 0.3, min_confidence = 0.5, 
      min_gain = 1.2):
    self.rmdb = rmdb.RMDB(dbpath)
    self.min_support = min_support
    self.min_confidence = min_confidence
    self.min_gain = min_gain

  def mine_seqs(self, baskets):
    freq_seqs = seqmining.freq_seq_enum(baskets, len(baskets) * self.min_support)

    # test the support and gain of each rule
    out_seqs = []
    total = len(baskets)
    freq_seqs = {x[0] : float(x[1])/total for x in freq_seqs}

    for seq in freq_seqs.keys():
      if len(seq) < 2:
        continue

      support = freq_seqs[seq]
      a = seq[:-1]
      b = (seq[-1],)
      support_a = freq_seqs[a]
      support_b = freq_seqs[b]

      confidence = support / support_a
      gain = support / (support_a * support_b)

      if confidence > self.min_confidence and gain > self.min_gain:
        out_seqs.append((a, b, support, confidence, gain))

    self.rules = sorted(out_seqs, key = lambda x: -x[4])

    print "found {} frequent sequences".format(len(freq_seqs))
    print "found {} rules with sufficient gain".format(len(self.rules))

    self.nonmax_suppression()
    print "found {} maximal rules".format(len(self.max_rules))

  def mine_rules(self, baskets):
    print "preparing itemset"
    relim_input = itemmining.get_relim_input(baskets)
    
    print "finding frequent itemsets"
    self.item_sets = itemmining.relim(relim_input, min_support = len(baskets) * self.min_support)
    
    print "finding association rules"
    self.rules = assocrules.mine_assoc_rules(self.item_sets, len(baskets), 
        min_support = self.min_support, min_confidence = self.min_confidence, 
        min_lift = self.min_lift)
    
    # sort by support
    self.rules = sorted(self.rules, key = lambda x: -x[2])
    
  def nonmax_suppression(self):
    ''' remove all rules from self.rules that are subsets of other rules. '''

    self.max_rules = []
    for idx, rule in enumerate(self.rules):
      subsumed = False
      for idx2, rule2 in enumerate(self.rules):
        if rule[0] < rule2[0] and rule[1] == rule2[1]:
          subsumed = True
          break

      if not subsumed and not rule in self.max_rules:
        self.max_rules.append(rule)

  def find_rules(self):
    print "querying db"
    self.rmdb.query()

    baskets = [[x.token() for x in self.rmdb.query[y]] for y in self.rmdb.query.keys()]
    print "{} baskets".format(len(baskets))

    tokens = reduce(set.union, [set(x) for x in baskets])
    print "{} unique tokens".format(len(tokens))

    print "{} longest sequence".format(max([len(x) for x in baskets]))

    self.token_to_idx = {x[1]:x[0] for x in enumerate(tokens)}
    self.idx_to_token = {x[0]:x[1] for x in enumerate(tokens)}

    # shorten strings to integers to make comparisons faster.
    baskets_short = [[self.token_to_idx[token] for token in transaction] for transaction in baskets]
 
    print "mining sequences"
    self.mine_seqs(baskets_short)

  def print_rules(self):
    for rule in self.max_rules:
      s = '['
      for item in rule[0]: # incedent
        s += self.idx_to_token[item] + ', '
      s += '] => ['
      for item in rule[1]: # consequent
        s += self.idx_to_token[item] + ', '
      s += '] : sup({:0.2f}), conf({:0.2f}), lift({:0.2f})'.format(rule[2], rule[3], rule[4])
      print s

  def print_rules_compact(self):
    for rule in self.max_rules:
      s = '['
      for item in rule[0]: # incedent
        s += str(item) + ', '
      s += '] => ['
      for item in rule[1]: # consequent
        s += str(item) + ', '
      s += '] : sup({:0.2f}), conf({:0.2f}), lift({:0.2f})'.format(rule[2], rule[3], rule[4])
      print s


if __name__ == "__main__":
  # test code
  asl = Assoc_Learner('data/rm.db', min_support = 0.2, min_confidence = 0.7, min_gain = 1.4)
  asl.rmdb.select([101])
  asl.find_rules()
  asl.print_rules()
  import pdb; pdb.set_trace()
