#!/usr/bin/python
# This file parses a plain text dataset, and creates a sqlite db
# Created by Denis Lantsman
# denis.lantsman@gmail.com
import sys
import os
import argparse
from pymining import itemmining, assocrules, seqmining
import rmdb
import matplotlib.pyplot as plt

class Assoc_Learner:
  def __init__(self, min_support = 0.3, min_confidence = 0.5, 
      min_lift = 1.2):
    self.min_support = min_support
    self.min_confidence = min_confidence
    self.min_lift = min_lift

  def mine_seqs(self, baskets):
    print "mining frequent sequences"
    freq_seqs = seqmining.freq_seq_enum(baskets, len(baskets) * self.min_support)
    print "found {} frequent sequences".format(len(freq_seqs))

    total = len(baskets)
    seq_supports = {frozenset(x[0]) : float(x[1])/total for x in freq_seqs}
    out_seqs = []
    # test the lift of each rule
    for seq in freq_seqs:
      seq_key = frozenset(seq[0])
      if len(seq_key) < 2:
        continue
      sup_total = seq_supports[seq_key]
      sup_split_max = 0
      for token in seq_key:
        token_set = frozenset((token,))
        sup_token = seq_supports[token_set]
        sup_rest = seq_supports[seq_key-token_set]

        if sup_token*sup_rest > sup_split_max:
          sup_split_max = sup_token*sup_rest

      if (sup_total/sup_split_max) > self.min_lift:
        out_seqs.append((seq[0], sup_total, sup_total/sup_split_max))
    
    freq_seqs = out_seqs
    print "found {} sequences with sufficient lift".format(len(freq_seqs))
    freq_seqs = self.nonmax_suppression_seqs(freq_seqs)
    print "found {} maximal sequences".format(len(freq_seqs))
    return freq_seqs

  def mine_rules_relim(self, baskets):
    print "preparing itemset"
    relim_input = itemmining.get_relim_input(baskets)
    
    print "finding frequent itemsets"
    self.item_sets = itemmining.relim(relim_input, min_support = len(baskets) * self.min_support)
    
    print "finding association rules"
    self.rules = assocrules.mine_assoc_rules(self.item_sets, len(baskets), 
        min_support = self.min_support, min_confidence = self.min_confidence, 
        min_lift = self.min_lift)
    
    # sort by support
    self.nonmax_suppression()
    self.rules = sorted(self.rules, key = lambda x: -x[2])

  def mine_rules_fp(self, baskets):
    print "preparing fptree"
    fptree = itemmining.get_fptree(baskets, min_support = len(baskets) * self.min_support)
    
    print "finding itemsets"
    self.item_sets = itemmining.fpgrowth(fptree, min_support = len(baskets) * self.min_support)

    print "found {} frequent sequences".format(len(self.item_sets))
    
    print "finding association rules"
    self.rules = assocrules.mine_assoc_rules(self.item_sets, len(baskets), 
        min_support = self.min_support, min_confidence = self.min_confidence, 
        min_lift = self.min_lift)
    
    # sort by support
    self.nonmax_suppression()
    self.max_rules = sorted(self.max_rules, key = lambda x: -x[2])

    print "found {} maximal rules with sufficient lift".format(len(self.max_rules))
    
  def nonmax_suppression_seqs(self, seqs):
    ''' remove all rules from self.rules that are subsets of other rules. '''

    max_seqs = []
    for idx1, seq1 in enumerate(seqs):
      subsumed = False
      for idx2, seq2 in enumerate(seqs):
        if (not idx1 == idx2) and frozenset(seq1[0]) < frozenset(seq2[0]):
          subsumed = True
          break

      if not subsumed and not seq1 in max_seqs:
        max_seqs.append(seq1)
    return max_seqs

  def nonmax_suppression(self):
    ''' remove all rules from self.rules that are subsets of other rules. '''

    self.max_rules = []
    for idx, rule in enumerate(self.rules):
      subsumed = False
      for idx2, rule2 in enumerate(self.rules):
        if rule[0] < rule2[0] and rule[1] < rule2[1]:
          subsumed = True
          break

      if not subsumed and not rule in self.max_rules:
        self.max_rules.append(rule)

if __name__ == "__main__":
  # test code
  db = rmdb.RMDB('data/rm.db')
  db.select([1])
  asl = Assoc_Learner(min_support = 0.2, min_confidence = 0.7, min_lift = 1.2)
  seqs = asl.mine_seqs(db.query())
  for seq in seqs:
    db.print_seq(seq[0])
    print "support: {}".format(seq[1])
    print "lift: {}".format(seq[2])
    print "\n\n"
