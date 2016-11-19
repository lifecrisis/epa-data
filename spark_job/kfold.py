"""
Run MARE and RMSPE based learning functions.

Kind of a mess, but uses bagging accurately, which is fine for our purposes.
"""

import copy
import math
import random

import kdtree


class KFoldConf:

    def __init__(self, folds, neighbors, power, radius, time_scale, alpha, bags):
        self.folds = folds
        self.neighbors = neighbors
        self.power = power
        self.radius = radius
        self.time_scale = time_scale
        # alpha value from bagging
        self.alpha = alpha
        # m value from bagging
        self.m = bags

    def __repr__(self):
        return ('KFoldConf(folds=' +
                repr(self.folds) +
                ', neighbors=' +
                repr(self.neighbors) +
                ', power=' +
                repr(self.power) +
                ', radius=' +
                repr(self.radius) +
                ', time_scale=' +
                repr(self.time_scale) +
                ')')


def sample_with_replacement(population, k):
    """
    Return a list of size k sampled randomly (with replacement) from
    iterable.
    """
    n = len(population)
    _random, _int = random.random, int  # speed hack
    result = [None] * k
    for i in xrange(k):
        j = _int(_random() * n)
        result[i] = population[j]
    return result


def mare(conf, point_list_brd):
    """
    Return the MARE error statistic generated from K-fold cross validation.

    Take the given KFoldConf object and an ordered broadcasted list of point
    objects and return the desired error statistic.
    """

    # deep copy point_list
    points = copy.deepcopy(point_list_brd.value)

    # scale time dimensions
    for p in points:
        p.scale_time(conf.time_scale)

    # build a list of sets representing the relevant partition
    partition = [list() for i in range(conf.folds)]
    for i, p in enumerate(points):
        partition[i % conf.folds].append(p)

    # generate results for kfold cross validation with this err stat
    results = [0.0] * conf.folds
    for i in range(conf.folds):

        # initialize validation set and training set
        validation_set = partition[i]
        training_set = list()
        for j in range(conf.folds):
            if j != i:
                training_set.extend(partition[j])

        # generate conf.m bags and trees at conf.alpha by sampling with
        # replacement
        n_prime = int(len(training_set) * conf.alpha)
        bags = [sample_with_replacement(training_set, n_prime)
                for i in range(conf.m)]
        trees = [kdtree.KDTree(bag) for bag in bags]

        for p in validation_set:
            # compute the average estimate for pollution at p over bags
            avg_estimate = 0.0
            for tree in trees:
                nnl = tree.query(p, conf.neighbors)
                avg_estimate += p.interpolate(nnl, conf.power)
            avg_estimate /= conf.m
            # incorporate this information into the results vector
            results[i] += (abs(avg_estimate - p.value()) / p.value())
        results[i] /= len(validation_set)

    # return the average of the elements in the results vector
    return sum(results) / len(results)


def rmspe(conf, point_list_brd):
    """
    Return the RMSPE error statistic generated from K-fold cross validation.

    Take the given KFoldConf object and an ordered broadcasted list of point
    objects and return the desired error statistic.
    """

    # deep copy point_list
    points = copy.deepcopy(point_list_brd.value)

    # scale time dimensions
    for p in points:
        p.scale_time(conf.time_scale)

    # build a list of sets representing the relevant partition
    partition = [list() for i in range(conf.folds)]
    for i, p in enumerate(points):
        partition[i % conf.folds].append(p)

    # generate results for kfold cross validation with this err stat
    results = [0.0] * conf.folds
    for i in range(conf.folds):

        # initialize validation_set and training_set
        validation_set = partition[i]
        training_set = list()
        for j in range(conf.folds):
            if j != i:
                training_set.extend(partition[j])

        # generate conf.m bags at conf.alpha by sampling with replacement
        n_prime = int(len(training_set) * conf.alpha)
        bags = [sample_with_replacement(training_set, n_prime)
                for i in range(conf.m)]
        trees = [kdtree.KDTree(bag) for bag in bags]

        for point in validation_set:
            # compute the average estimate for pollution at point over bags
            avg_estimate = 0.0
            for tree in trees:
                nnl = tree.query(point, conf.neighbors)
                avg_estimate += point.interpolate(nnl, conf.power)
            avg_estimate /= conf.m
            # incorporate this information into the results vector
            results[i] += ((avg_estimate - point.value()) /
                           point.value()) ** 2.0
        results[i] /= len(validation_set)
        results[i] = math.sqrt(results[i]) * 100

    # return the average of the elements in the results vector
    return sum(results) / len(results)
