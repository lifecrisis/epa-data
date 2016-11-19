"""
This module provides facilities for training our model on the EPA data set.
"""


import csv
import math
import StringIO


class Point(object):
    """ A single EPA data point. """

    def __init__(self, **kwargs):
        """ Initialize a new Point object from a decoded CSV record. """

        self.longitude = float(kwargs['longitude'])
        self.latitude = float(kwargs['latitude'])
        self.month = float(kwargs['month'])

        # Swap these two when training for maximum.
        # self.pm25 = float(kwargs['maximum'])
        self.pm25 = float(kwargs['mean'])

        self.scaled_time = None
        self.time_scale = None

    def distance(self, location):
        """ Return the Euclidean distance between this Point and location. """

        endpoint1 = self.location()
        endpoint2 = location
        return math.sqrt(sum([(a - b) ** 2
                              for a, b in zip(endpoint1, endpoint2)]))

    def interpolate(self, nodes, power):
        """ Return an estimate of self.pm25 given nodes list. """

        inv_distances = [(1.0 / self.distance(n.location)) ** power
                         for n in nodes]
        sum_inv_distances = sum(inv_distances)
        lambdas = [i / sum_inv_distances for i in inv_distances]
        result = sum([l * n.value for l, n in zip(lambdas, nodes)])
        return result

    def location(self):
        """ Return a tuple representing the location of this PMPoint. """
        return (self.longitude, self.latitude, self.scaled_time)

    def scale_time(self, scale):
        """
        Alter the scale applied to the time dimension of this Point.  You
        must do this before anything useful can be done with this Point.
        """
        self.scaled_time = self.month * scale
        self.time_scale = scale
        return self

    def value(self):
        """ Return the pollution measurement recorded at this location. """
        return self.pm25

    def __str__(self):
        """ Return the string representation of this object. """
        return '< PMPoint -- ' +\
               str(self.location()) +\
               ', ' +\
               str(self.value()) +\
               '>'


def load_pm25_rdd(csv_rdd):
    """
    Return an RDD of Point objects.

    The rdd argument must be an RDD of CSV records representative of Point
    objects.
    """

    def load_record(record):
        """ Parse a single CSV record. """
        result = StringIO.StringIO(record)
        fieldnames = ['longitude',
                      'latitude',
                      'month',
                      'max',
                      'mean']
        reader = csv.DictReader(result, fieldnames)
        return reader.next()

    return csv_rdd.map(load_record).map(lambda rec: Point(**rec))


def load_pm25_file(csv_file):
    """
    Return a list of Point objects loaded directly from a CSV file.

    Rather than loading Points from an RDD of CSV records, load them from
    a file directly.
    """
    csv_file_obj = open(csv_file, 'r')

    def load_record(record):
        """ Parse a single CSV record. """
        result = StringIO.StringIO(record)
        fieldnames = ['longitude',
                      'latitude',
                      'month',
                      'max',
                      'mean']
        reader = csv.DictReader(result, fieldnames)
        return reader.next()

    records = [load_record(r) for r in csv_file_obj]
    result = [Point(**r) for r in records]
    csv_file_obj.close()

    return result
