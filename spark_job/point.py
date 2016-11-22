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

        # These don't conflict with the 'pm25' attribute for this project,
        # so we add them now.
        self.max = float(kwargs['maximum'])
        self.mean = float(kwargs['mean'])

        # Swap these two when running training tasks.
        # self.pm25 = float(kwargs['mean'])
        self.pm25 = float(kwargs['maximum'])

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


class QueryPoint(object):
    """ A single query point. """

    def __init__(self, csv_record):
        """ Initialize a new QueryPoint object from a string CSV record. """

        field_list = csv_record.strip().split(',')
        self.longitude = float(field_list[2])
        self.latitude = float(field_list[1])
        self.blk_id = field_list[0]
        # "month" is set in a flatMap() operation in Spark.
        self.month = None

    def distance(self, location):
        """ Return the Euclidean distance between this Point and location. """

        endpoint1 = self.location()
        endpoint2 = location
        return math.sqrt(sum([(a - b) ** 2
                              for a, b in zip(endpoint1, endpoint2)]))

    def interpolate(self, nodes, power):
        """ Set the estimated max and mean for this node. """

        # Note that objects in 'nodes' now have 'max' and 'mean' attribues.
        inv_distances = [(1.0 / self.distance(n.location)) ** power
                         for n in nodes]
        sum_inv_distances = sum(inv_distances)
        lambdas = [i / sum_inv_distances for i in inv_distances]

        # The above can be shared.
        self.max_est = sum([l * n.max for l, n in zip(lambdas, nodes)])
        self.mean_est = sum([l * n.mean for l, n in zip(lambdas, nodes)])

        # This hook can be used to average multiple estimates for this
        # query point and then manually set the values when done.
        return (self.max_est, self.mean_est)

    def location(self):
        """ Return a tuple representing the location of this PMPoint. """
        return (self.longitude, self.latitude, self.scaled_time)

    def report(self):
        """ Return a CSV record representing this QueryPoint's attributes. """

        return self.blk_id +\
               ',' +\
               str(self.longitude) +\
               ',' +\
               str(self.latitude) +\
               ',' +\
               str(self.month) +\
               ',' +\
               str(self.max_est) +\
               ',' +\
               str(self.mean_est) + '\n'

    def scale_time(self, scale):
        """
        Alter the scale applied to the time dimension of this Point.  You
        must do this before anything useful can be done with this Point.
        """
        self.scaled_time = self.month * scale
        self.time_scale = scale
        return self


def load_point_rdd(csv_rdd):
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
                      'maximum',
                      'mean']
        reader = csv.DictReader(result, fieldnames)
        return reader.next()

    return csv_rdd.map(load_record).map(lambda rec: Point(**rec))


def load_point_file(csv_file):
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
                      'maximum',
                      'mean']
        reader = csv.DictReader(result, fieldnames)
        return reader.next()

    records = [load_record(r) for r in csv_file_obj]
    result = [Point(**r) for r in records]
    csv_file_obj.close()

    return result
