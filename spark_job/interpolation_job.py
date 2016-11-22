"""
The interpolation job for the EPA data set.
"""

import pyspark

import kdtree
import kfold
import point


CONF = pyspark.SparkConf()
SC = pyspark.SparkContext(conf=CONF)

CENTROIDS_DATA_FILE = 'test_centroids.csv'

# Fixed parameters.
NEIGHBORS = 3
POWER = 4.5
ALPHA = 0.75
NUM_BAGS = 3


def _data_interpolation(centroid_rdd, pollutant):
    """ Run the interpolation of ozone at the centroid locations. """

    # Set parameters unique for this interpolation task.
    if pollutant == 'ozone':
        time_scale = (0.4 + 2.0) / 2.0
        data_file = '../data/clean/monthly_ozone_1990-2015.csv'
        point_list = point.load_point_file(data_file)
        point_list = [p.scale_time(time_scale) for p in point_list]
    else:
        time_scale = (0.18 + 0.16) / 2.0
        data_file = '../data/clean/monthly_pm25_1990-2015.csv'
        point_list = point.load_point_file(data_file)
        point_list = [p.scale_time(time_scale) for p in point_list]

    # Bag the point list and produce a list of trees to use for prediction.
    bag_size = int(len(point_list) * ALPHA)
    bags = [kfold.sample_with_replacement(point_list, bag_size)
            for _ in range(NUM_BAGS)]
    trees = [kdtree.KDTree(bag) for bag in bags]
    tree_tuple_brd = SC.broadcast(trees)

    # Define a mapper for interpolating each query point.
    def interpolation_mapper(query_point, tree_tuple_brd):
        """
        Set the max and mean estimates for query_point using the list
        of KDTree objects for interpolation.
        """

        # Generate a list of estimates for this query point.
        estimates = []
        for tree in tree_tuple_brd.value:
            nodes = tree.query(query_point, NEIGHBORS)
            estimates.append(query_point.interpolate(nodes, POWER))

        # Average the estimates from each bag.
        max_est = sum([est[0] for est in estimates]) / len(estimates)
        mean_est = sum([est[1] for est in estimates]) / len(estimates)

        # Fix the results within query_point.
        query_point.max_est = max_est
        query_point.mean_est = mean_est

        return query_point

    # Transform centroid_rdd into an RDD of query points, scale the time
    # dimension, and cache the intermediate result.
    def query_point_factory(record, month):
        """ Build a QueryPoint from a CSV record and a month value. """
        result = point.QueryPoint(record)
        result.month = month
        return result
    query_point_rdd = centroid_rdd.map(lambda p: query_point_factory(*p))
    query_point_rdd = query_point_rdd.map(lambda q: q.scale_time(time_scale))
    query_point_rdd = query_point_rdd.cache()

    # Map the query_point_rdd through the interpolation mapper.
    query_point_rdd = query_point_rdd.map(lambda q: interpolation_mapper(q, tree_tuple_brd)).cache()

    # ----------------------  Aggregation  ----------------------------------

    # TESTING
    # -------

    def simple_report(query_point):
        """ No comment. """
        month = (query_point.month % 12) + 1
        year = (query_point.month / 12) + 1990

        return query_point.blk_id +\
               ',' +\
               str(month) +\
               ',' +\
               str(year) +\
               ',' +\
               str(query_point.max_est) +\
               ',' +\
               str(query_point.mean_est)

    # Write the output to a file.
    if pollutant == 'ozone':
        query_point_rdd.map(simple_report).saveAsTextFile('ozone_inter_output')
    else:
        query_point_rdd.map(simple_report).saveAsTextFile('pm25_inter_output')

    # ----------------------------------------------------------------------

#     # Key the query point data.
#     def query_point_year(query_point):
#         """ Return the year in which a query point occurred. """
#         return (query_point.month / 12) + 1990
#     pair_rdd = query_point_rdd.map(lambda q: ((q.blk_id, query_point_year(q)), q))
#
#     # Goup the data by key.
#     pair_rdd = pair_rdd.groupByKey()
#
#     def key_value_mapper(key, value):
#         """ Process pair_rdd defined above. """
#         # key -- 2-tuple of the form (blk_id, year)
#         # value -- list of query points under the given key
#
#         for elt in value:
#             latitude = elt.latitude
#             longitude = elt.longitude
#             break
#
#         max_of_max = max([q.max_est for q in value])
#         max_of_mean = max([q.mean_est for q in value])
#         mean_of_max = sum([q.max_est for q in value]) / len(value)
#
#         return key[0] +\
#                ',' +\
#                str(longitude) +\
#                ',' +\
#                str(latitude) +\
#                ',' +\
#                str(key[1]) +\
#                ',' +\
#                str(max_of_max) +\
#                ',' + \
#                str(max_of_mean) +\
#                ',' +\
#                str(mean_of_max)
#
#     # Write the output to a file.
#     if pollutant == 'ozone':
#         pair_rdd.map(lambda p: key_value_mapper(*p)).saveAsTextFile('ozone_interpolation_results.csv')
#     else:
#         pair_rdd.map(lambda p: key_value_mapper(*p)).saveAsTextFile('pm25_interpolation_results.csv')


def main():
    """ Application main. """

    # Expand the centroid data to include the month axis.
    centroid_rdd = SC.textFile(CENTROIDS_DATA_FILE).\
                      flatMap(lambda r: [(r, m) for m in range(1, 313)]).\
                      cache()

    # Interpolate the ozone values.
    _data_interpolation(centroid_rdd, 'ozone')

    # Filter out the unnecessary values.
    centroid_rdd = centroid_rdd.filter(lambda pair: pair[1] >= 88)

    # Interpolate the pm25 values.
    _data_interpolation(centroid_rdd, 'pm25')


if __name__ == "__main__":
    main()
