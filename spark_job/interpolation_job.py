"""
The interpolation job for the EPA data set.
"""


import kdtree
import point


CONF = SparkConf()
SC = SparkContext(conf=CONF)

CENTROIDS_DATA_FILE = 'test_centroids.csv'

# TODO(jrf): Make parameters that won't change constants.


def _ozone_interpolation(centroid_rdd):
    """ Run the interpolation of ozone at the centroid locations. """

    # Set parameters unique for this interpolation task.
    time_scale = 0.15

    ozone_data_file = '../data/clean/monthly_ozone_1990-2015.csv'
    ozone_point_list = point.load_point_file(ozone_data_file)
    ozone_point_list = [p.scale_time(time_scale) for p in ozone_point_list]

    # Steps:
    # ------
    #   (0) Fully pre-process the data for estimation (i.e. fix the
    #       timescales).
    #   (1) Turn the centroid_rdd into an RDD of query points with the
    #       proper time scale, and cache the intermediate result.
    #   (2) Make three bags of the ozone data (scale time dimension first!).
    #   (3) Turn the bags into trees... "kdtree.KDTree(point_list)"
    #   (4) Broadcast the trees.
    #   (5) Write a function that uses the broadcasted trees to interpolate
    #       a SINGLE query point.
    #   (6) Map the RDD of query points accordingly.
    #   (7) Write the report() output of the interpolated query points to
    #       a text file.


def _pm25_interpolation(centroid_rdd):
    """ Run the interpolation of ozone at the centroid locations. """

    # Set parameters unique for this interpolation task.
    time_scale = 0.15

    pm25_data_file = '../data/clean/monthly_pm25_1990-2015.csv'
    pm25_point_list = point.load_point_file(pm25_data_file)
    pm25_point_list = [p.scale_time(time_scale) for p in pm25_point_list]


def main():
    """ Application main. """

    # Expand the centroid data to include the month axis.
    centroid_rdd = SC.textFile(CENTROIDS_DATA_FILE).\
                      flatMap(lambda r: [(r, m) for m in range(1, 313)]).\
                      cache()

    # Run the ozone interpolation task.
    _ozone_interpolation(centroid_rdd)

    # Filter out the points that are no longer useful.
    centroid_rdd = centroid_rdd.filter(lambda pair: pair[1] >= 88)

    # Run the pm2.5 interpolation task.
    _pm25_interpolation(centroid_rdd)


if __name__ == "__main__":
    main()
