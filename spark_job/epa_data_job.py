"""
Here, we train our model on the EPA data sets in the local "partitions/"
directory.  The settings for our learning task are below, and they will be
run a total of six times (once for each of three pollutant types and once
for each of the mean and max metrics that must be learned).

Settings:
---------
  Folds:        10
  Neighbors:    3
  Power:        4.5
  Time Scale:   (only trained parameter here, see below)
  Alpha:        0.75
  Bags:         3

Expect a "results_[pollutant]_[metric].csv" as output in the local "results/"
directory.
"""


from pyspark import SparkConf, SparkContext

import kfold
import point


CONF = SparkConf()
SC = SparkContext(conf=CONF)


def report(result_record):
    """
    Return a report string (in CSV format) given a record from a result RDD.

    This report method is unique to this experiment. This makes sense because
    output differs across all experiments. Here, the "result_record" takes
    the form:

        (KFoldConf, MARE, RMSPE)

    The first element is an int. The last two are floating point numbers
    storing the MARE and RMSPE results respectively.
    """
    return (str(result_record[0].folds) +               # folds
            ',' + str(result_record[0].neighbors) +     # neighbors
            ',' + str(result_record[0].power) +         # power
            ',' + str(result_record[0].time_scale) +    # time_scale
            ',' + str(result_record[0].alpha) +         # alpha
            ',' + str(result_record[0].m) +             # m
            ',' + str(result_record[1]) +               # MARE
            ',' + str(result_record[2]))                # RMSPE


def main():
    """ Application main. """

    K = [10]   # folds
    N = [3]    # neighbors
    P = [4.5]  # powers
    # The time scale is the only parameter being trained here, so we
    # consider a number of options.
    C = [0.001 * i for i in range(1, 25)]
    C.extend([0.025 * i for i in range(1, 81)])
    A = [0.75] # alphas
    M = [3]    # bags

    # Build the list of k-fold configurations under analysis.
    conf_list = [kfold.KFoldConf(k, n, p, None, c, a, m)
                 for k in K
                 for n in N
                 for p in P
                 for c in C
                 for a in A
                 for m in M]

    # Distribute the RDD of k-fold configurations.
    conf_rdd = SC.parallelize(conf_list, 104).cache()

    # Group all the partitions that are to be examined.
    partition_files = ['partitions/monthly_ozone_1990-2015_partition.csv',
                       'partitions/monthly_pm25_1990-2015_partition.csv']

    # Run learning tasks for each partition.
    for file_name in partition_files:

        # Note that we reuse the method from "point.py" here.
        point_list = point.load_pm25_file(file_name)
        point_list_brd = SC.broadcast(point_list)

        # Define a mapper to run your statistical routines.
        def fold(conf):
            """ Return a result tuple for the given configuration. """
            return (conf,                               # KFoldConf object
                    kfold.mare(conf, point_list_brd),   # MARE statistic
                    kfold.rmspe(conf, point_list_brd))  # RMSPE statistic

        # Run the learning routines and generate the report.
        report_rdd = conf_rdd.map(fold).map(report)

        # Write the output to a file in a "results/" directory, regardless of
        # the order in which the partitions were analysed.
        if 'no2' in file_name:
            report_rdd.saveAsTextFile('results/no2_max_results')
        elif 'ozone' in file_name:
            report_rdd.saveAsTextFile('results/ozone_max_results')
        elif 'pm25' in file_name:
            report_rdd.saveAsTextFile('results/pm25_max_results')
        else:
            import sys
            sys.exit(1)

if __name__ == "__main__":
    main()
