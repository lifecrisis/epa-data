"""
Clean and amalgamate EPA data for analysis.
"""


import csv
import itertools
import os.path


# Project directory root.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Data directory roots.
DATA_ROOT = os.path.join(PROJECT_ROOT, 'data/')
NO2_DATA_ROOT = os.path.join(DATA_ROOT, 'no2_raw_data/')
OZONE_DATA_ROOT = os.path.join(DATA_ROOT, 'ozone_raw_data/')
PM25_DATA_ROOT = os.path.join(DATA_ROOT, 'pm25_raw_data/')


def load_data_file(pollutant, year):
    """
    Return a data file handle for the given pollutant ('ozone' or 'pm25')
    and year.
    """

    # Select the correct file or raise an IOError.
    if pollutant == 'ozone' and year in range(1990, 2016):
        path_to_file = os.path.join(OZONE_DATA_ROOT,
                                    "daily_44201_" + str(year) +".csv")
        result = open(path_to_file, 'r')
    elif pollutant == 'pm25' and year in range(1990, 2016):
        path_to_file = os.path.join(PM25_DATA_ROOT,
                                    "daily_88101_" + str(year) +".csv")
        result = open(path_to_file, 'r')
    elif pollutant == 'no2' and year in range(1990, 2016):
        path_to_file = os.path.join(NO2_DATA_ROOT,
                                    "daily_42602_" + str(year) +".csv")
        result = open(path_to_file, 'r')
    else:
        msg = "Data file with type '" + pollutant + "' and year '" +\
              str(year) + "' not found."
        raise IOError(msg)

    # Return a handle to the selected file.
    return result


def read_data_file(pollutant, year):
    """
    Read records from a EPA data CSV file into a list of dictionaries.

    This method also performs necessary type conversions for eeach field.
    Also, raises IOError if bad specs are passed due to calling
    load_data_file().
    """
    headers = ['longitude',
               'latitude',
               'date',
               'mean',
               'max']

    def to_dictionary(record):
        record = dict(zip(headers,
                          [record[6],
                           record[5],
                           record[11],
                           record[16],
                           record[17]]))
        return record

    data_file = load_data_file(pollutant, year)
    csv_reader = csv.reader(data_file)
    next(csv_reader)

    record_list = list(csv_reader)
    print record_list[10]
    record_list = map(to_dictionary, record_list)

    data_file.close()
    return record_list

# TODO: Convert date to datetime object and distance from Jan. 1990 in months.
# TODO: Convert mean and max to floats for amalgamation process.
# TODO: Use itertools.groupby to perform the aggregation process.
# TODO: Consider placing these actions in another method.
