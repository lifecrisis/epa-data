"""
Clean and amalgamate EPA data for analysis.
"""


import csv
import itertools
import os.path

from datetime import date


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


def read_raw_data_file(pollutant, year):
    """
    Read records from a EPA data CSV file into a list of dictionaries.

    This method also performs necessary type conversions for eeach field.
    Also, raises IOError if bad specs are passed due to calling
    load_data_file().
    """
    headers = ['longitude',
               'latitude',
               'month',
	       'day',
               'mean',
               'max']

    # TODO: This data doesn't handle missing values, denoted by ' - '.  If you
    # start getting Errors, try to remove these values.
    def to_dictionary(record):
	# Convert to numerical month value first.
	date_obj = date(*[int(val) for val in record[11].split('-')])
	month = 12 * (date_obj.year - 1990) + date_obj.month
	# Generate dictionary of our record.
        record = dict(zip(headers,
                          [record[6],
                           record[5],
                           month,
			   date_obj.day,
                           float(record[16]),
                           float(record[17])]))
        return record

    data_file = load_data_file(pollutant, year)
    csv_reader = csv.reader(data_file)
    next(csv_reader)

    record_list = list(csv_reader)
    record_list = map(to_dictionary, record_list)

    data_file.close()
    return record_list

# TODO: Use itertools.groupby to perform the aggregation process. Sort first!
