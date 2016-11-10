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


def _load_data_file(pollutant, year):
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
    _load_data_file().
    """
    headers = ['longitude',
               'latitude',
               'month',
               'day',
               'mean',
               'max']

    # NOTE:  Use an excpetion here if errors arrise in parsing missing values.
    def to_dictionary(record):
        """ Return a dictionary all critical data fields in a CSV record. """
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

    data_file = _load_data_file(pollutant, year)
    csv_reader = csv.reader(data_file)
    next(csv_reader)

    record_list = list(csv_reader)
    record_list = [to_dictionary(record) for record in record_list]

    data_file.close()
    return record_list

# At this state in the program, you have a list of dictionaries for a given
# file. You now need to do the following:
#   (1) Aggregate duplicate records for the same day by averaging their
#       values (Order given by Dr. Zhou).  Use "itertools.groupby()" for
#       this procedure (make sure sort works for a given set).
#   (2) Remove the day value altogether.  Only the month matters now.
#   (3) Aggregate based on month and lat/longitude, taking average of averages
#       and max of max values.
#   (4) Append the results sorted by month (ascending) to a file that stores
#       all output for the specific pollution type.
#   (5) Make sure this process is repeated for all types and files.

def _agg_day_key_func(dict_rec):
    """
    Compute the sort key for given dictionary record.
    The key is of the form "(longitude, latitude, month, day)".
    """
    return (dict_rec['longitude'],
            dict_rec['latitude'],
            dict_rec['month'],
            dict_rec['day'])

def _agg_day_iter(dict_record_list):
    """ Return an interator over keys and groups in dict_record_list. """
    # Note that itertools.groupby() recommended sorting first.
    dict_record_list = sorted(dict_record_list, key=_agg_day_key_func)
    return itertools.groupby(dict_record_list, _agg_day_key_func)

def agg_day_duplicates():
    """
    Aggregate duplicate records for days in a given list of dictionary records
    by averaging their means and taking the max of their maximums.  This works
    for all pollutant types.
    """
    pass
