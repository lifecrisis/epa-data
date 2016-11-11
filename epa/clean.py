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
        # NOTE: We start counting at 1!!!
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

def _agg_day_key_func(dict_record):
    """
    Compute the sort key for given dictionary record.
    The key is of the form "(longitude, latitude, month, day)".
    """
    return (dict_record['longitude'],
            dict_record['latitude'],
            dict_record['month'],
            dict_record['day'])

def _agg_day_iter(dict_record_list):
    """ Return an interator over keys and groups in dict_record_list. """
    # Note that itertools.groupby() recommended sorting first.
    dict_record_list = sorted(dict_record_list, key=_agg_day_key_func)
    return itertools.groupby(dict_record_list, _agg_day_key_func)

def agg_day_duplicates(dict_record_list):
    """
    Aggregate duplicate records for days in a given list of dictionary records
    by averaging their means and taking the max of their maximums.  This works
    for all pollutant types.
    """

    # Get an iterator over (key, group) pairs and extract the list of groups.
    iterator = _agg_day_iter(dict_record_list)
    groups = [list(group) for _, group in iterator]

    def reducer(rec1, rec2):
        """ Amalgamate two records from the same group. """
        maximum = rec1['max'] if rec1['max'] > rec2['max'] else rec2['max']
        return {'longitude': rec1['longitude'],
                'latitude': rec1['latitude'],
                'month': rec1['month'],
                'mean': rec1['mean'] + rec2['mean'],
                'max': maximum}

    result = []
    for group in groups:
        for elt in group:
            del elt['day']
        reduction = reduce(reducer, group)
        reduction['mean'] /= len(group)
        result.append(reduction)
    return result

def agg_by_month(clean_dict_records):
    """
    Aggregate records by month, where the records have been cleaned by
    the agg_day_duplicates() procedure.
    """
    # Note that this function was built quickly with but one simple functional
    # test... more testing may be required if errors arise.

    def key_func(clean_dict_record):
        """
        Return a key to be used in sorting the records for
        aggregation.
        """
        return (clean_dict_record['longitude'],
                clean_dict_record['latitude'],
                clean_dict_record['month'])

    # Sort the records and get an iterator over the (key, group) pairs,
    # extracting the groups from the iterator.
    clean_dict_records = sorted(clean_dict_records, key=key_func)
    iterator = itertools.groupby(clean_dict_records, key_func)
    groups = [list(group) for _, group in iterator]

    def reducer(rec1, rec2):
        """ Amalgamate two cleaned records according to Dr. Zhou's rule. """
        maximum = rec1['max'] if rec1['max'] > rec2['max'] else rec2['max']
        return {'longitude': rec1['longitude'],
                'latitude': rec1['latitude'],
                'month': rec1['month'],
                'mean': rec1['mean'] + rec2['mean'],
                'max': maximum}
    # Extract the groups and process them to build the result.
    result = []
    for group in groups:
        reduction = reduce(reducer, group)
        reduction['mean'] /= len(group)
        result.append(reduction)
    return result

def write_clean_records(records, pollutant):
    """
    Given a list of processed records with the type and year, write them
    to the appropriate output file.
    """
    path = os.path.join(DATA_ROOT,
                        'clean/monthly_' + pollutant + '_1990-2015.csv')
    with open(path, 'a') as outfile:
        fieldnames = ['longitude', 'latitude', 'month', 'max', 'mean']
        writer = csv.DictWriter(outfile, fieldnames)
        writer.writeheader()
        for row in records:
            writer.writerow(row)

def main():
    """ Application main. """
    records = read_data_file('ozone', 2000)
    write_clean_records(agg_by_month(agg_day_duplicates(records)), 'ozone')

if __name__ == "__main__":
    main()
