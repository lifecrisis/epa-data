"""
Clean and amalgamate EPA data for analysis.
"""


import csv
import os.path


# Project directory root.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Data directory roots.
DATA_ROOT = os.path.join(PROJECT_ROOT, 'data/')
NO2_DATA_ROOT = os.path.join(DATA_ROOT, 'no2_raw_data/')
OZONE_DATA_ROOT = os.path.join(DATA_ROOT, 'ozone_raw_data/')
PM25_DATA_ROOT = os.path.join(DATA_ROOT, 'pm25_raw_data/')

# Headers expected in every CSV file.
HEADERS_IN = ['State Code',
              'County Code',
              'Site Num',
              'Parameter Code',
              'POC',
              'Latitude',
              'Longitude',
              'Datum',
              'Parameter Name',
              'Sample Duration',
              'Pollutant Standard',
              'Date Local',
              'Units of Measure',
              'Event Type',
              'Observation Count',
              'Observation Percent',
              'Arithmetic Mean',
              '1st Max Value',
              '1st Max Hour',
              'AQI',
              'Method Code',
              'Method Name',
              'Local Site Name',
              'Address',
              'State Name',
              'County Name',
              'City Name',
              'CBSA Name',
              'Date of Last Change']


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


def epa_data_reader(pollutant, year):
    """
    Return a csv file reader object for reading from our raw data files.

    Raises IOError if bad specs are passed due to calling load_data_file().
    """

    # Might raise IOError.
    f = load_data_file(pollutant, year)
    return csv.reader(f)
