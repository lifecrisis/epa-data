"""
Clean and amalgamate EPA data for analysis.
"""


import os.path


# Project directory root.
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Data directory roots.
DATA_ROOT = os.path.join(PROJECT_ROOT, 'data/')
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
    else:
        msg = "Data file with type '" + pollutant + "' and year '" +\
              str(year) + "' not found."
        raise IOError(msg)

    # Return a handle to the selected file.
    return result
