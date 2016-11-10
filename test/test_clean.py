"""
Test the epa/clean.py module.

This module provides unit tests that ensure that the routines for cleaning
our EPA data work as expected.
"""


import os.path
import random
import unittest

from glob import glob

import epa.clean


class LoadDataTestCase(unittest.TestCase):
    """
    Test that all data files can be successfully loaded.

    Specifically, test "DATA_ROOT" constants and "_load_data_file(type, year)"
    method.
    """

    def test_load_no2_files(self):
        """ Test that no2 data files can be loaded. """

        # Test accuracy of NO2_DATA_ROOT.
        no2_pathnames = glob(epa.clean.NO2_DATA_ROOT + '/daily_*.csv')
        self.assertEqual(len(no2_pathnames), 26)
        for pathname in no2_pathnames:
            self.assertTrue(os.path.exists(pathname))

        # Test that files can, in fact, be loaded into memory.
        no2_pathnames = random.sample(no2_pathnames, 5)
        for pathname in no2_pathnames:
            year = int(pathname[-8:-4])
            no2_file = epa.clean._load_data_file('no2', year)
            self.assertIsInstance(no2_file, file)
            self.assertGreater(len(list(no2_file)), 0)
            no2_file.close()

        # Test that an incorrectly specified file raises an Error.
        self.assertRaises(IOError, epa.clean._load_data_file, 'xxxxx', 2020)
        self.assertRaises(IOError, epa.clean._load_data_file, 'xxxxx', 1995)
        self.assertRaises(IOError, epa.clean._load_data_file, 'no2', 1980)
        self.assertRaises(IOError, epa.clean._load_data_file, 'no2', 2020)

    def test_load_ozone_files(self):
        """ Test that ozone data files can be loaded. """

        # Test accuracy of OZONE_DATA_ROOT.
        ozone_pathnames = glob(epa.clean.OZONE_DATA_ROOT + '/daily_*.csv')
        self.assertEqual(len(ozone_pathnames), 26)
        for pathname in ozone_pathnames:
            self.assertTrue(os.path.exists(pathname))

        # Test that files can, in fact, be loaded into memory.
        ozone_pathnames = random.sample(ozone_pathnames, 5)
        for pathname in ozone_pathnames:
            year = int(pathname[-8:-4])
            ozone_file = epa.clean._load_data_file('ozone', year)
            self.assertIsInstance(ozone_file, file)
            self.assertGreater(len(list(ozone_file)), 10000)
            ozone_file.close()

        # Test that an incorrectly specified file raises an Error.
        self.assertRaises(IOError, epa.clean._load_data_file, 'xxxxx', 2020)
        self.assertRaises(IOError, epa.clean._load_data_file, 'xxxxx', 1995)
        self.assertRaises(IOError, epa.clean._load_data_file, 'ozone', 1980)
        self.assertRaises(IOError, epa.clean._load_data_file, 'ozone', 2020)

    def test_load_pm25_files(self):
        """ Test that pm25 data files can be loaded. """

        # Test accuracy of PM25_DATA_ROOT.
        pm25_pathnames = glob(epa.clean.PM25_DATA_ROOT + '/daily_*.csv')
        self.assertEqual(len(pm25_pathnames), 26)
        for pathname in pm25_pathnames:
            self.assertTrue(os.path.exists(pathname))

        # Test that files can, in fact, be loaded into memory.
        pm25_pathnames = random.sample(pm25_pathnames, 5)
        for pathname in pm25_pathnames:
            year = int(pathname[-8:-4])
            pm25_file = epa.clean._load_data_file('pm25', year)
            self.assertIsInstance(pm25_file, file)
            self.assertGreater(len(list(pm25_file)), 0)
            pm25_file.close()

        # Test that an incorrectly specified file raises an Error.
        self.assertRaises(IOError, epa.clean._load_data_file, 'xxxxx', 2020)
        self.assertRaises(IOError, epa.clean._load_data_file, 'xxxxx', 1995)
        self.assertRaises(IOError, epa.clean._load_data_file, 'pm25', 1980)
        self.assertRaises(IOError, epa.clean._load_data_file, 'pm25', 2020)


class AggregateDayDuplicatesTestCase(unittest.TestCase):
    """ Test that epa.clean.agg_day_duplicates() performs correctly. """

    def test_agg_day_key_func(self):
        """ Test the key function used to group records. """
        # Read random data file.
        chosen_year = random.sample(range(1990, 2016), 1)
        chosen_pollutant = random.sample(['no2', 'ozone', 'pm25'], 1)
        while chosen_pollutant == 'pm25' and chosen_year < 1997:
            chosen_year = random.sample(range(1990, 2016), 1)
            chosen_pollutant = random.sample(['no2', 'ozone', 'pm25'], 1)
        # Check for key_func accuracy.
        records = epa.clean.read_data_file(chosen_pollutant[0],
                                           chosen_year[0])
        for dict_rec in records:
            kf_result = epa.clean._agg_day_key_func(dict_rec)
            # Test for structure "(longitude, latitude, month, day)".
            self.assertEqual(dict_rec['longitude'], kf_result[0])
            self.assertEqual(dict_rec['latitude'], kf_result[1])
            self.assertEqual(dict_rec['month'], kf_result[2])
            self.assertEqual(dict_rec['day'], kf_result[3])
