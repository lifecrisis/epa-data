"""
Test the epa/clean.py module.

This module provides unit tests that ensure that the routines for cleaning
our EPA data work as expected.
"""

# TODO(jf): Test reading, then manipulating, then writing for each file set.
# TODO(jf): Test your code on the first/last lines of first/last files in both
#           sets.


import glob
import os.path
import random
import unittest

from epa import clean


class LoadDataTestCase(unittest.TestCase):
    """
    Test that all data files can be successfully loaded.

    Specifically, test "DATA_ROOT" constants and "load_data_file(type, year)"
    method.
    """

    def test_load_no2_files(self):
        """ Test that no2 data files can be loaded. """

        # Test accuracy of NO2_DATA_ROOT.
        no2_pathnames = glob.glob(clean.NO2_DATA_ROOT + '/daily_*.csv')
        self.assertEqual(len(no2_pathnames), 26)
        for pathname in no2_pathnames:
            self.assertTrue(os.path.exists(pathname))

        # Test that files can, in fact, be loaded into memory.
        no2_pathnames = random.sample(no2_pathnames, 5)
        for pathname in no2_pathnames:
            year = int(pathname[-8:-4])
            no2_file = clean.load_data_file('no2', year)
            self.assertIsInstance(no2_file, file)
            self.assertGreater(len(list(no2_file)), 0)
            no2_file.close()

        # Test that an incorrectly specified file raises an Error.
        self.assertRaises(IOError, clean.load_data_file, 'xxxxx', 2020)
        self.assertRaises(IOError, clean.load_data_file, 'xxxxx', 1995)
        self.assertRaises(IOError, clean.load_data_file, 'no2', 1980)
        self.assertRaises(IOError, clean.load_data_file, 'no2', 2020)

    def test_load_ozone_files(self):
        """ Test that ozone data files can be loaded. """

        # Test accuracy of OZONE_DATA_ROOT.
        ozone_pathnames = glob.glob(clean.OZONE_DATA_ROOT + '/daily_*.csv')
        self.assertEqual(len(ozone_pathnames), 26)
        for pathname in ozone_pathnames:
            self.assertTrue(os.path.exists(pathname))

        # Test that files can, in fact, be loaded into memory.
        ozone_pathnames = random.sample(ozone_pathnames, 5)
        for pathname in ozone_pathnames:
            year = int(pathname[-8:-4])
            ozone_file = clean.load_data_file('ozone', year)
            self.assertIsInstance(ozone_file, file)
            self.assertGreater(len(list(ozone_file)), 10000)
            ozone_file.close()

        # Test that an incorrectly specified file raises an Error.
        self.assertRaises(IOError, clean.load_data_file, 'xxxxx', 2020)
        self.assertRaises(IOError, clean.load_data_file, 'xxxxx', 1995)
        self.assertRaises(IOError, clean.load_data_file, 'ozone', 1980)
        self.assertRaises(IOError, clean.load_data_file, 'ozone', 2020)

    def test_load_pm25_files(self):
        """ Test that pm25 data files can be loaded. """

        # Test accuracy of PM25_DATA_ROOT.
        pm25_pathnames = glob.glob(clean.PM25_DATA_ROOT + '/daily_*.csv')
        self.assertEqual(len(pm25_pathnames), 26)
        for pathname in pm25_pathnames:
            self.assertTrue(os.path.exists(pathname))

        # Test that files can, in fact, be loaded into memory.
        pm25_pathnames = random.sample(pm25_pathnames, 5)
        for pathname in pm25_pathnames:
            year = int(pathname[-8:-4])
            pm25_file = clean.load_data_file('pm25', year)
            self.assertIsInstance(pm25_file, file)
            self.assertGreater(len(list(pm25_file)), 0)
            pm25_file.close()

        # Test that an incorrectly specified file raises an Error.
        self.assertRaises(IOError, clean.load_data_file, 'xxxxx', 2020)
        self.assertRaises(IOError, clean.load_data_file, 'xxxxx', 1995)
        self.assertRaises(IOError, clean.load_data_file, 'pm25', 1980)
        self.assertRaises(IOError, clean.load_data_file, 'pm25', 2020)


class ReadDataTestCase(unittest.TestCase):
    """
    Test that epa.clean.parse_raw_data parses our CSV records accurately.
    """

    def setUp(self):
        """
        Initialize a list of every (pollutant_type, year) pair under analysis.
        """
        # Specify all pairs of pollutant type and time.
        import itertools
        self.pollutant_year_pairs = itertools.product(['no2', 'ozone', 'pm25'],
                                                      range(1990, 2016))

    def test_headers_match(self):
        """
        Test that the headers read from every file match clean.HEADERS_IN.
        """

        # Verify that all files come with the proper headers.
        for pair in self.pollutant_year_pairs:
            header_fields = next(clean.epa_data_reader(*pair))
            for i, field in enumerate(header_fields):
                self.assertEqual(len(header_fields),
                                 29,
                                 msg="Number of header fields doesn't match " +
                                     " for \"" + str(pair) + "\".")
                self.assertEqual(field,
                                 clean.HEADERS_IN[i],
                                 msg="Header match failed for \"" +
                                     str(pair) + "\".")
