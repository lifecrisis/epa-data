"""
Test the epa/clean.py module.

This module provides unit tests that ensure that the routines for cleaning
our EPA data work as expected.
"""


import os.path
import unittest

from glob import glob
from random import sample

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
        no2_pathnames = sample(no2_pathnames, 5)
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
        ozone_pathnames = sample(ozone_pathnames, 5)
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
        pm25_pathnames = sample(pm25_pathnames, 5)
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

    def setUp(self):
        """ Get a random data file for this test. """
        self.chosen_pollutant = sample(['no2', 'ozone', 'pm25'], 1)[0]
        self.chosen_year = sample(range(1990, 2016), 1)[0]
        while self.chosen_pollutant == 'pm25' and self.chosen_year < 1997:
            self.chosen_pollutant =\
                sample(['no2', 'ozone', 'pm25'], 1)[0]
            self.chosen_year =\
                sample(range(1990, 2016), 1)[0]
        # Select the file.
        self.records = epa.clean.read_data_file(self.chosen_pollutant,
                                                self.chosen_year)

    def test_agg_day_key_func(self):
        """ Test the key function used to group records. """
        for dict_rec in self.records:
            kf_result = epa.clean._agg_day_key_func(dict_rec)
            # Test for structure "(longitude, latitude, month, day)".
            self.assertEqual(dict_rec['longitude'], kf_result[0])
            self.assertEqual(dict_rec['latitude'], kf_result[1])
            self.assertEqual(dict_rec['month'], kf_result[2])
            self.assertEqual(dict_rec['day'], kf_result[3])

    def test_agg_day_iter(self):
        """
        Test the iterator used to aggregate records by location, month,
        and day.  The iterator is generated using "itertools.groupby()".
        """
        iterator = epa.clean._agg_day_iter(self.records)
        total_len = 0
        for key, group in iterator:
            group = list(group)
            for rec in group:
                self.assertEqual(rec['longitude'], group[0]['longitude'])
                self.assertEqual(rec['latitude'], group[0]['latitude'])
                self.assertEqual(rec['month'], group[0]['month'])
                self.assertEqual(rec['day'], group[0]['day'])
            total_len += len(group)
        self.assertEquals(total_len, len(self.records))

    def test_agg_day_duplicates(self):
        """
        Establish a record list fixture and apply epa.clean.agg_day_duplicates
        to it.  Check that the result conforms to expectations.
        """
        # Establish a local fixture.  There are 7 elements here.  The average
        # for all day duplicates should be 7.5.  All maximums should be 10.0
        record_list_fixture = [{'day': 3,
                                'latitude': '35.0',
                                'longitude': '30.0',
                                'max': 10.0,
                                'mean': 5.0,
                                'month': 3},
                               {'day': 3,
                                'latitude': '35.0',
                                'longitude': '30.0',
                                'max': 1.0,
                                'mean': 7.5,
                                'month': 3},
                               {'day': 3,
                                'latitude': '35.0',
                                'longitude': '30.0',
                                'max': 1.0,
                                'mean': 10.0,
                                'month': 3},
                               {'day': 2,
                                'latitude': '25.0',
                                'longitude': '20.0',
                                'max': 10.0,
                                'mean': 5.0,
                                'month': 2},
                               {'day': 2,
                                'latitude': '25.0',
                                'longitude': '20.0',
                                'max': 1.0,
                                'mean': 10.0,
                                'month': 2},
                               {'day': 1,
                                'latitude': '15.0',
                                'longitude': '10.0',
                                'max': 10.0,
                                'mean': 5.0,
                                'month': 1},
                               {'day': 1,
                                'latitude': '15.0',
                                'longitude': '10.0',
                                'max': 1.0,
                                'mean': 10.0,
                                'month': 1}]

        # Aggregate duplicates within the fixture.
        result = epa.clean.agg_day_duplicates(record_list_fixture)
        # Check agg_day_duplicates accuracy.
        self.assertEqual(len(result), 3)
        # Check result for first record.
        self.assertEqual(result[0]['latitude'], '15.0')
        self.assertEqual(result[0]['longitude'], '10.0')
        self.assertEqual(result[0]['mean'], 7.5)
        self.assertEqual(result[0]['max'], 10.0)
        self.assertNotIn('day', result[0].keys())
        # Check result for second record.
        self.assertEqual(result[1]['latitude'], '25.0')
        self.assertEqual(result[1]['longitude'], '20.0')
        self.assertEqual(result[1]['mean'], 7.5)
        self.assertEqual(result[1]['max'], 10.0)
        self.assertNotIn('day', result[1].keys())
        # Check result for third record.
        self.assertEqual(result[2]['latitude'], '35.0')
        self.assertEqual(result[2]['longitude'], '30.0')
        self.assertEqual(result[2]['mean'], 7.5)
        self.assertEqual(result[2]['max'], 10.0)
        self.assertNotIn('day', result[2].keys())
