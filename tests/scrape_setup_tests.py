import unittest

import numpy as np

import scrapenhl2.scrape.scrape_setup as ss


class SSTest_check_types(unittest.TestCase):
    """Tests for scrape_setup.check_types()"""

    def test_int(self):
        self.assertTrue(ss.check_types(8471214))

    def test_str(self):
        self.assertTrue(ss.check_types('8471214'))

    def test_float(self):
        self.assertTrue(ss.check_types(8471214.0))

    def test_npint64(self):
        self.assertTrue(ss.check_types(np.int64(8471214)))

    def test_npint32(self):
        self.assertTrue(ss.check_types(np.int32(8471214)))

    def test_list(self):
        self.assertFalse(ss.check_types([1, 2, 3]))


class SSTest_infer_season_from_date(unittest.TestCase):
    """Tests for scrape_setup.infer_season_from_date()"""

    def test_jan(self):
        self.assertEquals(ss.infer_season_from_date('2017-01-01'), 2016)

    def test_jun(self):
        self.assertEquals(ss.infer_season_from_date('2017-06-01'), 2016)

    def test_sep(self):
        self.assertEquals(ss.infer_season_from_date('2017-08-01'), 2016)

    def test_dec(self):
        self.assertEquals(ss.infer_season_from_date('2017-12-01'), 2017)


if __name__ == '__main__':
    unittest.main()
