import unittest

import scripts.rating_event_dump as script


class TestRatingEventDump(unittest.TestCase):
    def test_data_clean(self):
        # Test for negative values of timestamp, userid and rating
        # Test for empty strings in the movieid
        # Test for string format issues in the movie id
        self.assertEqual(False, script.validate_year('0'))
        self.assertEqual(True, script.validate_year('1996'))
        self.assertEqual(False, script.validate_year('199d6'))
        self.assertEqual(False, script.validate_year('19986'))
        self.assertEqual(True, script.validate_year('2020'))
        self.assertEqual(True, script.validate_year('1900'))

        self.assertEqual(False, script.validate_rating('-5'))
        self.assertEqual(False, script.validate_rating('-2'))
        self.assertEqual(True, script.validate_rating('3'))
        self.assertEqual(True, script.validate_rating('5'))
        self.assertEqual(False, script.validate_rating('0'))
        
        self.assertEqual(True, script.validate_timestamp(103242123))
        self.assertEqual(False, script.validate_timestamp(0))
        
        self.assertEqual(False, script.validate_userid(0))
        self.assertEqual(True, script.validate_userid(100))
        self.assertEqual(False, script.validate_userid(-100))
        self.assertEqual(True, script.validate_userid('100'))
        self.assertEqual(False, script.validate_userid('10ax0'))


if __name__ == "__main__":
    unittest.main()
