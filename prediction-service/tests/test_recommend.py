import json
import unittest
from unittest.mock import patch

from app.recommend import Recommend


class TestRecommend(unittest.TestCase):

    def setUp(self):
        test_conf = None
        with open('tests/config/test_config.json') as cfg:
            test_conf = json.load(cfg)


        self.recommend = Recommend(
            test_conf['model']['recommendations_path'],
            test_conf['model']['mappings_path'],
            test_conf['model']['popular_movie_path'],None,
            test_conf['aws_access']
        )

    def test_valid_user(self):
        with patch.object(self.recommend, '_Recommend__record_recommendation'):
            recommendations = self.recommend.recommend(181270)
            self.assertIsNotNone(recommendations)
            self.assertEqual(20, len(recommendations.split(',')))

    def test_invalid_user(self):
        with patch.object(self.recommend, '_Recommend__record_recommendation'):
            recommendations = self.recommend.recommend(-1)
            self.assertIsNotNone(recommendations)
            self.assertEqual(20, len(recommendations.split(',')))


if __name__ == '__main__':
    unittest.main()
