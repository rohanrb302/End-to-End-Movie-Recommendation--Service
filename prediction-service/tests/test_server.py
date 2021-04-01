import unittest

import app.server
from flask import Response


class TestServer(unittest.TestCase):
    def test_healthcheck(self):
        self.assertIsNotNone(app.server.healthcheck())

    def test_recommend_for_invalid_user(self):
        # Check for invalid user id - Negative user id
        r = app.server.recommend('-5')
        self.assertEqual(406, r.status_code)

        # Check for invalid user id - Non-numeric
        r = app.server.recommend('bad user id')
        self.assertEqual(406, r.status_code)
        r = app.server.recommend('123~~')
        self.assertEqual(406, r.status_code)
        r = app.server.recommend('l33t')
        self.assertEqual(406, r.status_code)


if __name__ == '__main__':
    unittest.main()
