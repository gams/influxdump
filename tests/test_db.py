# -*- coding: utf-8 -*-
import unittest

from influxdump.db import Query


class TestQuery(unittest.TestCase):
    def test_default_query(self):
        q = Query("test_measurement")
        self.assertEqual(
                q.get_query(),
                "SELECT * FROM \"test_measurement\""
        )
