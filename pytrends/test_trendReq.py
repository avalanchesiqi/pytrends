from unittest import TestCase

from pytrends.request import TrendReq


class TestTrendReq(TestCase):

    def test__get_data(self):
        """ Should use same values as in the documentation.
        """
        trends_crawler = TrendReq()
        self.assertEqual(trends_crawler.hl, 'en-US')
        self.assertEqual(trends_crawler.tz, 360)
        self.assertEqual(trends_crawler.geo, '')
        self.assertTrue(trends_crawler.cookies['NID'])

    def test_build_payload(self):
        """ Should return the widgets to get data.
        """
        trends_crawler = TrendReq()
        trends_crawler.build_payload(keyword='python')
        self.assertIsNotNone(trends_crawler.token_payload)

    def test__tokens(self):
        trends_crawler = TrendReq()
        trends_crawler.build_payload(keyword='python')
        self.assertIsNotNone(trends_crawler.interest_over_time_widget)

    def test_interest_over_time(self):
        trends_crawler = TrendReq()
        trends_crawler.build_payload(keyword='python')
        self.assertIsNotNone(trends_crawler.interest_over_time())
