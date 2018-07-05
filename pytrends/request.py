# -*- coding: utf-8 -*-
"""
This is the main class of google_trends crawler for daily interests.
It crawls historical interest data from web search or youtube search.

Inspired by https://github.com/GeneralMills/pytrends and modified by Siqi Wu.
Email: siqi dot wu at anu dot edu dot au
"""

import time, json, requests
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

from pytrends import exceptions


class TrendReq(object):
    """ A wrapper for Google Trends API - Interest over time.
    """

    GET_METHOD = 'get'
    POST_METHOD = 'post'

    GENERAL_URL = 'https://trends.google.com/trends/api/explore'
    INTEREST_OVER_TIME_URL = 'https://trends.google.com/trends/api/widgetdata/multiline'

    def __init__(self, hl='en-US', tz=360, geo='', proxies=''):
        """ Initialize default values for params
        """
        # google rate limit
        self.google_rl = 'You have reached your quota limit. Please try again later.'
        self.results = None

        # set user defined options used globally
        self.tz = tz
        self.hl = hl
        self.geo = geo  # geo scope, 'worldwide' is ''
        # we don't support multiple keywords therefore change kw_list to keyword
        self.keyword = None

        # self.proxies = proxies  # add a proxy option
        # # proxies format: {"http": "http://192.168.0.1:8888" , "https": "https://192.168.0.1:8888"}
        self.cookies = dict(filter(
            lambda i: i[0] == 'NID',
            requests.get('https://trends.google.com').cookies.items()
        ))

        # initialize widget payloads
        self.token_payload = dict()
        self.interest_over_time_widget = dict()

    def _get_data(self, url, method=GET_METHOD, trim_chars=0, **kwargs):
        """ Send a request to Google and return the JSON response as a Python object.

        :param url: the url to which the request will be sent
        :param method: the HTTP method ('get' or 'post')
        :param trim_chars: how many characters should be trimmed off the beginning of the content of the response
            before this is passed to the JSON parser
        :param kwargs: any extra key arguments passed to the request builder (usually query parameters or data)
        :return:
        """
        s = requests.session()
        # if self.proxies != '':
        #     s.proxies.update(self.proxies)
        if method == TrendReq.POST_METHOD:
            response = s.post(url, cookies=self.cookies, **kwargs)
        else:
            response = s.get(url, cookies=self.cookies, **kwargs)

        # check if the response contains json and throw an exception otherwise.
        # Google mostly sends 'application/json' in the Content-Type header,
        # but occasionally it sends 'application/javascript' and sometimes even 'text/javascript'
        if 'application/json' in response.headers['Content-Type'] or \
            'application/javascript' in response.headers['Content-Type'] or \
                'text/javascript' in response.headers['Content-Type']:

            # trim initial characters
            # some responses start with garbage characters, like ")]}',"
            # these have to be cleaned before being passed to the json parser
            content = response.text[trim_chars:]

            # parse json
            return json.loads(content)
        else:
            # this is often the case when the amount of keywords in the payload for the IP
            # is not allowed by Google
            raise exceptions.ResponseError('The request failed: Google returned a '
                                           'response with code {0}.'.format(response.status_code), response=response)

    def build_payload(self, keyword, cat=0, timeframe='today 5-y', geo='', gprop=''):
        """ Create the payload for interest over time.
        """
        self.keyword = keyword
        self.geo = geo
        self.token_payload = {
            'hl': self.hl,
            'tz': self.tz,
            'req': {'comparisonItem': [], 'category': cat, 'property': gprop}
        }

        # build out json for a keyword
        keyword_payload = {'keyword': keyword, 'time': timeframe, 'geo': self.geo}
        self.token_payload['req']['comparisonItem'].append(keyword_payload)
        # requests will mangle this if it is not a string
        self.token_payload['req'] = json.dumps(self.token_payload['req'])
        # get tokens
        self._tokens()
        return

    def _tokens(self):
        """ Makes request to Google to get API tokens for interest over time.
        """

        # make the request and parse the returned json
        widget_dict = self._get_data(url=TrendReq.GENERAL_URL, method=TrendReq.GET_METHOD, params=self.token_payload,
                                     trim_chars=4,)['widgets']

        # assign requests
        for widget in widget_dict:
            if widget['id'] == 'TIMESERIES':
                self.interest_over_time_widget = widget
        return

    def interest_over_time(self):
        """ Request data from Google's Interest Over Time section and return a dataframe.
        """

        over_time_payload = {
            # convert to string as requests will mangle
            'req': json.dumps(self.interest_over_time_widget['request']),
            'token': self.interest_over_time_widget['token'],
            'tz': self.tz
        }

        # make the request and parse the returned json
        req_json = self._get_data(url=TrendReq.INTEREST_OVER_TIME_URL, method=TrendReq.GET_METHOD, trim_chars=5,
                                  params=over_time_payload,)

        df = pd.DataFrame(req_json['default']['timelineData'])
        if df.empty:
            return None

        interest_array = np.array(df['value'].apply(lambda x: int(str(x)[1: -1])).tolist())
        return interest_array
