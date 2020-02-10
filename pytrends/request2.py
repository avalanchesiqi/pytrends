# -*- coding: utf-8 -*-
"""
This is the main class of google trends crawler for daily interests.
It crawls historical interest data from web search or youtube search, and also returns the topic id of input terms.

Inspired by https://github.com/GeneralMills/pytrends and modified by Siqi Wu.
Email: siqi dot wu at anu dot edu dot au
"""

import json, requests
import pandas as pd
from pandas.io.json._normalize import nested_to_record
from urllib3.util.retry import Retry
from pytrends import exceptions


class TrendReq(object):
    """
    Google Trends API
    """
    GET_METHOD = 'get'
    POST_METHOD = 'post'
    GENERAL_URL = 'https://trends.google.com/trends/api/explore'
    INTEREST_OVER_TIME_URL = 'https://trends.google.com/trends/api/widgetdata/multiline'
    INTEREST_BY_REGION_URL = 'https://trends.google.com/trends/api/widgetdata/comparedgeo'
    RELATED_QUERIES_URL = 'https://trends.google.com/trends/api/widgetdata/relatedsearches'
    TRENDING_SEARCHES_URL = 'https://trends.google.com/trends/hottrends/visualize/internal/data'
    TOP_CHARTS_URL = 'https://trends.google.com/trends/api/topcharts'
    SUGGESTIONS_URL = 'https://trends.google.com/trends/api/autocomplete/'
    CATEGORIES_URL = 'https://trends.google.com/trends/api/explore/pickers/category'
    TODAY_SEARCHES_URL = 'https://trends.google.com/trends/api/dailytrends'

    def __init__(self, hl='en-US', tz=360, geo='', timeout=(2, 5), proxies='',
                 retries=0, backoff_factor=0):
        """
        Initialize default values for params
        """
        # google rate limit
        self.google_rl = 'You have reached your quota limit. Please try again later.'
        self.results = None
        # set user defined options used globally
        self.tz = tz
        self.hl = hl
        self.geo = geo
        self.kw_list = list()
        self.timeout = timeout
        self.proxies = proxies  # add a proxy option
        self.retries = retries
        self.backoff_factor = backoff_factor
        self.proxy_index = 0
        self.cookies = self.GetGoogleCookie()
        # intialize widget payloads
        self.token_payload = dict()
        self.interest_over_time_widget = dict()
        self.interest_by_region_widget = dict()
        self.related_topics_widget_list = list()
        self.related_queries_widget_list = list()

    def GetGoogleCookie(self):
        """
        Gets google cookie (used for each and every proxy; once on init otherwise)
        Removes proxy from the list on proxy error
        """
        while True:
            if len(self.proxies) > 0:
                proxy = {'https': self.proxies[self.proxy_index]}
            else:
                proxy = ''
            try:
                return dict(filter(lambda i: i[0] == 'NID', requests.get(
                    'https://trends.google.com/?geo={geo}'.format(
                        geo=self.hl[-2:]),
                    timeout=self.timeout,
                    proxies=proxy
                ).cookies.items()))
            except requests.exceptions.ProxyError:
                print('Proxy error. Changing IP')
                if len(self.proxies) > 0:
                    self.proxies.remove(self.proxies[self.proxy_index])
                else:
                    print('Proxy list is empty. Bye!')
                continue

    def GetNewProxy(self):
        """
        Increment proxy INDEX; zero on overflow
        """
        if self.proxy_index < (len(self.proxies) - 1):
            self.proxy_index += 1
        else:
            self.proxy_index = 0

    def _get_data(self, url, method=GET_METHOD, trim_chars=0, **kwargs):
        """Send a request to Google and return the JSON response as a Python object
        :param url: the url to which the request will be sent
        :param method: the HTTP method ('get' or 'post')
        :param trim_chars: how many characters should be trimmed off the beginning of the content of the response
            before this is passed to the JSON parser
        :param kwargs: any extra key arguments passed to the request builder (usually query parameters or data)
        :return:
        """
        s = requests.session()
        # Retries mechanism. Activated when one of statements >0 (best used for proxy)
        if self.retries > 0 or self.backoff_factor > 0:
            retry = Retry(total=self.retries, read=self.retries,
                          connect=self.retries,
                          backoff_factor=self.backoff_factor)

        s.headers.update({'accept-language': self.hl})
        if len(self.proxies) > 0:
            self.cookies = self.GetGoogleCookie()
            s.proxies.update({'https': self.proxies[self.proxy_index]})
        if method == TrendReq.POST_METHOD:
            response = s.post(url, timeout=self.timeout,
                              cookies=self.cookies, **kwargs)  # DO NOT USE retries or backoff_factor here
        else:
            response = s.get(url, timeout=self.timeout, cookies=self.cookies,
                             **kwargs)  # DO NOT USE retries or backoff_factor here
        # check if the response contains json and throw an exception otherwise
        # Google mostly sends 'application/json' in the Content-Type header,
        # but occasionally it sends 'application/javascript
        # and sometimes even 'text/javascript
        if response.status_code == 200 and 'application/json' in \
                response.headers['Content-Type'] or \
                'application/javascript' in response.headers['Content-Type'] or \
                'text/javascript' in response.headers['Content-Type']:
            # trim initial characters
            # some responses start with garbage characters, like ")]}',"
            # these have to be cleaned before being passed to the json parser
            content = response.text[trim_chars:]
            # parse json
            self.GetNewProxy()
            return json.loads(content)
        else:
            # error
            raise exceptions.ResponseError(
                'The request failed: Google returned a '
                'response with code {0}.'.format(response.status_code),
                response=response)

    def build_payload(self, kw_list, cat=0, timeframe='today 5-y', geo='',
                      gprop=''):
        """Create the payload for related queries, interest over time and interest by region"""
        self.kw_list = kw_list
        self.geo = geo or self.geo
        self.token_payload = {
            'hl': self.hl,
            'tz': self.tz,
            'req': {'comparisonItem': [], 'category': cat, 'property': gprop}
        }

        # build out json for each keyword
        for kw in self.kw_list:
            keyword_payload = {'keyword': kw, 'time': timeframe,
                               'geo': self.geo}
            self.token_payload['req']['comparisonItem'].append(keyword_payload)
        # requests will mangle this if it is not a string
        self.token_payload['req'] = json.dumps(self.token_payload['req'])
        # get tokens
        self._tokens()
        return

    def _tokens(self):
        """Makes request to Google to get API tokens for interest over time, interest by region and related queries"""
        # make the request and parse the returned json
        widget_dict = self._get_data(
            url=TrendReq.GENERAL_URL,
            method=TrendReq.GET_METHOD,
            params=self.token_payload,
            trim_chars=4,
        )['widgets']
        # order of the json matters...
        first_region_token = True
        # clear self.related_queries_widget_list and self.related_topics_widget_list
        # of old keywords'widgets
        self.related_queries_widget_list[:] = []
        self.related_topics_widget_list[:] = []
        # assign requests
        for widget in widget_dict:
            if widget['id'] == 'TIMESERIES':
                self.interest_over_time_widget = widget
            if widget['id'] == 'GEO_MAP' and first_region_token:
                self.interest_by_region_widget = widget
                first_region_token = False
            # response for each term, put into a list
            if 'RELATED_TOPICS' in widget['id']:
                self.related_topics_widget_list.append(widget)
            if 'RELATED_QUERIES' in widget['id']:
                self.related_queries_widget_list.append(widget)
        return

    def interest_over_time(self):
        """Request data from Google's Interest Over Time section and return a dataframe"""

        over_time_payload = {
            # convert to string as requests will mangle
            'req': json.dumps(self.interest_over_time_widget['request']),
            'token': self.interest_over_time_widget['token'],
            'tz': self.tz
        }

        # make the request and parse the returned json
        req_json = self._get_data(
            url=TrendReq.INTEREST_OVER_TIME_URL,
            method=TrendReq.GET_METHOD,
            trim_chars=5,
            params=over_time_payload,
        )

        df = pd.DataFrame(req_json['default']['timelineData'])
        if (df.empty):
            return df

        df['date'] = pd.to_datetime(df['time'].astype(dtype='float64'),
                                    unit='s')
        df = df.set_index(['date']).sort_index()
        # split list columns into seperate ones, remove brackets and split on comma
        result_df = df['value'].apply(lambda x: pd.Series(
            str(x).replace('[', '').replace(']', '').split(',')))
        # rename each column with its search term, relying on order that google provides...
        for idx, kw in enumerate(self.kw_list):
            # there is currently a bug with assigning columns that may be
            # parsed as a date in pandas: use explicit insert column method
            result_df.insert(len(result_df.columns), kw,
                             result_df[idx].astype('int'))
            del result_df[idx]

        if 'isPartial' in df:
            # make other dataframe from isPartial key data
            # split list columns into seperate ones, remove brackets and split on comma
            df = df.fillna(False)
            result_df2 = df['isPartial'].apply(lambda x: pd.Series(
                str(x).replace('[', '').replace(']', '').split(',')))
            result_df2.columns = ['isPartial']
            # concatenate the two dataframes
            final = pd.concat([result_df, result_df2], axis=1)
        else:
            final = result_df
            final['isPartial'] = False

        return final

    def related_topics(self):
        """Request data from Google's Related Topics section and return a dictionary of dataframes
        If no top and/or rising related topics are found, the value for the key "top" and/or "rising" will be None
        """

        # make the request
        related_payload = dict()
        result_dict = dict()
        for request_json in self.related_topics_widget_list:
            # ensure we know which keyword we are looking at rather than relying on order
            kw = request_json['request']['restriction'][
                'complexKeywordsRestriction']['keyword'][0]['value']
            # convert to string as requests will mangle
            related_payload['req'] = json.dumps(request_json['request'])
            related_payload['token'] = request_json['token']
            related_payload['tz'] = self.tz

            # parse the returned json
            req_json = self._get_data(
                url=TrendReq.RELATED_QUERIES_URL,
                method=TrendReq.GET_METHOD,
                trim_chars=5,
                params=related_payload,
            )

            # top topics
            try:
                top_list = req_json['default']['rankedList'][0][
                    'rankedKeyword']
                df_top = pd.DataFrame(
                    [nested_to_record(d, sep='_') for d in top_list])
            except KeyError:
                # in case no top topics are found, the lines above will throw a KeyError
                df_top = None

            # rising topics
            try:
                rising_list = req_json['default']['rankedList'][1][
                    'rankedKeyword']
                df_rising = pd.DataFrame(
                    [nested_to_record(d, sep='_') for d in rising_list])
            except KeyError:
                # in case no rising topics are found, the lines above will throw a KeyError
                df_rising = None

            result_dict[kw] = {'rising': df_rising, 'top': df_top}
        return result_dict
