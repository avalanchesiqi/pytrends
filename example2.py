#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Example to crawl search interest data from Google trends (unofficial) API wrapper.

Example inputs:
{"keyword": "adele - rolling in the deep", "gt_queries": "\"adele\" \"rolling in the deep\"", "vid": ["O-Dmt2-7VqQ", "rYEDA3JcQqw"], "start_date": "2010-12-09", "popularity": 1135201492}

Example query:
python example.py -i data/example_queries.json -o data/example_out.json -v
"""

from __future__ import print_function, division
import sys, os, argparse, time, json, logging
import numpy as np
from datetime import datetime, timedelta

from pytrends.request import TrendReq
from pytrends.utils import reformat, diff_month, calendar_days
from pytrends import dailydata
# from pytrends.utils import plot_interest_over_time


if __name__ == '__main__':
    # == == == == == == Part 1: Read youtube insight json from file == == == == == == #
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', help='input file path', required=True)
    parser.add_argument('-p', '--plot', dest='plot', action='store_true', default=False)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', default=False)
    parser.set_defaults(plot=False)
    parser.set_defaults(verbose=False)
    args = parser.parse_args()

    input_path = args.input
    logging.basicConfig(filename='./google_trends_crawler.log', level=logging.INFO)

    if not os.path.exists(input_path):
        print('>>> Input file does not exist!')
        print('>>> Exit...')
        sys.exit(1)

    # == == == == == == Part 3: Start Google trends crawler == == == == == == #
    # read queries from the input file
    with open(input_path, 'r') as input_data:
        for line in input_data:
            query_json = json.loads(line.rstrip())
            keyword = query_json['keyword']
            mid = query_json['mid']

            start_date_str = query_json['start_date']
            end_date_str = query_json['end_date']
            start_date_obj = datetime.strptime(start_date_str, '%Y-%m-%d')
            logging.info('>>> Query for topic {0}'.format(keyword))

            # result dict
            google_trends = {'start_date': start_date_str, 'end_date': end_date_str, 'daily_search': []}

            res_df = dailydata.get_daily_data(word=mid, start_year=2017, start_mon=1, stop_year=2018, stop_mon=4)
            res_df.to_csv('data/{0}.csv'.format(keyword))
