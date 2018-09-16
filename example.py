#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Example to crawl youtube interest data from Google trends platform.

Example inputs:
{"keyword": "adele - hello", "gt_queries": "\"adele hello\" + \"hello adele\"", "vid": ["YQHsXMglC9A", "DfG6VKnjrVw"], "start_date": "2015-10-23", "popularity": 1961453485}
{"keyword": "usher - rivals", "gt_queries": "\"usher rivals\" + \"rivals usher\"", "vid": ["IYRJYApTlUQ"], "start_date": "2016-09-02", "popularity": 14016762}

Example query:
python example.py -i data/query_partition.json -o data/out_partition.json -v
"""

from __future__ import print_function, division
import sys, os, argparse, time, json, logging
import numpy as np
from datetime import datetime, timedelta

from pytrends.request import TrendReq
from pytrends.utils import diff_month, calendar_days
# from pytrends.utils import plot_interest_over_time


if __name__ == '__main__':
    # == == == == == == Part 1: Read youtube insight json from file == == == == == == #
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', help='input file path of vevo en queries', required=True)
    parser.add_argument('-o', '--output', help='output file path of search interests', required=True)
    parser.add_argument('-p', '--plot', dest='plot', action='store_true', default=False)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', default=False)
    parser.set_defaults(plot=False)
    parser.set_defaults(verbose=False)
    args = parser.parse_args()

    input_path = args.input
    output_path = args.output
    logging.basicConfig(filename='./google_trends_crawler.log', level=logging.INFO)

    if not os.path.exists(input_path):
        print('>>> Input file does not exist!')
        print('>>> Exit...')
        sys.exit(1)

    visited_query = set()
    if os.path.exists(output_path):
        print('>>> Output file already exists, continue from breaking point...')
        with open(output_path, 'r') as existing_file:
            for line in existing_file:
                keyword = json.loads(line.rstrip())['keyword']
                visited_query.add(keyword)
        output_data = open(output_path, 'a+')
    else:
        print('>>> Start a new output file...')
        output_data = open(output_path, 'w+')

    # == == == == == == Part 2: Set up global parameters == == == == == == #
    # sleep to avoid rate limit
    SLEEP_TIME = 62

    # query period from 2009-12-01 to 2017-06-30
    ALL_PERIOD = '2009-12-01 2017-06-30'

    # set group property
    GPROP = 'youtube'

    # global counter for number of requests
    GLOBAL_CNT = 0

    # end date for all videos
    end_date_str = '2017-06-30'
    end_date_obj = datetime.strptime(end_date_str, '%Y-%m-%d')

    # time range for backwards querying, every 8 month
    QUERY_PERIODS = ['2016-11-01 2017-06-30', '2016-03-01 2016-10-31', '2015-07-01 2016-02-29',
                     '2014-11-01 2015-06-30', '2014-03-01 2014-10-31', '2013-07-01 2014-02-28',
                     '2012-11-01 2013-06-30', '2012-03-01 2012-10-31', '2011-07-01 2012-02-29',
                     '2010-11-01 2011-06-30', '2010-03-01 2010-10-31', '2009-12-01 2010-02-28']

    # == == == == == == Part 3: Start Google trends crawler == == == == == == #
    # read queries from the input file
    with open(input_path, 'r') as input_data:
        for line in input_data:
            try:
                start_time = time.time()
                # local counter
                local_cnt = 0

                query_json = json.loads(line.rstrip())
                keyword = query_json['keyword']

                # if current keyword exists in visited query, skip current keyword
                if len(visited_query) > 0 and keyword in visited_query:
                    # visited_query.remove(keyword)
                    continue

                gt_queries = query_json['gt_queries']
                start_date_str = query_json['start_date']
                start_date_obj = datetime.strptime(start_date_str, '%Y-%m-%d')
                # in Google trends setting, both ends are inclusive
                num_days = (end_date_obj - start_date_obj).days + 1

                # result dict
                google_trends = {'start_date': start_date_str, 'end_date': end_date_str, 'daily_search': []}

                logging.info('-' * 79)
                # ----------- crawl branch 1 -----------
                # if start date after 2016-11-01, we only request once to get daily interest
                if start_date_str >= '2016-11-01':
                    logging.info('>>> One query to be sent for query ({0})'.format(gt_queries))
                    query_period = '{0} {1}'.format(start_date_str, end_date_str)

                    # sleep before crawling
                    time.sleep(SLEEP_TIME)

                    # initialize and start google trends crawler
                    trends_crawler = TrendReq()
                    trends_crawler.build_payload(keyword=gt_queries, timeframe=query_period, gprop=GPROP)
                    daily_search = trends_crawler.interest_over_time().tolist()
                    GLOBAL_CNT += 1
                    local_cnt += 1

                    if daily_search is not None:
                        google_trends['daily_search'] = daily_search
                        if args.verbose:
                            logging.info('start date: {0}; number of days: {1}'.format(start_date_str, len(daily_search)))
                            logging.info(','.join(map(str, daily_search)))

                        # # visualize over time interest data
                        # if args.plot:
                        #     plot_interest_over_time(google_trends)
                    else:
                        logging.error('+++ No enough data for query ({0})'.format(gt_queries))
                # ----------- crawl branch 2 -----------
                # if release date before 2016-07-01, we query monthly data first then rescale to daily data
                else:
                    logging.info('>>> Query monthly interest from 2009-12-01 for query {0}'.format(gt_queries))

                    # sleep before crawling
                    time.sleep(SLEEP_TIME)

                    # initialize and start google trends crawler
                    trends_crawler = TrendReq()
                    trends_crawler.build_payload(keyword=gt_queries, timeframe=ALL_PERIOD, gprop=GPROP)
                    alltime_search = trends_crawler.interest_over_time()
                    GLOBAL_CNT += 1
                    local_cnt += 1

                    if alltime_search is None:
                        logging.error('+++ No enough data for query ({0})'.format(keyword))
                    else:
                        if args.verbose:
                            logging.info('>>> ALL TIME query period: {0}'.format(ALL_PERIOD))
                            logging.info('>>> {0}'.format(','.join(map(str, alltime_search))))

                        # crawl every 8 months
                        # number of months till 2017-06, we hard code the query periods list
                        num_months = diff_month(end_date_obj, start_date_obj)
                        num_requests = num_months // 8 + 1

                        for request_idx in range(num_requests):
                            batch_query_period = QUERY_PERIODS[request_idx]

                            if args.verbose:
                                logging.info('>> batch query period: {0}, request {1} out of {2}'.format(batch_query_period, request_idx+1, num_requests))

                            batch_start_date, batch_end_date = batch_query_period.split()
                            if request_idx == 0:
                                batch_month_weight = alltime_search[-8:]
                            else:
                                batch_month_weight = alltime_search[-8*request_idx-8: -8*request_idx]

                            # sleep before crawling
                            time.sleep(SLEEP_TIME)

                            # initialize and start google trends crawler
                            trends_crawler = TrendReq()
                            trends_crawler.build_payload(keyword=gt_queries, timeframe=batch_query_period, gprop=GPROP)
                            # return interest over time as a numpy array, every batch covers a month
                            batch_raw_interest = trends_crawler.interest_over_time()
                            GLOBAL_CNT += 1
                            local_cnt += 1

                            if batch_raw_interest is not None:
                                # rescale by the weight of each month
                                # first get the number of days in each month
                                days_in_month = calendar_days(batch_start_date, batch_end_date)
                                batch_num_months = len(days_in_month)
                                stacked_days_in_month = [sum(days_in_month[:i+1]) for i in range(batch_num_months)]

                                monthly_total_interest = [sum(batch_raw_interest[: stacked_days_in_month[0]])]
                                for j in range(1, batch_num_months):
                                    monthly_total_interest.append(sum(batch_raw_interest[stacked_days_in_month[j-1]: stacked_days_in_month[j]]))

                                # put scale factor into one vector
                                sfactor = []
                                for i, t in enumerate(days_in_month):
                                    if monthly_total_interest[i] == 0:
                                        sfactor.extend([0] * t)
                                    else:
                                        sfactor.extend([batch_month_weight[i] / monthly_total_interest[i]] * t)
                                sfactor = np.array(sfactor)

                                batch_scaled_interest = (batch_raw_interest * sfactor).tolist()

                                if args.verbose:
                                    logging.info('>>> {0}'.format(','.join(map(str, batch_raw_interest))))

                            else:
                                batch_start_date_obj = datetime.strptime(batch_start_date, '%Y-%m-%d')
                                batch_end_date_obj = datetime.strptime(batch_end_date, '%Y-%m-%d')
                                batch_num_days = (batch_end_date_obj - batch_start_date_obj).days + 1
                                batch_scaled_interest = [0] * batch_num_days

                                if args.verbose:
                                    logging.info('>>> {0}'.format(','.join(map(str, batch_scaled_interest))))

                            batch_scaled_interest.extend(google_trends['daily_search'])
                            google_trends['daily_search'] = batch_scaled_interest

                        # if args.plot:
                        #     plot_interest_over_time(google_trends)

                # == == == == == == Part 4: Update output data == == == == == == #
                # slicing the last 'num_days' elements, this removes the first 7 days in 2009-12
                google_trends['daily_search'] = google_trends['daily_search'][-num_days:]

                # sanity check: length of youtube_interest should equal to num_days
                if not len(google_trends['daily_search']) == num_days:
                    logging.warning('+++ Error output the length of overtime interest does not equal to num of days!!!')

                query_json['trends'] = google_trends
                output_data.write('{0}\n'.format(json.dumps(query_json)))

                # get running time
                logging.info('>>> Local request number: {0}'.format(local_cnt))
                logging.info('>>> Running time: {0}'.format(str(timedelta(seconds=time.time() - start_time))[:-3]))
                logging.info('*' * 79)
            except Exception as e:
                logging.error('+++ Error on line {0}: {1}'.format(sys.exc_info()[-1].tb_lineno, str(e)))
                # handle rate limit message, else skip (such as bad requests)
                if 'code 429' in str(e):
                    break

    # == == == == == == Part 5: Close file handler == == == == == == #
    output_data.close()
    logging.info('>>> Total request sent: {0}'.format(GLOBAL_CNT))
