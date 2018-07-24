#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Example to crawl youtube interest data from Google trends platform.

Example inputs:
{"keyword": "adele - hello", "gt_queries": "\"adele hello\" + \"hello adele\"", "vid": ["YQHsXMglC9A", "DfG6VKnjrVw"], "release_date": "2015-10-23T06:54:18", "popularity": 1961453485}
{"keyword": "usher - rivals", "gt_queries": "\"usher rivals\" + \"rivals usher\"", "vid": ["IYRJYApTlUQ"], "release_date": "2016-09-02T07:00:01", "popularity": 14016762}

Example query:
python example.py -i data/query_partition.json -o data/out_partition.json -v
"""

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

    # query period from 2008-01-01 to 2017-02-28
    ALL_PERIOD = '2008-01-01 2017-02-28'

    # set group property
    GPROP = 'youtube'

    # global counter for number of requests
    GLOBAL_CNT = 0

    # end date for all videos
    END_DATE = '2017-02-28'
    END_DATE_OBJ = datetime.strptime(END_DATE, '%Y-%m-%d')

    # time range for backwards querying, every 8 month
    QUERY_PERIODS = ['2016-07-01 2017-02-28', '2015-11-01 2016-06-30', '2015-03-01 2015-10-31',
                     '2014-07-01 2015-02-28', '2013-11-01 2014-06-30', '2013-03-01 2013-10-31',
                     '2012-07-01 2013-02-28', '2011-11-01 2012-06-30', '2011-03-01 2011-10-31',
                     '2010-07-01 2011-02-28', '2009-11-01 2010-06-30', '2009-03-01 2009-10-31',
                     '2008-07-01 2009-02-28', '2008-01-01 2008-06-30']

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
                    visited_query.remove(keyword)
                    continue

                gt_queries = query_json['gt_queries']
                release_date = query_json['release_date'][:10]
                if release_date < '2008-01-01':
                    release_date = '2008-01-01'
                release_date_obj = datetime.strptime(release_date, '%Y-%m-%d')
                # in Google trends setting, both ends are inclusive
                num_days = (END_DATE_OBJ - release_date_obj).days + 1

                # result dict
                google_trends = {'release_date': release_date, 'end_date': END_DATE, 'youtube_interest': []}

                logging.info('-' * 79)
                # ----------- crawl branch 1 -----------
                # if release date after 2016-07-01, we only request once to get daily interest
                if release_date >= '2016-07-01':
                    logging.info('>>> One query to be sent for query ({0})'.format(gt_queries))
                    query_period = '{0} {1}'.format(release_date, END_DATE)

                    # sleep before crawling
                    time.sleep(SLEEP_TIME)

                    # initialize and start google trends crawler
                    trends_crawler = TrendReq()
                    trends_crawler.build_payload(keyword=gt_queries, timeframe=query_period, gprop=GPROP)
                    daily_interest = trends_crawler.interest_over_time().tolist()
                    GLOBAL_CNT += 1
                    local_cnt += 1

                    if daily_interest is not None:
                        google_trends['youtube_interest'] = daily_interest
                        if args.verbose:
                            logging.info('start date: {0}; number of days: {1}'.format(release_date, len(daily_interest)))
                            logging.info(','.join(map(str, daily_interest)))

                        # # visualize over time interest data
                        # if args.plot:
                        #     plot_interest_over_time(google_trends)
                    else:
                        logging.error('+++ No enough data for query ({0})'.format(gt_queries))
                # ----------- crawl branch 2 -----------
                # if release date before 2016-07-01, we query monthly data first then rescale to daily data
                else:
                    logging.info('>>> Query monthly interest from 2008 for query {0}'.format(gt_queries))

                    # sleep before crawling
                    time.sleep(SLEEP_TIME)

                    # initialize and start google trends crawler
                    trends_crawler = TrendReq()
                    trends_crawler.build_payload(keyword=gt_queries, timeframe=ALL_PERIOD, gprop=GPROP)
                    alltime_interest = trends_crawler.interest_over_time().tolist()
                    GLOBAL_CNT += 1
                    local_cnt += 1

                    if alltime_interest is None:
                        logging.error('+++ No enough data for query ({0})'.format(keyword))
                    else:
                        if args.verbose:
                            logging.info('>>> ALL TIME query period: {0}'.format(ALL_PERIOD))
                            logging.info('>>> {0}'.format(','.join(map(str, alltime_interest))))

                        # crawl every 8 months
                        # number of months till 2017-02, we hard code the query periods list
                        num_months = diff_month(END_DATE_OBJ, release_date_obj)
                        num_requests = num_months // 8 + 1

                        for request_idx in range(num_requests):
                            batch_query_period = QUERY_PERIODS[request_idx]

                            if args.verbose:
                                logging.info('>> batch query period: {0}, request {1} out of {2}'.format(batch_query_period, request_idx+1, num_requests))

                            batch_start_date, batch_end_date = batch_query_period.split()
                            if request_idx == 0:
                                batch_month_weight = alltime_interest[-8:]
                            else:
                                batch_month_weight = alltime_interest[-8*request_idx-8: -8*request_idx]

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

                            batch_scaled_interest.extend(google_trends['youtube_interest'])
                            google_trends['youtube_interest'] = batch_scaled_interest

                        # if args.plot:
                        #     plot_interest_over_time(google_trends)

                # == == == == == == Part 4: Update output data == == == == == == #
                # slicing the last 'num_days' elements
                google_trends['youtube_interest'] = google_trends['youtube_interest'][-num_days:]

                # sanity check: length of youtube_interest should equal to num_days
                if not len(google_trends['youtube_interest']) == num_days:
                    logging.warning('+++ output the length of overtime interest does not equal to num of days!!!')

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
