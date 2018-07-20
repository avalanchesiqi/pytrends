#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Example to crawl youtube insight data.

Example inputs:
{"keyword": "adele - hello", "gt_queries": "\"adele hello\" + \"hello adele\"", "vid": ["YQHsXMglC9A", "DfG6VKnjrVw"], "release_date": "2015-10-23T06:54:18", "popularity": 1961453485}
{"keyword": "usher - rivals", "gt_queries": "\"usher rivals\" + \"rivals usher\"", "vid": ["IYRJYApTlUQ"], "release_date": "2016-09-02T07:00:01", "popularity": 14016762}
"""

import sys, os, argparse, time, json, logging
import numpy as np
import numpy.random as random
from datetime import datetime, timedelta

from pytrends.request import TrendReq
from pytrends.utils import diff_month, plot_interest_over_time, calendar_days


if __name__ == '__main__':
    # == == == == == == Part 1: Read youtube insight json from file == == == == == == #
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', help='input file path of vevo en queries', required=True)
    parser.add_argument('-o', '--output', help='output file path of search interests', required=True)
    parser.add_argument('-p', '--plot', dest='plot', action='store_true', default=False)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', default=False)
    parser.add_argument('-f', '--force', dest='force', action='store_true', default=False)
    parser.set_defaults(plot=False)
    parser.set_defaults(verbose=False)
    parser.set_defaults(force=False)
    args = parser.parse_args()

    input_path = args.input
    output_path = args.output
    logging.basicConfig(filename='./google_trends_crawler.log', level=logging.INFO)

    if not os.path.exists(input_path):
        print('>>> Input file does not exist!')
        print('>>> Exit...')
        sys.exit(1)

    if os.path.exists(output_path) and not args.force:
        print('>>> Output file already exists, rename or backup it before starting new job!')
        print('>>> Exit...')
        sys.exit(1)

    output_data = open(output_path, 'w+')

    # == == == == == == Part 2: Start GT crawler == == == == == == #
    # query period from 2008-01-01 to 2017-02-28
    all_period = '2008-01-01 2017-02-28'

    # set group property
    gprop = 'youtube'

    # global counter for number of requests
    global_cnt = 0

    # end date for all videos
    end_date = '2017-02-28'
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')

    # time range for backwards querying, every 8 month
    query_periods = ['2016-07-01 2017-02-28', '2015-11-01 2016-06-30', '2015-03-01 2015-10-31',
                     '2014-07-01 2015-02-28', '2013-11-01 2014-06-30', '2013-03-01 2013-10-31',
                     '2012-07-01 2013-02-28', '2011-11-01 2012-06-30', '2011-03-01 2011-10-31',
                     '2010-07-01 2011-02-28', '2009-11-01 2010-06-30', '2009-03-01 2009-10-31',
                     '2008-07-01 2009-02-28', '2008-01-01 2008-06-30']

    # read the input file, start the crawler
    with open(input_path, 'r') as input_data:
        for line in input_data:
            start_time = time.time()
            # local counter
            local_cnt = 0

            query_json = json.loads(line.rstrip())

            keyword = query_json['keyword']
            gt_queries = query_json['gt_queries']
            release_date = query_json['release_date'][:10]
            if release_date < '2008-01-01':
                release_date = '2008-01-01'
            release_date_obj = datetime.strptime(release_date, '%Y-%m-%d')
            num_days = (end_date_obj - release_date_obj).days

            # result dict
            google_trends = {'release_date': release_date, 'end_date': end_date, 'youtube_interest': []}

            logging.info('-' * 79)
            # if release date after 2016-07-01, we only request once to get daily interest
            if release_date >= '2016-07-01':
                logging.info('>>> One query to be sent for query ({0})'.format(gt_queries))
                query_period = '{0} {1}'.format(release_date, end_date)

                # initialize google trends crawler
                trends_crawler = TrendReq()
                trends_crawler.build_payload(keyword=gt_queries, timeframe=query_period, gprop=gprop)
                daily_interest = trends_crawler.interest_over_time()
                global_cnt += 1
                local_cnt += 1

                if daily_interest is not None:
                    google_trends['youtube_interest'] = daily_interest
                    if args.verbose:
                        logging.info('start date: {0}; number of days: {1}'.format(release_date, len(daily_interest)))

                    # visualize over time interest data
                    if args.plot:
                        plot_interest_over_time(google_trends)
                else:
                    logging.error('+++ No enough data for query ({0})'.format(gt_queries))
            # if release date before 2016-07-01, we query monthly data first then rescale daily data
            else:
                logging.info('>>> Query monthly interest from 2008 for query {0}'.format(gt_queries))

                # initialize google trends crawler
                trends_crawler = TrendReq()
                trends_crawler.build_payload(keyword=gt_queries, timeframe=all_period, gprop=gprop)
                alltime_interest = trends_crawler.interest_over_time()
                global_cnt += 1
                local_cnt += 1

                if alltime_interest is None:
                    logging.error('+++ No enough data for query ({0})'.format(keyword))
                    # print('+++ No enough data for {0}'.format(keyword))
                else:
                    if args.verbose:
                        logging.info('monthly query period: {0}'.format(all_period))
                        logging.info(','.join(map(str, alltime_interest)))
                        # print(gt_queries)
                        # print(alltime_interest)

                    # crawl every 8 months
                    # number of month till 2017-02, we hard code the query periods list
                    num_months = diff_month(end_date_obj, release_date_obj)
                    num_requests = num_months // 8 + 1

                    for request_idx in range(num_requests):
                        # sleep to avoid rate limit
                        time.sleep(12 * random.random())

                        batch_query_period = query_periods[request_idx]
                        batch_start_date, batch_end_date = batch_query_period.split()
                        batch_month_weight = alltime_interest[-8*request_idx-8:][:8]

                        trends_crawler.build_payload(keyword=gt_queries, timeframe=batch_query_period, gprop=gprop)
                        # return interest over time as a numpy array, every batch covers a month
                        batch_raw_interest = trends_crawler.interest_over_time()
                        global_cnt += 1
                        local_cnt += 1

                        if batch_raw_interest is not None:
                            # rescale via month weights
                            # get the number of days in each month
                            days_in_month = calendar_days(batch_start_date, batch_end_date)
                            stacked_days_in_month = [sum(days_in_month[:i+1]) for i in range(len(days_in_month))]

                            monthly_interest = [sum(batch_raw_interest[: stacked_days_in_month[0]])]
                            for j in range(1, len(stacked_days_in_month)):
                                monthly_interest.append(sum(batch_raw_interest[stacked_days_in_month[j-1]: stacked_days_in_month[j]]))

                            # put scale factor into one vector
                            sfactor = []
                            for i, t in enumerate(days_in_month):
                                if monthly_interest[i] == 0:
                                    sfactor.extend([0] * t)
                                else:
                                    sfactor.extend([batch_month_weight[i] / monthly_interest[i]] * t)
                            sfactor = np.array(sfactor)

                            scaled_interest = batch_raw_interest * sfactor
                            # normalize to max 100
                            # scaled_interest = scaled_interest/np.max(scaled_interest)*100
                            scaled_interest = scaled_interest.tolist()

                            if args.verbose:
                                logging.info('batch query period: {0}'.format(batch_query_period))
                                logging.info(','.join(map(str, batch_raw_interest)))

                        else:
                            start_date_obj = datetime.strptime(batch_start_date, '%Y-%m-%d')
                            end_date_obj = datetime.strptime(batch_end_date, '%Y-%m-%d')
                            batch_num_days = (end_date_obj - start_date_obj).days + 1
                            scaled_interest = [0] * batch_num_days

                        tmp = google_trends['youtube_interest']
                        scaled_interest.extend(tmp)
                        google_trends['youtube_interest'] = scaled_interest

                    if args.plot:
                        plot_interest_over_time(google_trends)

            # == == == == == == Part 4: Update output data == == == == == == #
            google_trends['youtube_interest'] = google_trends['youtube_interest'][-num_days:]
            if len(google_trends['youtube_interest']) > 0:
                query_json['trends'] = google_trends
                output_data.write('{0}\n'.format(json.dumps(query_json)))
            # get running time
            logging.info('>>> Local request number: {0}'.format(local_cnt))
            logging.info('>>> Running time: {0}'.format(str(timedelta(seconds=time.time() - start_time))[:-3]))
            logging.info('*' * 79)

    # == == == == == == Part 5: Close file handler == == == == == == #
    output_data.close()
    print('Total requests sent: {0}'.format(global_cnt))
