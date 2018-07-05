#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Example to add google trends search interests to youtube insight data.
"""

import sys, os, argparse, json, string, math, logging
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

from pytrends.request import TrendReq
from pytrends.utils import denormalize_interests


if __name__ == '__main__':
    # == == == == == == == == Part 1: Read youtube insight json from file == == == == == == == == #
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', help='input file path of video insight data', required=True)
    parser.add_argument('-o', '--output', help='output file path of video insight data with search interests', required=True)
    parser.add_argument('-f', '--force', dest='force', action='store_true', default=False)
    parser.set_defaults(force=False)
    args = parser.parse_args()

    input_path = args.input
    output_path = args.output
    logging.basicConfig(filename='./google_trends_crawler.log', level=logging.WARNING)

    if not os.path.exists(input_path):
        print('>>> Input file does not exist!')
        print('>>> Exit...')
        sys.exit(1)

    if os.path.exists(output_path) and not args.force:
        print('>>> Output file already exists, rename or backup it before starting new job!')
        print('>>> Exit...')
        sys.exit(1)

    output_data = open(output_path, 'w+')

    # == == == == == == Part 2: Set up pytrends crawler == == == == == == #
    # initialize google trends crawler
    trends_crawler = TrendReq()

    # == == == == == == == == Part 3: Start crawler == == == == == == == == #
    # punctuation to remove
    table = str.maketrans({key: None for key in string.punctuation})
    gprop_flags = ['web', 'youtube']

    # read the input file, start the crawler
    with open(input_path, 'r') as input_data:
        for line in input_data:
            video_data = json.loads(line.rstrip())
            video_title = video_data['snippet']['title']
            # strip all punctuations
            keyword = video_title.translate(table)
            published_at = video_data['snippet']['publishedAt'][:10]

            print('keyword', keyword)
            print('-'*79)

            google_trends = {'start_date': published_at, 'web_interest': [], 'youtube_interest': []}

            published_at = datetime(*map(int, published_at.split('-')))
            # our last date for Vevo music insights data is 2018-03-31
            terminate_date = datetime(2018, 3, 31)
            # every batch is 30 days
            num_batch = 30
            # we roll the query windows by num_batch / 2 each time
            num_epoch = 2 * int(math.ceil((terminate_date - published_at).days / num_batch)) - 1

            for epoch_idx in range(num_epoch):
                start_date = published_at + timedelta(days=epoch_idx*num_batch//2)
                end_date = published_at + timedelta(days=epoch_idx*num_batch//2 + num_batch - 1)
                query_period = '{0} {1}'.format(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
                google_trends['end_date'] = end_date

                for prop_idx, gprop in enumerate(['', 'youtube']):
                    # create payload for each epoch
                    trends_crawler.build_payload(keyword=keyword, timeframe=query_period, gprop=gprop)

                    # return interest over time as a numpy array
                    batch_interest = trends_crawler.interest_over_time()

                    tmp = google_trends['{0}_interest'.format(gprop_flags[prop_idx])]
                    if len(tmp) == 0:
                        google_trends['{0}_interest'.format(gprop_flags[prop_idx])] = batch_interest
                    else:
                        google_trends['{0}_interest'.format(gprop_flags[prop_idx])] = denormalize_interests(tmp, batch_interest, num_batch//2)
                    # print('{0}_interest'.format(gprop_flags[prop_idx]), len(google_trends['{0}_interest'.format(gprop_flags[prop_idx])]))
                    # print(google_trends['{0}_interest'.format(gprop_flags[prop_idx])])
                print('---- finish {0} epoch'.format(epoch_idx), query_period)

                #
                # print(interest_over_time_df.head())
                # print(len(interest_over_time_df))
                #
                # # == == == == == == Part 2: visualize returned data frame == == == == == == #
                # fig = plt.figure(figsize=(8, 5))
                # ax1 = fig.add_subplot(111)
                #
                # ax1.plot(interest_over_time_df, label=keyword)
                #
                # ax1.spines['right'].set_visible(False)
                # ax1.spines['top'].set_visible(False)
                # ax1.legend(loc='best', frameon=False)
                #
                # plt.tight_layout()
                # plt.show()

            break
