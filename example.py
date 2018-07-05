#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Example to add google trends search interests to youtube insight data.
"""

import sys, os, argparse, time, re, json, string, math, logging
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
    punctuation = str.maketrans({key: None for key in string.punctuation})
    # query item 0: group property
    gprop_ids = ['', 'youtube']
    gprop_names = ['web', 'youtube']

    # read the input file, start the crawler
    with open(input_path, 'r') as input_data:
        for line in input_data:
            print('-' * 79)
            start_time = time.time()

            video_data = json.loads(line.rstrip())

            # query item 1: keyword
            video_title = video_data['snippet']['title']
            # remove content in brackets
            keyword = re.sub(r'\(.*?\)', '', video_title)
            # strip all punctuation
            keyword = keyword.translate(punctuation)
            print('keyword:', keyword)

            # query item 2: query period
            published_at = video_data['snippet']['publishedAt'][:10]
            published_at = datetime(*map(int, published_at.split('-')))
            # our last date for Vevo music insights data is 2018-03-31
            terminate_date = datetime(2018, 3, 31)
            # every batch is 30 days
            num_batch = 30
            # we roll the query windows by num_batch / 2 each time
            num_epoch = 2 * int(math.ceil((terminate_date - published_at).days / num_batch)) - 1

            # result dict
            google_trends = {'start_date': published_at, 'end_date': None, 'web_interest': [], 'youtube_interest': []}

            for epoch_idx in range(num_epoch):
                start_date = published_at + timedelta(days=epoch_idx*num_batch//2)
                end_date = published_at + timedelta(days=epoch_idx*num_batch//2 + num_batch - 1)
                query_period = '{0} {1}'.format(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
                google_trends['end_date'] = end_date

                for prop_idx, gprop in enumerate(gprop_ids):
                    # create payload for each epoch
                    trends_crawler.build_payload(keyword=keyword, timeframe=query_period, gprop=gprop)

                    # return interest over time as a numpy array
                    batch_interest = trends_crawler.interest_over_time()

                    tmp = google_trends['{0}_interest'.format(gprop_names[prop_idx])].copy()
                    if len(tmp) == 0:
                        google_trends['{0}_interest'.format(gprop_names[prop_idx])] = batch_interest
                    else:
                        google_trends['{0}_interest'.format(gprop_names[prop_idx])] = denormalize_interests(tmp, batch_interest, num_batch//2)
                    # print(google_trends['{0}_interest'.format(gprop_flags[prop_idx])])
                # print('---- finish {0} epoch'.format(epoch_idx), query_period)
                # print(len(google_trends['web_interest']), google_trends['web_interest'])
                # print('**'*79)

            # == == == == == == Part 5: Visualize over time interest data == == == == == == #
            fig = plt.figure(figsize=(8, 6))
            ax1 = fig.add_subplot(211)
            ax2 = fig.add_subplot(212)

            start_date = google_trends['start_date']
            end_date = google_trends['end_date']
            duration = (end_date - start_date).days + 1  # add one day to account for the tail
            calendar_axis = [start_date + timedelta(days=i) for i in range(duration)]

            ax1.plot_date(calendar_axis, google_trends['web_interest'], '-', label='web interest')
            ax2.plot_date(calendar_axis, google_trends['youtube_interest'], '-', label='youtube interest')

            for ax_idx, ax in enumerate([ax1, ax2]):
                ax.spines['right'].set_visible(False)
                ax.spines['top'].set_visible(False)
                ax.legend(loc='best', frameon=False)

            # get running time
            print('>>> Total running time: {0}'.format(str(timedelta(seconds=time.time() - start_time)))[:-3])

            plt.tight_layout()
            plt.savefig('{0}.pdf'.format(keyword), bbox_inches='tight')
            # plt.show()

            # == == == == == == Part 4: Update output data == == == == == == #
            google_trends['start_date'] = google_trends['start_date'].strftime('%Y-%m-%d')
            google_trends['end_date'] = google_trends['end_date'].strftime('%Y-%m-%d')
            google_trends['web_interest'] = google_trends['web_interest'].tolist()
            google_trends['youtube_interest'] = google_trends['youtube_interest'].tolist()
            video_data['trends'] = google_trends
            output_data.write('{0}\n'.format(json.dumps(video_data)))
            print('-' * 79)

    # == == == == == == Part 6: Close file handler == == == == == == #
    output_data.close()
