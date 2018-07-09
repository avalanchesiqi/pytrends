#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Example to add google trends search interests to youtube insight data.
"""

import sys, os, argparse, time, re, json, string, calendar, logging
import numpy.random as random
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter

from pytrends.request import TrendReq
from pytrends.utils import diff_month


if __name__ == '__main__':
    # == == == == == == == == Part 1: Read youtube insight json from file == == == == == == == == #
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', help='input file path of video insight data', required=True)
    parser.add_argument('-o', '--output', help='output file path of video insight data with search interests', required=True)
    parser.add_argument('-s', '--save', dest='save', action='store_true', default=False)
    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', default=False)
    parser.add_argument('-f', '--force', dest='force', action='store_true', default=False)
    parser.set_defaults(save=False)
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

    # == == == == == == == == Part 2: Start crawler == == == == == == == == #
    # query period from 2008
    all_period = 'all_2008'
    # number of month from 2008-01 to 2017-07
    all_num_month = 9 * 12 + 7
    # punctuation to remove
    punctuation = str.maketrans({key: None for key in string.punctuation})
    # query item 0: group property
    gprop_ids = ['', 'youtube']
    # gprop_ids = ['youtube']
    gprop_names = ['web', 'youtube']
    # gprop_names = ['youtube']

    # read the input file, start the crawler
    with open(input_path, 'r') as input_data:
        for line in input_data:
            start_time = time.time()

            video_json = json.loads(line.rstrip())

            start_date = datetime.strptime(video_json['insights']['startDate'], '%Y-%m-%d')

            # == == == == == == == == Part 3: Extract daily view data == == == == == == == == #
            # we set a cut off date because Google changed overtime interest data on 05 Aug, 2017.
            # all time series data will be before or at 31 Jul, 2017.
            num_daily = (datetime.strptime('2017-07-31', '%Y-%m-%d') - start_date).days + 1
            daily_axis = [start_date + timedelta(days=di) for di in range(num_daily)]

            days = video_json['insights']['days']
            compact_daily_view = video_json['insights']['dailyView']
            # daily view series
            daily_view = [0] * num_daily
            for i, d in enumerate(days):
                if d >= num_daily:
                    break
                daily_view[d] = compact_daily_view[i]
            # == == == == == == == == == == == == == == == ==== == == == == == == == == == == #

            # number of month till 2017-07
            num_monthly = diff_month(datetime(2017, 7, 31), start_date)
            monthly_axis = [start_date + relativedelta(months=i) for i in range(num_monthly)]
            logging.info('>>> Number of query to be sent: {0}'.format((num_monthly + 1) * 2))

            # result dict
            google_trends = {'start_date': start_date, 'end_date': datetime(2017, 7, 31),
                             'web_interest': [], 'youtube_interest': [],
                             'web_interest_monthly': [], 'youtube_interest_monthly': []}

            # query item 1: keyword
            video_title = video_json['snippet']['title']
            # remove content in brackets
            keyword = re.sub(r'\(.*?\)', '', video_title)
            # strip all punctuation
            keyword = keyword.translate(punctuation)
            logging.info('>>> Start to crawl google trends for video: {0}'.format(video_title))
            logging.info('>>> Query keyword: {0}'.format(keyword))

            # initialize google trends crawler
            trends_crawler = TrendReq()
            # trends_crawler = TrendReq(proxies={'https': 'https://103.88.234.90:53281'})

            # query item 2: query period
            for prop_idx, gprop in enumerate(gprop_ids):
                # == == == == == == == == Part 4: Query monthly data == == == == == == == == #
                # create payload for all time query
                trends_crawler.build_payload(keyword=keyword, timeframe=all_period, gprop=gprop)
                logging.info('-'*79)
                monthly_interest = trends_crawler.interest_over_time().tolist()
                google_trends['{0}_interest_monthly'.format(gprop_names[prop_idx])] = monthly_interest[:all_num_month][-num_monthly:]
                if args.verbose:
                    logging.info('start date: {0}; number of months: {1}'.format(video_json['insights']['startDate'], num_monthly))
                    logging.info(','.join(map(str, google_trends['{0}_interest_monthly'.format(gprop_names[prop_idx])])))

                # == == == == == == == == Part 5: Query daily data == == == == == == == == #
                for month_idx in range(num_monthly):
                    # sleep to avoid rate limit
                    time.sleep(5 * random.random())

                    current_year = (google_trends['start_date'] + relativedelta(months=month_idx)).year
                    current_month = (google_trends['start_date'] + relativedelta(months=month_idx)).month
                    last_day_of_month = calendar.monthrange(current_year, current_month)[1]

                    current_start_date = datetime(current_year, current_month, 1)
                    current_end_date = datetime(current_year, current_month, last_day_of_month)
                    query_period = '{0} {1}'.format(current_start_date.strftime('%Y-%m-%d'),
                                                    current_end_date.strftime('%Y-%m-%d'))

                    # create payload for each month
                    logging.info('-' * 79)
                    trends_crawler.build_payload(keyword=keyword, timeframe=query_period, gprop=gprop)

                    # return interest over time as a numpy array, every batch covers a month
                    batch_interest = trends_crawler.interest_over_time()
                    if args.verbose:
                        logging.info('query period: {0}'.format(query_period))
                        logging.info(','.join(map(str, batch_interest)))

                    if batch_interest is not None:
                        denormalized_interest = google_trends['{0}_interest_monthly'.format(gprop_names[prop_idx])][month_idx] / sum(batch_interest) * batch_interest
                        google_trends['{0}_interest'.format(gprop_names[prop_idx])].extend(denormalized_interest.tolist())

                # we need to cut date before start date
                google_trends['{0}_interest'.format(gprop_names[prop_idx])] = google_trends['{0}_interest'.format(gprop_names[prop_idx])][-num_daily:]
                logging.info('*'*79)

            # == == == == == == Part 6: Update output data == == == == == == #
            google_trends['start_date'] = google_trends['start_date'].strftime('%Y-%m-%d')
            google_trends['end_date'] = google_trends['end_date'].strftime('%Y-%m-%d')
            video_json['trends'] = google_trends
            output_data.write('{0}\n'.format(json.dumps(video_json)))
            # get running time
            logging.info('>>> file output completed!')
            logging.info('>>> Total running time: {0}'.format(str(timedelta(seconds=time.time() - start_time))[:-3]))
            logging.info('*' * 79)

            # == == == == == == Part 7: Option - visualize over time interest data == == == == == == #
            if args.save:
                fig = plt.figure(figsize=(14, 8))
                ax1 = fig.add_subplot(2, 2, 1)
                ax2 = fig.add_subplot(2, 2, 2)
                ax3 = fig.add_subplot(2, 2, 3, sharex=ax1)
                ax4 = fig.add_subplot(2, 2, 4, sharex=ax2)
                axes = [ax1, ax2, ax3, ax4]
                axes_twin = []
                for ax in axes:
                    axes_twin.append(ax.twinx())

                for prop_idx, gprop in enumerate(gprop_ids):
                    for row_idx in range(2):
                        # plot daily view
                        axes[2*row_idx+prop_idx].plot_date(daily_axis, daily_view, 'k-', label='daily view')

                        if row_idx == 0:
                            # plot monthly interest data
                            axes_twin[2*row_idx+prop_idx].plot_date(monthly_axis, google_trends['{0}_interest_monthly'.format(gprop_names[prop_idx])],
                                                                    'bo--', mfc='none',
                                                                    label='{0} interest monthly'.format(gprop_names[prop_idx]))
                        else:
                            # plot daily interest data
                            axes_twin[2*row_idx+prop_idx].plot_date(daily_axis, google_trends['{0}_interest'.format(gprop_names[prop_idx])],
                                                                    'r--', label='{0} interest'.format(gprop_names[prop_idx]))

                for ax_idx in [0, 1]:
                    axes[ax_idx].set_title('{0} search'.format(gprop_names[ax_idx]), fontsize=14)
                    axes_twin[ax_idx].tick_params('y', colors='b')
                    plt.setp(axes[ax_idx].get_xticklabels(), visible=False)

                for ax_idx in [2, 3]:
                    axes[ax_idx].xaxis.set_major_locator(mdates.MonthLocator(interval=12))
                    axes[ax_idx].xaxis.set_major_formatter(mdates.DateFormatter('%y-%m'))
                    axes[ax_idx].set_xlabel('calendar date', fontsize=14)
                    axes_twin[ax_idx].tick_params('y', colors='r')

                for ax_idx in [0, 2]:
                    axes[ax_idx].set_ylabel('daily view', fontsize=14)

                for ax_idx in [1, 3]:
                    plt.setp(axes[ax_idx].get_yticklabels(), visible=False)
                axes_twin[1].set_ylabel('monthly interest', fontsize=14, color='b')
                axes_twin[3].set_ylabel('daily interest', fontsize=14, color='r')

                for ax_idx in [0, 1, 2, 3]:
                    axes[ax_idx].yaxis.set_major_formatter(FuncFormatter(
                        lambda y, _: '{0:.0f}M'.format(y / 1000000) if y // 1000000 > 0 else '{0:.0f}K'.format(
                            y / 1000) if y // 1000 > 0 else '{0:.0f}'.format(y)))
                    axes[ax_idx].tick_params(axis='both', which='major', labelsize=12)
                    axes_twin[ax_idx].tick_params(axis='both', which='major', labelsize=12)

                    # axes[ax_idx].spines['right'].set_visible(False)
                    # axes[ax_idx].spines['top'].set_visible(False)

                plt.suptitle('{0}'.format(video_title), fontsize=14)
                plt.tight_layout(rect=[0, 0, 1, 0.96])
                plt.subplots_adjust(wspace=0.1)
                plt.savefig('./images/{0}.pdf'.format(video_title), bbox_inches='tight')
                plt.show()

    # == == == == == == Part 8: Close file handler == == == == == == #
    output_data.close()
