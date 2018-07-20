# -*- coding: utf-8 -*-
"""
Helper functions in pytrends.
"""

from datetime import datetime, timedelta
from collections import Counter
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

mpl.rcParams['lines.linewidth'] = 1


def calendar_days(start_date, end_date):
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')

    num_days = (end_date_obj - start_date_obj).days + 1
    full_days_obj = [start_date_obj + timedelta(days=di) for di in range(num_days)]
    full_days_str = [datetime.strftime(x, '%Y-%m') for x in full_days_obj]

    ret = []
    c = Counter(full_days_str)
    for k in sorted(c.keys()):
        ret.append(c[k])
    return ret


def diff_month(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month + 1


def plot_interest_over_time(gt_dict):
    fig = plt.figure(figsize=(8, 3))
    ax1 = fig.add_subplot(1, 1, 1)

    release_date_obj = datetime.strptime(gt_dict['release_date'], '%Y-%m-%d')
    end_date_obj = datetime.strptime(gt_dict['end_date'], '%Y-%m-%d')

    num_daily = (end_date_obj - release_date_obj).days + 1
    daily_axis = [release_date_obj + timedelta(days=di) for di in range(num_daily)]

    ax1.plot_date(daily_axis, gt_dict['youtube_interest'][-num_daily:], 'r-')

    ax1.set_xlabel('calendar date', fontsize=14)
    ax1.set_ylabel('YouTube interest', fontsize=14)

    # ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=12))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%y-%m'))
    ax1.tick_params(axis='both', which='major', labelsize=12)

    plt.tight_layout()
    # plt.savefig('./images/{0}.pdf'.format(video_title), bbox_inches='tight')
    plt.show()