# -*- coding: utf-8 -*-
"""
Helper functions in pytrends.
"""

import re
from datetime import datetime, timedelta
from collections import Counter
# import matplotlib.pyplot as plt
# import matplotlib.dates as mdates


def reformat(text):
    # remove punctuations and double whitespace
    text = _remove_version(text)
    for ch in ["’s", '"', '!', "*", '.', ':', '‐', '-', "’", "'", '/']:
            text = text.replace(ch, ' ')
    text = text.replace('$', 's')
    for ch in ["+", "&"]:
        if ' {0} '.format(ch) in text:
            text = text.replace(' {0} '.format(ch), ' and ')
        elif ch in text:
            text = text.replace(ch, ' and ')
    return re.sub(' +', ' ', text).strip()


def _remove_version(text):
    """ Remove texts within parentheses and brackets.
    """
    text = text.replace('"', '')
    ret = ''
    cnt = 0
    for ch in text:
        if ch == '(' or ch == '[' or ch == '“':
            if len(ret.strip()) == 0:
                cnt += 1
            else:
                return ret.strip()
        elif ch == ')' or ch == ']' or ch == '”':
            cnt -= 1
        elif cnt == 0:
            ret += ch
    return ret.strip()


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


def diff_month(end_date_obj, start_date_obj):
    return (end_date_obj.year - start_date_obj.year) * 12 + end_date_obj.month - start_date_obj.month


# def plot_interest_over_time(gt_dict):
#     fig = plt.figure(figsize=(8, 3))
#     ax1 = fig.add_subplot(1, 1, 1)
#
#     start_date_obj = datetime.strptime(gt_dict['start_date'], '%Y-%m-%d')
#     end_date_obj = datetime.strptime(gt_dict['end_date'], '%Y-%m-%d')
#
#     num_daily = (end_date_obj - start_date_obj).days + 1
#     daily_axis = [start_date_obj + timedelta(days=di) for di in range(num_daily)]
#
#     ax1.plot_date(daily_axis, gt_dict['daily_search'], 'r-')
#
#     ax1.set_xlabel('calendar date', fontsize=14)
#     ax1.set_ylabel('daily search', fontsize=14)
#
#     # ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=12))
#     ax1.xaxis.set_major_formatter(mdates.DateFormatter('%y-%m'))
#     ax1.tick_params(axis='both', which='major', labelsize=12)
#
#     plt.tight_layout()
#     plt.show()
