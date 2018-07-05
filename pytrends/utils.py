# -*- coding: utf-8 -*-
"""
Helper functions in pytrends
"""

import numpy as np


def denormalize_interests(history, current, step):
    overlap1 = history[-step:]
    overlap2 = current[:step]
    # print(overlap1)
    # print(overlap2)

    non_zero_idx = np.nonzero(overlap2)
    history_to_current_coef = overlap1[non_zero_idx] / overlap2[non_zero_idx]
    mean_coef = np.mean(history_to_current_coef)

    # print('---'*12)
    # print(history_to_current_coef)
    # print(mean_coef)
    # print('---'*12)

    ret = np.hstack((history[:-step], (overlap1+overlap2*mean_coef)/2, current[step:]*mean_coef))
    # print(len(ret))
    return ret


# a = [  0.,          47.58047499,  23.23697616,  23.23697616,  22.68371482,
#        22.68371482,   0.,          24.34349883,   0.,           0.,
#        11.61848808,   11.61848808,   0.,          11.06522674,  11.61848808,
#        0.,          47.58047499,  23.23697616,  23.23697616,  22.68371482,
#        22.68371482,   0.,          24.34349883,   0.,           0.,
#        11.61848808,   11.61848808,   0.,          11.06522674,  11.61848808]
# b = [ 0, 52, 39, 38, 50,
#       50,  0, 27, 26,  0,
#       52, 25, 25, 25, 52,
#       0, 52, 39, 38, 50,
#       50, 0, 27, 26, 0,
#       52, 25, 25, 25, 52]
# a= np.array(a)
# b = np.array(b)
#
# print(denormalize_interests(a, b, 15))