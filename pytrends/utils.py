# -*- coding: utf-8 -*-
"""
Helper functions in pytrends
"""

import numpy as np


def denormalize_interests(history, current, step):
    overlap1 = history[-step:]
    overlap2 = current[:step]

    history_to_current_coef = overlap1 / overlap2
    mean_coef = np.mean(history_to_current_coef[np.argwhere(np.isnan(history_to_current_coef, where=False))])
    print(mean_coef)

    ret = np.hstack((history, current[step:]*mean_coef))

    if np.isnan(ret).any():
        print('error!!!!!!!!!!!nan')
        print('debug!!!')
        print(overlap1)
        print(overlap2)

    return ret
