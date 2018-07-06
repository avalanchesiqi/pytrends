# -*- coding: utf-8 -*-
"""
Helper functions in pytrends.
"""

import numpy as np


def denormalize_interests(history, current, step):
    overlap1 = history[-step:]
    overlap2 = current[:step]

    non_zero_idx = np.nonzero(overlap2)
    history_to_current_coef = overlap1[non_zero_idx] / overlap2[non_zero_idx]
    mean_coef = np.mean(history_to_current_coef)

    ret = np.hstack((history[:-step], (overlap1+overlap2*mean_coef)/2, current[step:]*mean_coef))
    return ret
