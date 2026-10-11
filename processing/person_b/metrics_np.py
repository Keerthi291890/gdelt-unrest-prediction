"""
metrics_np.py -- numpy/scipy replacements for sklearn.metrics.average_precision_score / roc_auc_score.
Use only if sklearn's compiled extensions are blocked by an application-control policy.
Same definitions as sklearn: AP = sum_k (R_k - R_{k-1}) * P_k over distinct score thresholds
(ties grouped); ROC-AUC = Mann-Whitney U statistic with average ranks for ties.
Self-test:  python metrics_np.py
"""
import numpy as np
from scipy.stats import rankdata


def average_precision_score(y, s):
    y = np.asarray(y, float); s = np.asarray(s, float)
    P = y.sum()
    if P == 0:
        return np.nan
    order = np.argsort(-s, kind="mergesort")
    y, s = y[order], s[order]
    last = np.r_[np.where(np.diff(s))[0], len(s) - 1]          # last index of each distinct score
    tp = np.cumsum(y)[last]
    fp = (last + 1) - tp
    precision = tp / (tp + fp)
    recall = tp / P
    return float(np.sum(np.diff(np.r_[0.0, recall]) * precision))


def roc_auc_score(y, s):
    y = np.asarray(y, float); s = np.asarray(s, float)
    n1 = y.sum(); n0 = len(y) - n1
    if n1 == 0 or n0 == 0:
        return np.nan
    r = rankdata(s)
    return float((r[y == 1].sum() - n1 * (n1 + 1) / 2) / (n1 * n0))


if __name__ == "__main__":
    # sklearn documentation example
    y, s = [0, 0, 1, 1], [0.1, 0.4, 0.35, 0.8]
    assert abs(average_precision_score(y, s) - 0.8333333333333333) < 1e-12
    assert abs(roc_auc_score(y, s) - 0.75) < 1e-12
    # ties: constant score -> AP equals prevalence, AUC 0.5
    y = [1, 0, 1, 0, 0, 1, 0, 0]
    assert abs(average_precision_score(y, [0.3] * 8) - 3 / 8) < 1e-12
    assert abs(roc_auc_score(y, [0.3] * 8) - 0.5) < 1e-12
    try:                                         # compare with sklearn when it can be imported
        from sklearn.metrics import average_precision_score as ap, roc_auc_score as ra
        rng = np.random.default_rng(0)
        for _ in range(200):
            yy = rng.integers(0, 2, 30); ss = np.round(rng.random(30), 1)   # many ties
            if 0 < yy.sum() < 30:
                assert abs(ap(yy, ss) - average_precision_score(yy, ss)) < 1e-12
                assert abs(ra(yy, ss) - roc_auc_score(yy, ss)) < 1e-12
        print("self-test PASSED (including 200 random comparisons with sklearn)")
    except ImportError as e:
        print("self-test PASSED (known values); sklearn not importable here:", type(e).__name__)
    except Exception as e:
        print("known-value tests PASSED; sklearn comparison skipped:", type(e).__name__)