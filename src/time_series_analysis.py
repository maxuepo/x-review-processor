from util import ReviewUtil
import pandas as pd

from datetime import datetime

from collections import Counter
from datetime import datetime
from collections import Counter
import numpy as np
import ntpath
from scipy.stats import norm
from zyj_temporal_analysis import ZYJTemporalAnalysis
import os
input_path = '/home/xuepo/zyj/data/aws-s3/'
files = ReviewUtil.get_all_valid_path(input_path)
df_total = []
for file in files:
    prodid = os.path.basename(str(file)).split(".")[0]
    df = pd.read_csv(file)
    df['referenceTime'] = pd.to_datetime(df["referenceTime"])
    df['creationTime'] = pd.to_datetime(df["creationTime"])
    diff = df['creationTime'] - df['referenceTime']
    df['date_diff'] = diff.apply(lambda x: x.days)
    try:
        dateDiff = df[df['userExpValue'] <= 300]
        count_iterable = Counter(dateDiff['date_diff']).items()
        date_deltas = [date_delta for (date_delta, count) in count_iterable]
        suspicious_delta = set(date_deltas[:3])
        fake_review_list = df[df['date_diff'].isin(suspicious_delta)]['id'].values.tolist()
    except KeyError:
        continue





