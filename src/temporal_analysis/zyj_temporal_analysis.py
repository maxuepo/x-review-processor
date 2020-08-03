from common.util import ReviewUtil
import pandas as pd
from collections import Counter
import numpy as np
from scipy.stats import norm
import os


class ZYJTemporalAnalysis:
    def __init__(self, input_dir: str, threshold: float = 0.9999, num_day_thres: float = 100.):
        self.input_path = input_dir
        self.threshold = threshold
        self.num_day_thres = num_day_thres

    @staticmethod
    def get_risks(df, prod_id):
        if df.empty or df.shape[0] < 100:
            return pd.DataFrame
        review_date = df["creationTime"].apply(lambda x: x.split(" ")[0]).tolist()
        date_freqs = Counter(review_date).items()
        (date, freqs) = zip(*date_freqs)
        np_freqs = np.array(freqs)
        m = np.mean(np_freqs)
        sd = np.std(np_freqs)
        r = (np_freqs - 5 * m) / sd
        prob = norm(0, 1).cdf(r)
        mus = [m] * len(date)
        sds = [sd] * len(date)
        prod_ids = [prod_id] * len(date)
        analysis_df = pd.DataFrame({"date": date, "count": freqs, "prob": prob, "mu": mus, "sd": sds, "prod_id": prod_ids})
        return analysis_df

    @staticmethod
    def is_outlier(points, thresh=3.5):
        if len(points.shape) == 1:
            points = points[:, None]
        mu = np.mean(points, axis=0)
        diff = np.sum((points - mu) ** 2, axis=-1)
        diff = np.sqrt(diff)
        med_abs_deviation = np.mean(diff)
        modified_z_score = (0.6745 * diff + 0.0001) / (med_abs_deviation + 0.0001)
        return modified_z_score > thresh

    @staticmethod
    def get_water_army_reviews2(df):
        df['creationTime'] = pd.to_datetime(df["creationTime"])
        df['referenceTime'] = pd.to_datetime(df["referenceTime"])
        diff = df['referenceTime'] - df['creationTime']
        df['date_diff'] = diff.apply(lambda x: x.days)
        df['ratingDate'] = df['creationTime'].apply(lambda x: x.date())
        try:
            count_iterable = Counter(df['ratingDate'])
            df_counter = pd.DataFrame.from_dict(count_iterable, orient='index').reset_index()
            mask = ZYJTemporalAnalysis.is_outlier(df_counter[0])
            outlier_dates = set(df_counter['index'][mask])
            suspicious_list = df[df['ratingDate'].isin(outlier_dates) & (df['date_diff'] < 30)]['id'].values.tolist()
            if len(suspicious_list) > 20:
                print(suspicious_list)
                return suspicious_list
            else:
                return list()
        except KeyError:
            return list()

    @staticmethod
    def get_water_army_reviews(df):
        num_top_date_delta = 5
        df['referenceTime'] = pd.to_datetime(df["referenceTime"])
        df['creationTime'] = pd.to_datetime(df["creationTime"])
        diff = df['referenceTime'] - df['creationTime']
        df['date_diff'] = diff.apply(lambda x: x.days)
        try:
            date_diff_temp = df
            count_iterable = Counter(date_diff_temp['date_diff']).items()
            date_deltas = [date_delta for (date_delta, count) in count_iterable]
            suspicious_delta = set(date_deltas[:num_top_date_delta])
            suspicious_list = df[df['date_diff'].isin(suspicious_delta)]['id'].values.tolist()
            if len(suspicious_list) > 20:
                return suspicious_list
            else:
                return list()
        except KeyError:
            return list()

    def suspicious(self, df, prodid):
        analysis_df = ZYJTemporalAnalysis.get_risks(df, prodid)
        suspicious = False
        if analysis_df.empty:
            return suspicious, analysis_df
        suspicious_dates = analysis_df.loc[analysis_df['prob'] > self.threshold]
        if (analysis_df.shape[0] > self.num_day_thres) & (suspicious_dates.shape[0] > 0):
            suspicious = True
        return suspicious, suspicious_dates

    def get_temporal_analysis(self):
        files = ReviewUtil.get_all_valid_path(self.input_path)
        df_total = []
        for file in files:
            prodid = os.path.basename(str(file)).split(".")[0]
            df_raw = pd.read_csv(file)
            if df_raw.empty:
                continue
            flag, analysis_df = self.suspicious(df_raw, prodid)
            if not analysis_df.empty:
                df_total.append(analysis_df)
        df_all = pd.concat(df_total)
        df_all.to_csv('/home/xuepo/dump.csv')
