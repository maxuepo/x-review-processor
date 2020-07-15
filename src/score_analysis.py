
from __future__ import print_function
from util import ReviewUtil
import pandas as pd
df = ReviewUtil.read_as_pandas("resource/valid_review_dist.tsv", "\t")

df2_good_vs_bad = pd.read_csv('resource/invalid_valid_dist.tsv', "\t")



n_valid = df['valid_cnt'].sum()
n_total = df['total_cnt'].sum()


df_filtered = df.loc[df['total_cnt'] > 50]
df_filtered.reset_index(drop=True)

df_filtered['valid_percentage'] = (df_filtered['valid_cnt'] + 1) / (df_filtered['total_cnt'] + 1)


df_summary = df_filtered.describe()


beta = df_summary.loc['mean', 'valid_percentage'] / (df_summary.loc['std', 'valid_percentage'] ** 2)
alpha = ( df_summary.loc['mean', 'valid_percentage'] ** 2) / (df_summary.loc['std', 'valid_percentage'] ** 2)

print(alpha)
print(beta)
print(df_summary)

# mean = 0.039626
# std = 0.032359


# analysis 2


df2_filtered = df2_good_vs_bad.loc[(df2_good_vs_bad['num_negative'] != 0) & (df2_good_vs_bad['num_positive'] != 0)]

df2_filtered.reset_index(drop=True)

df2_filtered.loc[:, 'total'] = df2_filtered['num_negative'].values + df2_filtered['num_positive'].values

df2_filtered.loc[:, 'percentage'] = (df2_filtered['num_positive'] + 1) / (df2_filtered['total'] + 1)

df2_summary = df2_filtered.describe()

print(df2_summary)


beta2 = 0.772907 / ( 0.104540 ** 2)
alpha2 = (0.772907 ** 2) / ( 0.104540 ** 2)

print(alpha2)
print(beta2)