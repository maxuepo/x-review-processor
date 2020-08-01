from __future__ import print_function
from sklearn.feature_extraction.text import TfidfVectorizer
from util import ReviewUtil
import numpy as np
import ntpath
import pandas as pd
import os
from common.base_task import BaseTask
import sys


class ReviewDedupTask(BaseTask):
    def __init__(self, inputdir, threshold=0.9, site='jd'):
        BaseTask.__init__(self)
        self.input_path = inputdir
        self.threshold = threshold
        self.sys_default_reviews = self.default_review_set()
        self.site = site

    def __str__(self):
        return "ReviewDedupTask"

    def default_review_set(self):
        df = pd.read_csv(self.str_params['sys_default_path'], sep='\t', header=None)
        return set(df.loc[:, 0].values)

    def find_similar_reviews(self, df, threshold):
        visited = set()
        output_df_repeated = list()
        all_temp = set()
        if df.empty:
            self.logger.warn('Input dataframe is empty! Returning returning empty dataframe!')
            return pd.DataFrame()
        df_dft, df_input = self.pre_process(df)
        vect = TfidfVectorizer(min_df=1)
        doc_word_split = df_input['content'].apply(lambda t: ReviewUtil.spaced_words(t))
        chn_only_doc = doc_word_split.apply(lambda x: ReviewUtil.chn_char_only(x))
        try:
            tfidf = vect.fit_transform(chn_only_doc)
        except ValueError:
            return pd.DataFrame()
        sim_mat = tfidf * tfidf.T
        ind = 0
        for row in sim_mat:
            index_set = set()
            arry = row.toarray()
            if ind in visited:
                ind += 1
                continue
            (x, y) = np.where(arry > threshold)
            for yy in y:
                if ind >= yy:
                    continue
                visited.add(ind)
                visited.add(yy)
                index_set.add(ind)
                index_set.add(yy)
            all_temp.union(index_set)
            temp = df_input.ix[list(index_set)]
            if len(temp) != 0:
                output_df_repeated.append(temp['id'].values.tolist())
            ind += 1
        output_df_nonrepeated = df_input.loc[~df_input.index.isin(all_temp)]
        return output_df_repeated, output_df_nonrepeated, df_dft

    def run(self):
        file_paths = ReviewUtil.get_all_valid_path(self.input_path)
        for f in file_paths:
            self.logger.info("Review dedup task on: {}".format(str(f)))
            prodid = os.path.splitext(ntpath.basename(str(f)))[0]
            df = ReviewUtil.read_as_pandas(f, self.site, delimiter=',')
            if df.empty:
                continue
            output_path_repeated = ReviewUtil.get_output_path(str(f), prodid, "repeated")
            output_path_nonrepeated = ReviewUtil.get_output_path(str(f), prodid, "nonrepeated")
            output_path_dft = ReviewUtil.get_output_path(str(f), prodid, "dft")
            file_repeated = open(output_path_repeated, "w+")
            file_nonrepeated = open(output_path_nonrepeated, "w+")
            file_default = open(output_path_dft, "w+")
            try:
                output_repeated, ouput_nonrepeated, df_dft = self.find_similar_reviews(df, self.threshold)
            except ValueError:
                continue
            for lst in output_repeated:
                str_temp = "\t".join(str(x) for x in lst)
                file_repeated.write(str_temp)
                file_repeated.write("\n")
            ouput_nonrepeated['id'] = ouput_nonrepeated['id'].astype(int)
            ouput_nonrepeated.to_csv(file_nonrepeated, index=False)
            df_dft.to_csv(file_default, index=False)
            file_default.close()
            file_repeated.close()
            file_nonrepeated.close()

    def pre_process(self, df: pd.DataFrame) -> pd.DataFrame:
        df.loc[:, 'content'] = df['content'].apply(lambda content: content.strip())
        df_dft = df.loc[df['content'].isin(self.sys_default_reviews)]
        df_input = df.loc[~df['content'].isin(self.sys_default_reviews)].reset_index(drop=True)
        return df_dft, df_input
