from __future__ import print_function
from sklearn.feature_extraction.text import TfidfVectorizer
from common.util import ReviewUtil
from sklearn.naive_bayes import MultinomialNB
import pandas as pd
import os
from pathlib import Path
import _pickle as cPickle
from topic.topicscore import TopicVerification
from common.base_task import BaseTask


class ReviewClassifyer(BaseTask):
    def __init__(self,
                 labeled_data_dir: str,
                 input_path: str,
                 site: str,
                 ext: str = '.csv'):
        BaseTask.__init__(self)
        self.labeled_data_dir = labeled_data_dir
        self.input_path = input_path
        self.ext = ext
        self.prior = [0.8, 0.2]
        self.alpha_value = 0.2
        self.site = site
        self.model_path = self.str_params['model_path']
        self.vect_transformer_path = self.str_params['vect_transformer_path']
        model = Path(self.model_path)

        if model.is_file():
            self.logger.info("load existing models")
            with open(self.model_path, 'rb') as fid:
                self.clf = cPickle.load(fid)
            with open(self.vect_transformer_path, 'rb') as fid:
                self.vect = cPickle.load(fid)
        else:
            self.logger.info("models do not exist, building...")
            self.vect = TfidfVectorizer(min_df=1)
            labeled_dataframe = self.get_labeled_data()
            doc_word_split = labeled_dataframe['review'].apply(lambda x: ReviewUtil.spaced_words(x))
            self.tfidf = self.vect.fit_transform(doc_word_split)
            self.clf = MultinomialNB(alpha=self.alpha_value, fit_prior=True, class_prior=self.prior) \
                .fit(self.tfidf, labeled_dataframe[["isValid"]])
            with open(self.model_path, 'wb') as fid:
                cPickle.dump(self.clf, fid)
            with open(self.vect_transformer_path, 'wb') as fid:
                cPickle.dump(self.vect, fid)

    def get_labeled_data(self):
        file_list = ReviewUtil.get_list_of_files(self.labeled_data_dir)
        list_ = []
        for file_ in file_list:
            if str(file_).endswith("csv"):
                labeled_df = pd.read_csv(file_, index_col=None, header=0)
                list_.append(labeled_df)
        frame = pd.concat(list_)
        return frame

    @staticmethod
    def post_process(input_df):
        input_df['char_only'] = input_df['content'].apply(lambda x: ReviewUtil.chn_char_only(x))
        input_df['review_len'] = input_df['char_only'].apply(lambda x: len(x))
        df_processed = input_df[input_df["review_len"] > 5]
        return df_processed

    def predict_class(self, df):
        try:
            doc_spaced = df['content'].apply(lambda x: ReviewUtil.spaced_words(x))
        except KeyError:
            return pd.DataFrame
        doc_vect_transformed = self.vect.transform(doc_spaced)
        doc_label = self.clf.predict(doc_vect_transformed)
        doc_prob = self.clf.predict_proba(doc_vect_transformed)
        df["label"] = doc_label
        df["prob"] = doc_prob[:, 1]
        outdf_valid = df[(df.label == 1)].sort_values(by=['prob'], ascending=False)
        outdf_invalid = df[(df.label != 1)].sort_values(by=['prob'])
        return outdf_valid, outdf_invalid

    def run(self):
        files = ReviewUtil.get_all_valid_path(self.input_path, self.ext)
        for file in files:
            filename = os.path.basename(str(file)).split(".")[0]
            outputpath_valid = ReviewUtil.get_output_path(str(file), filename, "valid")
            outputpath_invalid = ReviewUtil.get_output_path(str(file), filename, "review_invalid")
            try:
                df = TopicVerification(file, self.site).get_df_with_meta_sim_score()
            except ValueError:
                df = pd.DataFrame
            if df.empty:
                continue
            (df_nb, df_invalid) = self.predict_class(df)
            df_valid_new = ReviewClassifyer.reconstruct_valid(df_nb)
            df_invalid_new = ReviewClassifyer.reconstruct_invalid(df_invalid)
            if ~df_valid_new.empty:
                self.logger.info(outputpath_valid)
                output_file_valid = open(outputpath_valid, "w+")
                df_valid_new.to_csv(output_file_valid, header=True, index=False)
                output_file_valid.close()
            if ~df_invalid_new.empty:
                self.logger.info(outputpath_invalid)
                output_file_invalid = open(outputpath_invalid, "w+")
                df_invalid_new.to_csv(output_file_invalid, header=True, index=False)
                output_file_invalid.close()

    @staticmethod
    def reconstruct_valid(df_invalid):
        nonrelavent = df_invalid[df_invalid['meta_score'] == 0].reset_index(drop=True)
        relavent = df_invalid[df_invalid['meta_score'] > 0].reset_index(drop=True)
        relavent.loc[:, 'prob'] = relavent["prob"] + 0.2
        if nonrelavent.shape[0] != 0:
            relavent = relavent.append(nonrelavent)
        return relavent

    @staticmethod
    def reconstruct_invalid(df_valid):
        nonrelavent = df_valid[df_valid['meta_score'] == 0].reset_index(drop=True)
        relavent = df_valid[df_valid['meta_score'] > 0].reset_index(drop=True)
        relavent.loc[:, 'prob'] = relavent["prob"] + 0.5
        if relavent.shape[0] != 0:
            nonrelavent = nonrelavent.append(relavent)
        return nonrelavent

