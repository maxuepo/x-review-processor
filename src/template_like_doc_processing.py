from __future__ import print_function
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
from util import ReviewUtil
from sklearn.feature_extraction.text import CountVectorizer
import os
import pandas as pd
from base_task import BaseTask


class TemplateRemovalTask(BaseTask):
    def __init__(self, template_path, input_path, site='jd', threshold=0.7, ext=".csv"):
        BaseTask.__init__(self)
        self.tempplate_path = template_path
        self.input_path = input_path
        self.ext = ext
        self.df_fake_reviews = pd.read_csv(self.tempplate_path, sep=",")
        self.df_fake_reviews[['id']] = self.df_fake_reviews[['id']].astype(int)
        self.threshold = threshold
        self.site = site

        self.vectorizer = CountVectorizer()
        self.transformer = TfidfTransformer()
        self.vect = TfidfVectorizer(min_df=1)
        self.doc_word_split = self.df_fake_reviews['review'].apply(lambda x: ReviewUtil.spaced_words(x))
        self.tfidf = self.vect.fit_transform(self.doc_word_split)

    def filter_out_template_like(self, df_input):
        if df_input.empty:
            return pd.DataFrame
        df_input["fake_score_template"] = df_input["content"].apply(lambda x: self.get_template_sim_score(x))
        df_fake = df_input[df_input['fake_score_template'] > self.threshold]
        df_rest = df_input[df_input['fake_score_template'] <= self.threshold]
        return df_fake, df_rest

    def get_template_sim_score(self, input_str):
        processed_str = ReviewUtil.spaced_words(input_str)
        input_tfidf = self.vect.transform([processed_str])
        similarity = input_tfidf * self.tfidf.T
        template_sim = (max(similarity.toarray()[0, ]))
        return template_sim

    def batch_process(self):
        input_paths = ReviewUtil.get_all_valid_path(self.input_path, ext=self.ext)
        for f in input_paths:
            self.logger.info("processing {}".format(f))
            df = ReviewUtil.read_as_pandas(f, self.site, ",")
            if df.empty:
                continue
            filename = os.path.basename(str(f)).split(".")[0]
            (fake, rest) = self.filter_out_template_like(df)
            rest_filtered = rest.loc[rest.id.notnull()]
            outputpath_fake = ReviewUtil.get_output_path(str(f), filename, "fake")
            outputpath_rest = ReviewUtil.get_output_path(str(f), filename, "rest")
            output_file_fake = open(outputpath_fake, "w+")
            output_file_rest = open(outputpath_rest, "w+")
            fake.to_csv(output_file_fake, index=False)
            rest_filtered.to_csv(output_file_rest, index=False)
            output_file_fake.close()
            output_file_rest.close()

    def run(self):
        self.batch_process()
