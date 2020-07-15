from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
from util import ReviewUtil
from sklearn.feature_extraction.text import CountVectorizer
import numpy as np
from base_task import BaseTask


class TopicVerification(BaseTask):
    def __init__(self, input_path, site):
        BaseTask.__init__(self)
        self.REVIEW_CONTENT = 'content'
        self.DESCRIPTION = 'referenceName'
        self.input_path = input_path
        self.site = site
        self.df = ReviewUtil.read_as_pandas(input_path, site=self.site, delimiter=",")
        self.df[['id']] = self.df[['id']].astype(int)
        self.vectorizer = CountVectorizer()
        self.transformer = TfidfTransformer()
        self.vect = TfidfVectorizer(min_df=1)

    def create_word_mat(self):
        reviews = self.df[self.REVIEW_CONTENT]
        description = self.df[self.DESCRIPTION].head(1)
        aggregated_text_list = reviews.append(description)
        doc_word_split = aggregated_text_list.apply(lambda x: ReviewUtil.spaced_words(x))
        tfidf_comments = self.vect.fit_transform(doc_word_split)
        return tfidf_comments

    def is_description_valid(self):
        if self.df.empty:
            return False
        try:
            description = str(self.df[self.DESCRIPTION].values[0])
        except KeyError:
            description = 'nan'
            return False
        if len(description) < 5 or description == 'nan':
            return False
        else:
            return True

    def get_relavent_score(self):
        mat = self.create_word_mat()
        score_mat = mat * mat.T
        score_vec = score_mat[-1, 0:-1]
        return score_vec

    def get_df_with_meta_sim_score(self):
        if self.is_description_valid():
            score_vec = self.get_relavent_score().todense()
            self.df["meta_score"] = score_vec.T
        else:
            self.df["meta_score"] = np.zeros([self.df.shape[0], 1])
        return self.df
