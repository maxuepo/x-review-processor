from __future__ import print_function
from common.util import ReviewUtil
import os
import json
from datetime import datetime
import pandas as pd
from base_task import BaseTask
from temporal_analysis.zyj_temporal_analysis import ZYJTemporalAnalysis



class Report(BaseTask):

    def __init__(self, input_dir, site='jd'):
        BaseTask.__init__(self)
        self.input_dir = input_dir
        self.datetime_format = '%Y-%d-%m %H:%M:%S'
        self.RELIABILITY_ALPHA = self.numerical_params['reliability_alpha']
        self.RELIABILITY_BETA = self.numerical_params['reliability_beta']
        self.GOOD_RATING_ALPHA = self.numerical_params['good_rating_alpha']
        self.GOOD_RATING_BETA = self.numerical_params['good_rating_beta']
        self.MU_VALID_PERCENTAGE = self.numerical_params['mu_valid_percentage']
        self.SIGMA_VALID_PERCENTAGE = self.numerical_params['sigma_valid_percentage']
        self.MU_VALID_RATE = self.numerical_params['mu_valid_rate']
        self.SIGMA_VALID_RATE = self.numerical_params['sigma_valid_rate']
        self.TEMP_FILE_EXTENSIONS = self.list_params['temp_file_extensions']
        self.VERSION = self.str_params['version']
        self.site = site
        self.logger.info('All tasks done start to build report.')

    def get_valid_review_info(self, file):
        df_prediction = ReviewUtil.read_as_pandas(file, site=self.site, delimiter=",")
        df_prediction['id'] = df_prediction['id'].astype(int)
        report_list_pos = []
        report_list_neg = []
        weighted_score = 0
        if not df_prediction.empty:
            weighted_score = sum(df_prediction['score'].values) / df_prediction.shape[0]
            df_valid_pos = df_prediction[(df_prediction['score'] >= 4)]
            df_valid_neg = df_prediction[(df_prediction['score'] <= 3)]
            for index, row in df_valid_pos.iterrows():
                report_dict_pos = dict()
                report_dict_pos["review_id"] = row['id']
                report_dict_pos['score'] = row['prob']
                report_list_pos.append(report_dict_pos)
            for index, row in df_valid_neg.iterrows():
                report_dict_neg = dict()
                report_dict_neg["review_id"] = row['id']
                report_dict_neg['score'] = row['prob']
                report_list_neg.append(report_dict_neg)
        return report_list_pos, report_list_neg, df_prediction, weighted_score

    @staticmethod
    def get_invalid_review_info(file, site):
        df_prediction = ReviewUtil.read_as_pandas(file, site=site, delimiter=",")
        report_list = []
        if not df_prediction.empty:
            for index, row in df_prediction.iterrows():
                report_dict = dict()
                report_dict["review_id"] = row['id']
                report_dict['score'] = row['prob']
                report_list.append(report_dict)
        return report_list

    @staticmethod
    def get_summary(df):
        df['id'] = df['id'].astype(int)
        suspicious_reviews = ZYJTemporalAnalysis.get_water_army_reviews2(df)
        total_cnt = df.shape[0]
        suspicious_percentage = 0.0
        if total_cnt != 0:
            suspicious_percentage = len(suspicious_reviews) / total_cnt
        suspicious_entry = {"count": len(suspicious_reviews), "reviews": suspicious_reviews, "suspicious_percentage": suspicious_percentage}
        summary = dict()
        suspicious_reviews_set = set(suspicious_reviews)
        reliable_entries = df[~df['id'].isin(suspicious_reviews_set)]
        reliable_scores = reliable_entries['score'].values.tolist()
        if len(reliable_entries) == 0:
            summary['weighted_score'] = 3
        else:
            summary['weighted_score'] = sum(reliable_scores) / (len(reliable_scores))
        summary['suspicious_reviews'] = suspicious_entry
        summary["total_number_of_reviews"] = 0
        summary["total_number_of_reviews"] = df.shape[0]
        create_datetime = df['referenceTime']
        min_datetime = min(create_datetime)
        max_datetime = max(create_datetime)
        summary['comment_start'] = str(min_datetime)
        summary['comment_end'] = str(max_datetime)
        num_of_days = (max_datetime - min_datetime).days + 1
        summary['avg_daily_review_number'] = summary["total_number_of_reviews"] / num_of_days
        return summary

    def get_report_dict(self, dir):
        json_dict = dict()
        valid_neg = []
        valid_pos = []
        invalid = []
        template_id = []
        repeated_id = []
        sys_dft_ids = []
        summary = dict()
        total_score = 0
        files = [y for y in dir.iterdir() if y.is_file()]
        for f in files:
            file_name_only = f.parts[-1]
            repeated_total_cnt = 0
            if file_name_only.endswith(".repeated"):
                with open(str(f)) as f_opened:
                    for line in f_opened.read().splitlines():
                        temp = list(map(int, line.split("\t")))
                        repeated_total_cnt += len(temp)
                        repeated_id.append(temp)

            if file_name_only.endswith(".fake"):
                df_temp = pd.read_csv(f, sep=",")
                df_temp['id'] = df_temp['id'].astype(int)
                if not df_temp.empty:
                    template_id = df_temp["id"].values.tolist()

            if file_name_only.endswith(".dft"):
                df_dft = ReviewUtil.read_data(f, ",")
                if not df_dft.empty:
                    sys_dft_ids = df_dft["id"].values.tolist()

            if file_name_only.endswith(".csv"):
                df_temp = ReviewUtil.read_as_pandas(f, site=self.site, delimiter=",")
                if not df_temp.empty:
                    summary = Report.get_summary(df_temp)

            if file_name_only.endswith(".valid"):
                (pos_dict, neg_dict, valid_df, weighted_score) = self.get_valid_review_info(f)
                valid_pos.extend(pos_dict)
                valid_neg.extend(neg_dict)
                if not valid_df.empty:
                    total_score = valid_df['score'].sum()

            if file_name_only.endswith(".review_invalid"):
                invalid_dict = Report.get_invalid_review_info(f, site=self.site)
                invalid.extend(invalid_dict)

        if len(summary) == 0:
            return json_dict
        direcotry_base = os.path.basename(str(dir))
        json_dict['product_id'] = direcotry_base
        json_dict['valid_positive_review_ids'] = valid_pos
        json_dict['valid_negative_review_ids'] = valid_neg
        json_dict['invalid_review_ids'] = invalid
        json_dict['review_similar_to_template'] = template_id
        json_dict['analyzed_score'] = (total_score + summary["total_number_of_reviews"] * 2 + 1) / (summary["total_number_of_reviews"] + 1)
        json_dict['weighted_score'] = summary['weighted_score']
        json_dict['comments_after_filter'] = len(valid_pos) + len(valid_neg)
        json_dict['repeated_review_ids'] = repeated_id

        # Temporally unavailable fields
        # TODO add some mock results.
        ai_summary_list = []
        ai_summary_dict = dict()
        percent_repeated = repeated_total_cnt / summary["total_number_of_reviews"] + 0.000000000001
        percent_template = len(template_id) / summary["total_number_of_reviews"] + 0.000000000001
        if percent_repeated > 0.1:
            ai_summary_dict['type'] = 12
            ai_summary_dict["values"] = [summary["total_number_of_reviews"], repeated_total_cnt]
            ai_summary_list.append(ai_summary_dict)

        ai_summary_dict = dict()
        if percent_template > 0.2:
            ai_summary_dict["type"] = 11
            ai_summary_dict["values"] = [summary["total_number_of_reviews"], len(template_id)]
            ai_summary_list.append(ai_summary_dict)

        percent_valid = (len(valid_pos) + len(valid_neg)) / summary["total_number_of_reviews"] + 0.000000000001
        ai_summary_dict = dict()
        if percent_valid < 0.1:
            ai_summary_dict["type"] = 10
            ai_summary_dict["values"] = [summary["total_number_of_reviews"], (len(valid_pos) + len(valid_neg))]
            ai_summary_list.append(ai_summary_dict)
        json_dict['valid_count'] = len(valid_pos) + len(valid_neg)
        json_dict['total_num_of_reviews'] = summary["total_number_of_reviews"]
        json_dict['review_reliability'] = self.reliability_score(json_dict)
        currentDT = datetime.utcnow()
        json_dict['ai_version'] = self.VERSION
        json_dict['analysis_time'] = str(currentDT)
        json_dict['num_sim_to_template'] = len(template_id)
        json_dict['total_num_repeated'] = repeated_total_cnt
        json_dict['total_valid_reviews'] = (len(valid_pos) + len(valid_neg))
        json_dict['num_positive'] = len(valid_pos)
        json_dict['num_negative'] = len(valid_neg)
        json_dict['total_num_of_reviews'] = summary["total_number_of_reviews"]
        json_dict['ai_summary'] = ai_summary_list
        json_dict['zyj_review_score'] = self.zyj_review_score(json_dict)
        json_dict['sys_default_reviews'] = sys_dft_ids
        json_dict.update(summary)
        self.logger.info(json_dict)
        return json_dict

    def reliability_score(self, input_json_dict):
        posterior_total_cnt = input_json_dict['total_num_of_reviews'] + self.RELIABILITY_BETA
        posterior_valid_cnt = input_json_dict['valid_count'] + self.RELIABILITY_ALPHA
        percentage = posterior_valid_cnt / posterior_total_cnt
        review_reliability = 5 * (percentage - self.MU_VALID_PERCENTAGE) / \
                             self.SIGMA_VALID_PERCENTAGE + 3.0
        return review_reliability

    def zyj_review_score(self, input_json_dict):
        num_p = input_json_dict['num_positive']
        num_n = input_json_dict['num_negative']
        num_t = num_n + num_p
        percent = (num_p + self.GOOD_RATING_ALPHA) / (num_t + self.GOOD_RATING_BETA)
        product_revew = 5 * (percent - self.MU_VALID_RATE) / self.SIGMA_VALID_RATE + 3.0
        return product_revew

    def generate_report(self):
        similar_review_path = ReviewUtil.get_list_of_dir(self.input_dir)
        valid_count_list = []
        total_count_list = []
        num_positive = []
        num_negative = []
        zyj_scores = []
        review_reliabilities = []
        url_list = []
        for dir in similar_review_path:
            outputpath_json = os.path.join(str(dir), "analysis.json")
            json_dict = self.get_report_dict(dir)
            outputfile = open(outputpath_json, "w+")
            json.dump(json_dict, outputfile, ensure_ascii=False, indent=2)
            if len(json_dict) == 0:
                continue
            valid_count_list.append(json_dict['valid_count'])
            total_count_list.append(json_dict['total_number_of_reviews'])
            num_positive.append(json_dict['num_positive'])
            num_negative.append(json_dict['num_negative'])
            zyj_scores.append(json_dict['zyj_review_score'])
            review_reliabilities.append(json_dict['review_reliability'])
            pid = json_dict['product_id']
            url_list.append("http://item.jd.com/{}.html".format(pid))
        review_scores_df = pd.DataFrame({'zyj_score': zyj_scores,
                                         'review_reliability': review_reliabilities,
                                         'url': url_list, 'valid_cnt': valid_count_list,
                                         'total_cnt': total_count_list,
                                         'num_positive': num_positive,
                                         'num_negative': num_negative})

        outputfile = os.path.join(str(self.input_dir), "valid_review_dist.tsv")
        outputfile2 = os.path.join(str(self.input_dir), 'invalid_valid_dist.tsv')
        outputfile_debug = os.path.join(self.input_dir, "review_score_debug.tsv")
        debug_summary = os.path.join(self.input_dir, "debug_summary.tsv")
        valid_cnt_dist = pd.DataFrame({'valid_cnt': valid_count_list, 'total_cnt': total_count_list})
        valid_vs_invalid_dist = pd.DataFrame({'num_positive': num_positive, 'num_negative': num_negative})
        valid_cnt_dist.to_csv(outputfile, sep='\t', index=False)
        review_scores_df.to_csv(debug_summary, sep='\t', index=False)
        valid_vs_invalid_dist.to_csv(outputfile2, sep='\t', index=False)
        review_scores_df.to_csv(outputfile_debug, sep='\t', index=False)
        self.logger.info("Report generation complete. Cleaning up.")
        try:
            ReviewUtil.clean(self.TEMP_FILE_EXTENSIONS, similar_review_path)
        except TypeError:
            self.logger.error("Cannot remove {}".format(similar_review_path))
        self.logger.info("Clean up complete.")
