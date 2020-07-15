#!/usr/bin/env python3
import argparse
from repeated_review_detection import ReviewDedupTask
from template_like_doc_processing import TemplateRemovalTask
from ReviewClassifier import ReviewClassifyer
from report import Report
from base_task import BaseTask


class ZYJ(BaseTask):
    def __init__(self, review_json_input_path, site):
        BaseTask.__init__(self)
        self.site = site
        self.input_path = review_json_input_path

    def run(self):

        template_path = self.str_params['template_path']
        labeled_path = self.str_params['labeled_path']
        self.logger.info("template path: {}".format(template_path))
        self.logger.info("labeled data path: {}".format(labeled_path))
        ReviewDedupTask(self.input_path, site=self.site).run()
        TemplateRemovalTask(template_path, self.input_path, ext=".nonrepeated", site=self.site).run()
        ReviewClassifyer(labeled_path, self.input_path, site=self.site, ext=".rest").run()
        Report(self.input_path, site=self.site).generate_report()
        self.logger.info("ZYJ task completed.")


def main():
    parser = argparse.ArgumentParser(description="analyze the review data")
    parser.add_argument("-i", "--input", help="the folder where data was saved", dest='input')
    parser.add_argument("-s", "--site", help="the site, e.g. jd or taobao, to be analyzed", dest='site', default='jd')
    args = parser.parse_args()
    if args.input is None:
        review_json_input_path = r"../data/product_1315_1343_9717/"
        site = 'jd'
    else:
        review_json_input_path = args.input
        site = args.site
    ZYJ(review_json_input_path, site).run()

if __name__ == '__main__':
    main()
