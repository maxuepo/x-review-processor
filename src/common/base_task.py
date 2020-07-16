import configparser
import logging
from logging.config import fileConfig
import pandas as pd
from pandas.errors import EmptyDataError


class BaseTask:
    numerical_params = {}
    list_params = {}
    str_params = {}

    def __init__(self):
        self.config_path = '../../resource/configs/parameters.ini'
        self.config = configparser.ConfigParser()
        self.config.read(self.config_path)
        for entry in self.config['NUMERICAL']:
            self.numerical_params[entry] = float(self.config['NUMERICAL'][entry])
        for entry in self.config['LIST']:
            lst = self.config['LIST'][entry].split(',')
            self.list_params[entry] = lst
        for entry in self.config['STRING']:
            self.str_params[entry] = self.config['STRING'][entry]
        fileConfig('../../resource/configs/logging.ini')
        self.logger = logging.getLogger()

    def read_csv(self, path, delimitter="\t"):
        try:
            df = pd.read_csv(path, sep=delimitter)
            df[['content', 'referenceName']] = df[['content', 'referenceName']].astype(str)
            df[['id']] = df[['id']].astype(int)
            df[['score']] = df[['score']].astype(float)
        except EmptyDataError:
            df = pd.DataFrame
        return df
