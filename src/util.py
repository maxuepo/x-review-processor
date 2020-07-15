from __future__ import print_function
import re
from pathlib import Path
import jieba
import sys
import pandas as pd
from pandas.parser import CParserError
from pandas.errors import EmptyDataError
import os
from hashlib import md5
from hashlib import sha1
from hashlib import sha224
from hashlib import sha384
from hashlib import sha512
import numpy as np
from datetime import datetime
import ast


class ReviewUtil:
    @staticmethod
    def get_list_of_files(input_path):
        in_path = Path(input_path)
        try:
            abs_path = in_path.resolve()
        except FileNotFoundError:
            sys.exit()
        files = []
        if abs_path.is_dir():
            dirs = [x for x in abs_path.iterdir() if x.is_dir()]
            file_sub_depth1 = [y for y in abs_path.iterdir() if y.is_file()]
            files.extend(file_sub_depth1)
            for directory in dirs:
                files_sub = [z for z in directory.iterdir() if z.is_file()]
                files.extend(files_sub)
        else:
            files.append(abs_path)
        return files

    @staticmethod
    def read_as_pandas(file_path, site=None, delimiter="\t"):
        if site == 'jd':
            return ReviewUtil.read_jd_data(file_path, delimiter)
        elif site == 'tb':
            return ReviewUtil.read_tmall_data(file_path, delimiter)
        else:
            return ReviewUtil.read_jd_data(file_path, delimiter)

    @staticmethod
    def read_data(file_path, delimiter):
        try:
            df_raw = pd.read_csv(file_path, index_col=None, sep=delimiter)
        except CParserError:
            df_raw = pd.DataFrame()
        except EmptyDataError:
            df_raw = pd.DataFrame()
        return df_raw

    @staticmethod
    def read_jd_data(file_path, delimiter):
        try:
            df_raw = pd.read_csv(file_path, index_col=None, sep=delimiter)
            df_raw['content'] = df_raw['content'].astype(str)
            df_raw['id'] = df_raw['id'].astype(str)
            df_raw = df_raw[(df_raw['id'] != 'False') & (df_raw['id'] != 'nan')]
            df_raw.reset_index(drop=True, inplace=True)
            df_raw['id'] = df_raw['id'].astype(float)
            df_raw['id'] = df_raw['id'].astype(int)
        except CParserError:
            df_raw = pd.DataFrame()
        except EmptyDataError:
            df_raw = pd.DataFrame()
        return df_raw

    @staticmethod
    def read_tmall_data(file_path, delimiter):
        try:
            df_raw = pd.read_csv(file_path, index_col=None, sep=delimiter)
            df_raw['content'] = df_raw['rateContent']
            m, n = df_raw.shape
            df_raw['score'] = np.ones(m)
            df_raw['referenceTime'] = df_raw['rateDate']
            df_raw['creationTime'] = df_raw['gmtCreateTime'].apply(lambda x: ReviewUtil.convert_timestamp_to_date(x))
            df_raw['content'] = df_raw['content'].astype(str)
            df_raw['id'] = df_raw['id'].astype(str)
            df_raw = df_raw[(df_raw['id'] != 'False') & (df_raw['id'] != 'nan')]
            df_raw.reset_index(drop=True, inplace=True)
            df_raw['id'] = df_raw['id'].astype(float)
            df_raw['id'] = df_raw['id'].astype(int)
        except CParserError:
            df_raw = pd.DataFrame()
        except EmptyDataError:
            df_raw = pd.DataFrame()
        return df_raw

    @staticmethod
    def convert_timestamp_to_date(input_str):
        input_dict = ast.literal_eval(input_str)
        timestamp = int(input_dict['time']) / 1000
        date_str = str(datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S'))
        return date_str

    @staticmethod
    def chn_char_only(input_str):
        pttn = "[A-Za-z0-9\[\`\~\@\#\$\^\&\*\(\)\=\|\{\}\'\:\;\，。∶'\,\[\]\.\<\>\/\?\~\@\#\\\&\*\%]!"
        try:
            str_out = re.sub(pttn, " ", input_str).strip()
        except TypeError:
            str_out = ""
        return str_out

    @staticmethod
    def spaced_words(input_str):
        char_only = ReviewUtil.chn_char_only(input_str)
        doc_word_split = " ".join(jieba.cut(char_only, cut_all=False))
        return doc_word_split

    @staticmethod
    def is_file_valid(f, ext=".csv"):
        file_name_only = f.parts[-1]
        if file_name_only == ext:
            return False
        if file_name_only.endswith(ext):
            return True
        return False

    @staticmethod
    def get_all_valid_path(input_path, ext=".csv"):
        files = ReviewUtil.get_list_of_files(input_path)
        valid_file_paths = []
        for p in files:
            if ReviewUtil.is_file_valid(p, ext):
                valid_file_paths.append(p)
        return valid_file_paths

    @staticmethod
    def get_output_path(input_path, filename, extension=".txt"):
        ouput_dir = os.path.dirname(input_path)
        output_path = os.path.join(ouput_dir, filename + "." + extension)
        return output_path

    @staticmethod
    def get_list_of_dir(input_path):
            in_path = Path(input_path)
            try:
                abs_path = in_path.resolve()
            except FileNotFoundError:
                sys.exit()

            files = []
            if abs_path.is_dir():
                files.append(abs_path)
                dirs = [x for x in abs_path.iterdir() if x.is_dir()]
                files.extend(dirs)
            return files

    @staticmethod
    def hashForString(method, srcbyte):
        srcbyte = srcbyte.encode("gb2312")
        if method == 'md5':
            m = md5()
            m.update(srcbyte)
            srcbyte = m.hexdigest()
        elif method == 'sha1':
            s = sha1()
            s.update(srcbyte)
            srcbyte = int(s.hexdigest()[:8], 16)
        elif method == 'sha224':
            s = sha224()
            s.update(srcbyte)
            srcbyte = s.hexdigest()
        elif method == 'sha384':
            s = sha384()
            s.update(srcbyte)
            srcbyte = s.hexdigest()
        elif method == 'sha512':
            s = sha512()
            s.update(srcbyte)
            srcbyte = s.hexdigest()
        return srcbyte


