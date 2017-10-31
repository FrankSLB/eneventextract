# -*- coding: utf-8 -*-

import os

THIS_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(THIS_DIR, u'data')
JARS_DIR = os.path.join(THIS_DIR, u'jars')
CONFIG_DIR = os.path.join(DATA_DIR, u'config')
DICT_DIR = os.path.join(DATA_DIR, u'dictionaries')
CSV_DIR = os.path.join(DATA_DIR, u'csv')

# 人名和国家对应
PERSON_COUNTRY_FILE = os.path.join(DICT_DIR, u'personToCountry.txt')

# 国家别名和国家全称对应，目前每个国家只有一条
COUNTRY_COUNTRY_FILE = os.path.join(DICT_DIR, u'countryToCountry.txt')

# 数据库表country_codes导出的csv文件，主要用于国家代码在标准Alpha2和Alpha3之间的转换
COUNTRY_CODES_FILE = os.path.join(CSV_DIR, u'country_codes.csv')

# 数据库表EVENT_CODE_SCALE_xxx导出的txt文件，映射事件类型得分
EVENT_CODE_SCALE = os.path.join(DICT_DIR, u'EVENT_CODE_SCALE.txt')

# 数据库连接信息
DB_INFO_FILE = os.path.join(CONFIG_DIR, u'DB_info.ini')

# 本程序日志文件
LOG_FILE = os.path.join(THIS_DIR, u'PETRARCH.log')

# Solr连接信息
SOLR_INFO_FILE = os.path.join(CONFIG_DIR, u'Solr_info.ini')

# 多进程处理时的日志所在目录
MULTI_PROCESS_LOG_DIR = os.path.join(THIS_DIR, u'log')

# 多进程处理时的默认日志
MULTI_PROCESS_INFO = os.path.join(MULTI_PROCESS_LOG_DIR, u'multi_process.log')

# 多进程配置文件
MULTI_PROCESS_CONFIG = os.path.join(CONFIG_DIR, u'multi_process.ini')
