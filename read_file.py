# -*- coding: utf-8 -*-

import csv
import io
import sys
try:
    from ConfigParser import ConfigParser
except ImportError:
    from configparser import ConfigParser

from constants import COUNTRY_CODES_FILE, SOLR_INFO_FILE, DB_INFO_FILE


def read_key_value_file(absolute_path, delimiter=u','):
    """read a file, the only two columns in which are comma-separated, to a dict"""

    ret_dict = {}

    with io.open(absolute_path, u'r', encoding=u'utf-8') as f:

        for line in f:

            # skip blank line
            line = line.strip()
            if not line:
                continue

            # skip if line contains '#'
            if line.find(u'#') > -1:
                continue

            # skip if fields number is not 2
            items = line.split(delimiter)
            if len(items) != 2:
                continue

            item0 = items[0].strip()
            item1 = items[1].strip()

            # skip if either element is blank
            if (not item0) or (not item1):
                continue

            ret_dict[item0.upper()] = item1

    return ret_dict


def read_country_codes():
    '''read country codes from csv file'''

    country_codes = {}
    with open(COUNTRY_CODES_FILE, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            alpha2_code = unicode(row[0], 'utf-8')
            alpha3_code = unicode(row[1], 'utf-8')
            country_codes[alpha2_code] = alpha3_code

    return country_codes


def read_solr_address(addressClass = 'news'):
    """read Solr connection address"""

    geo_address = ''
    news_address = ''
    geoSetFlag = 'n'  # y 允许赋值
    newsSetFlag = 'n'
    with open(SOLR_INFO_FILE, 'r') as f:
        for line in f:
            line = line.strip()
            # skip blank line or comment line
            if line == '' or line.startswith('#'):
                continue
            else:
                templine_list = line.strip().split()
                if templine_list[0] == '[GEO]':
                   geoSetFlag = 'y'
                elif templine_list[0] == '[NEWS]':
                   newsSetFlag = 'y'
                elif geoSetFlag == 'y' and len(templine_list) >= 3:
                   geoSetFlag = 'n'
                   geo_address = templine_list[2]
                elif newsSetFlag == 'y'and len(templine_list) >= 3:
                   newsSetFlag = 'n'
                   news_address = templine_list[2]

    if addressClass == 'news':
        return news_address
    elif addressClass == 'geo':
        return geo_address

    return None


def read_db_ini():
    """read database connection information from the default file"""

    # parse the config file
    parser = ConfigParser()
    config_file = parser.read(DB_INFO_FILE)
    if len(config_file) == 0:
        print("Error: Could not find the db config file:",
              config_file)
        print("Terminating program")
        sys.exit()

    # generate the database URI
    db_uri = ""
    try:
        # for oracle database
        if parser.get('Database', 'database').upper() == "Oracle".upper():
            username = parser.get('Oracle', 'username')
            password = parser.get('Oracle', 'password')
            tnsname = parser.get('Oracle', 'tnsname')
            ip = parser.get('Oracle', 'ip')
            port = parser.get('Oracle', 'port')
            sidname = parser.get('Oracle', 'sidname')
            if tnsname != "":
                # 'oracle+cx_oracle://scott:tiger@tnsname'
                db_uri = 'oracle+cx_oracle://{}:{}@{}'.format(username, password, tnsname)
            else:
                # 'oracle+cx_oracle://scott:tiger@127.0.0.1:1521/sidname'
                db_uri = 'oracle+cx_oracle://{}:{}@{}:{}/{}'.format(username, password, ip, port, sidname)
        # for mysql database
        elif parser.get('Database', 'database').upper() == "MySQL".upper():
            pass
    except Exception as e:
        print('parse_config() encountered an error: check the options in',
              r'data\config\DB_info.ini')
        print("Terminating program")
        sys.exit()

    return db_uri


if __name__ == '__main__':
    read_country_codes()
