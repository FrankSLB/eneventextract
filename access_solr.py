# -*- coding: utf-8 -*-

import sys

import json
import requests
import time

from read_file import read_country_codes, read_solr_address
from write_file import write_multiprocess_log


def query_info_by_solr(q):
    """
    q is used by solr when querying
    """

    solr_address = read_solr_address('geo')
    solr_address = solr_address + '/select?indent=on&q=%s&wt=json'

    result = {}

    try:
        # result = requests.get('http://localhost:8983/solr/geoencode_test/select?indent=on&q=%s&wt=json' % q).text
        result = requests.get(solr_address % q).text
    except requests.exceptions.ConnectionError:
        sys.exit("Solr connection error!")



    # countrycode
    r_countrycode = None
    if result:
        result = json.loads(result)
        r_countrycode = result["response"]["docs"][0]["countrycode"]

    return r_countrycode


def alpha2_to_alpha3(countrycode):
    '''
    translate countrycode from alpha2 to alpha3 standard
    if alpha3 not exist, return the original value
    '''

    country_codes = read_country_codes()
    if countrycode in country_codes:
        countrycode = country_codes[countrycode]

    return countrycode


def read_stories(max_stories_to_read=1):
    solr_address = read_solr_address('news')
    solr_address = solr_address + '/select?fq=language:EN&fq=processed:0&indent=on&q=*:*&rows=%s&start=0&wt=json'%str(max_stories_to_read)
    result = dict()
    try:
        result = requests.get(solr_address).text
    except requests.exceptions.ConnectionError:
        return None

    if result:
        result = json.loads(result)
        stories = result["response"]["docs"]
        return stories
        # if len(stories) >= max_stories_to_read:
        #     return stories[:max_stories_to_read]
        # else:
        #     return stories[:len(stories)]
    else:
        return None


def write_to_solr(id):
    solr_address_tmp = read_solr_address('news')
    solr_address = solr_address_tmp + '/select?q=id:%s&indent=on&wt=json'%id
    #solr_address = solr_address_tmp + '/select?q=id:20171025120921e3dab4b810604f45aef9ba5b80059dc3&indent=on&wt=json'
    data = dict()
    try:
        data = requests.get(solr_address).text
    except requests.exceptions.ConnectionError:
        sys.exit("Solr connection error!")
    if data:
        data = json.loads(data)
        data = data["response"]['docs']
    if data:
        tmp = str(int(time.time() * 1000))
        solr_address = solr_address_tmp + '/update?_=%s&boost=1.0&commitWithin=1000&overwrite=true&wt=json' % tmp
        try:
            site_url = data[0]['siteUrl'].encode('utf-8')
            crawl_time = str(data[0]['crawlTime'])
            publish_date = str(data[0]['publishDate'])
            if 'mainImgUrl' in data[0]:
                main_img_url = ','.join(data[0]['mainImgUrl']).encode('utf-8')
            site_name = data[0]['siteName'].encode('utf-8')
            page_url = data[0]['pageUrl'].encode('utf-8')
            id = data[0]['id'].encode('utf-8')
            title = data[0]['title'].encode('utf-8').replace('"', r'\"')
            poster = data[0]['poster'].encode('utf-8')
            content = data[0]['content'].encode('utf-8').replace('\\', r'').replace('"', r'\"')
            if 'mainImgUrl' in data[0]:
                params = '[{"processed":"1","siteUrl":"%s","crawlTime":"%s","publishDate":"%s","mainImgUrl":"%s","siteName":"%s","pageUrl":"%s","language":"EN","id":"%s","title":"%s","poster":"%s","content":"%s"}]' \
                         % (site_url, crawl_time, publish_date,
                            main_img_url, site_name, page_url, id,
                            title, poster, content)
            else:
                params = '[{"processed":"1","siteUrl":"%s","crawlTime":"%s","publishDate":"%s","siteName":"%s","pageUrl":"%s","language":"EN","id":"%s","title":"%s","poster":"%s","content":"%s"}]' \
                         % (site_url, crawl_time, publish_date,
                            site_name, page_url, id,
                            title, poster, content)
            # params = '[{"processed":"1","siteUrl":"%s","crawlTime":"%s","publishDate":"%s","mainImgUrl":"%s","siteName":"%s","pageUrl":"%s","language":"EN","id":"%s","title":"%s","poster":"%s","content":"%s"}]'\
            #          %(str(data[0]['siteUrl']), str(data[0]['crawlTime']), str(data[0]['publishDate']), data[0]['mainImgUrl'], str(data[0]['siteName']), str(data[0]['pageUrl']), str(data[0]['id']), str(data[0]['title']), str(data[0]['poster']), content)
            header = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36',
                'Content-type': 'application/json'}

            result = requests.post(url=solr_address, data=params, headers=header).text
            result_code = json.loads(result)['responseHeader']['status']
            if str(result_code) == '0':
                return True
            else:
                return False
        except Exception as e:
            print 'write to solr error   id:%s'%str(data[0]['id'])
            return False
    else:
        print 'This id is not in solr!'
        return False


if __name__ == '__main__':
    print read_stories()

