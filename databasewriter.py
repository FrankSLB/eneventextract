# -*- coding: utf-8 -*-

import os
import time
import logging
import requests
import json
from datetime import datetime
try:
    from ConfigParser import ConfigParser
except ImportError:
    from configparser import ConfigParser

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import constants
import PETRglobals
import GetInfo
import ParseRoleCode
from read_file import read_solr_address, read_key_value_file, read_db_ini
from access_solr import query_info_by_solr, alpha2_to_alpha3
from write_file import write_multiprocess_log
from access_solr import write_to_solr
from session_factory import PetrarchEvents2

os.environ['NLS_LANG'] = "SIMPLIFIED CHINESE_CHINA.UTF8"


def do_write_db(obj_list):
    """write objects to database"""

    logger = logging.getLogger('petr_log')

    # database connection string
    con_str = read_db_ini()
    
    # Lazy Connecting
    engine = create_engine(con_str)
    
    session = None
    flag = True
    try:
        # Create a Schema. If the relevant table doesn't exist in database, it will be created automatically.
        # Base.metadata.create_all(engine)
        
        # Creating a Session
        session_maker = sessionmaker(bind=engine)
        session = session_maker()
        
        # Adding and Updating Objects
        session.add_all(obj_list)
        
        # Commit the transaction
        session.commit()
    except Exception as e:
        print('{}'.format(e).decode("utf-8", errors="ignore"))
        logger.warning('{}'.format(e).decode("utf-8", errors="replace"))
        flag = False
    finally:
        # Close the Session
        if session:
            session.close()

    return flag


def do_write_db_multiprocess(session, obj_list, multi_log_lock):
    """write objects to database"""

    process_id = os.getpid()

    error_msg_ignore = ""
    error_msg_replace = ""
    flag = True

    # Adding and Updating Objects
    session.add_all(obj_list)

    try:
        # Commit the transaction
        session.commit()
    except Exception as e:
        session.rollback()
        flag = False
        error_msg_ignore = '{}'.format(e).decode("utf-8", errors="ignore")
        error_msg_replace = '{}'.format(e).decode("utf-8", errors="replace")
        print(error_msg_ignore)
        write_multiprocess_log(multi_log_lock, u'Process ' + unicode(process_id) + u': ' + error_msg_replace)
    else:
        # 执行成功
        write_multiprocess_log(multi_log_lock,
                               'Process {}: {}'.format(process_id, u"Write to database successfully."))
    finally:
        # Close the Session
        if session:
            session.close()

    return flag


def write_events(updated_events, multi_log_lock, session, multi_process_flag=False):
    """
    write events to database
    parameter updated_events is a dictionary that contains all coded information and original information
    """

    records_insert = []
    for story_id in updated_events:
        # story's meta data
        story_meta = updated_events[story_id]["meta"]
        story_title = story_meta["title"]
        story_date = story_meta["date"]
        story_source = story_meta["source"]
        story_url = story_meta["url"]

        # all sentences in a story
        sents = updated_events[story_id]["sents"]

        # if the story was discarded when do_coding, no records will be generated to db
        if sents is None:
            break

        # sent_no表示当前句子在story中的编号
        for sent_no in sents:

            # the current sentence
            sent = sents[sent_no]

            # if a sentence has no events, skip it
            if "events" not in sent:
                continue
            
            # the sentenct's original content
            sent_content = sent['content']
            # all events extracted from the sentence
            sent_events = sent['events']

            # event_no表示当前事件在句子中的编号
            # event表示当前事件
            for event_no, event in enumerate(sent_events):
                one_record = None
                # generate one record to be inserted into database
                one_record = generate_one_record(sent, event, story_id, sent_no, event_no, sent_content)
                if one_record:
                    records_insert.append(one_record)

    # write to db
    if records_insert:
        if multi_process_flag:
            flag = do_write_db_multiprocess(session, records_insert, multi_log_lock)
            if flag:
                for story_id in updated_events:
                    result = write_to_solr(story_id)
                    if result is False:
                        write_multiprocess_log(multi_log_lock, u'Process ' + unicode(os.getpid()) + u': ' + u'Something goes wrong in solr write, please check the log1'+u'id:story_id:%s'%story_id)
        else:
            flag = do_write_db(records_insert)
            if flag:
                for story_id in updated_events:
                    result = write_to_solr(story_id)
                    if result is False:
                        logging.warn('something goes wrong in solr write,please check the log2'+'id:story_id:%s'%story_id)
    else:
        # write to solr
        result = write_to_solr(story_id)
        if result is False:
            write_multiprocess_log(multi_log_lock, u'Process ' + unicode(
                os.getpid()) + u': ' + u'Something goes wrong in solr write, please check the log3'+u'id:story_id:%s'%story_id +u'       records_insert:%s'%(','.join(records_insert)))


def generate_one_record(sent, event, story_id, sent_no, event_no, sent_content):
    """
    sent 当前句子
    event 当前事件
    """

    one_record = None

    # 获取时间、地点
    the_time,the_location = get_time_location(sent_content)

    # 事件的全局唯一ID
    global_event_id = "{}_{}_{}".format(story_id, sent_no, event_no)

    # 事件发生时间，事件发生年月，事件发生年份
    sql_date = datetime.date(datetime.today())
    month_year = None
    the_year = None
    if 'YMD' in the_time:
        sql_date = datetime.date(datetime.strptime(the_time['YMD'], '%Y%m%d'))
        month_year = the_time['YMD'][:-2]
        the_year = the_time['YMD'][:-4]

    elif 'YM' in the_time:
        try:
            sql_date = datetime.date(datetime.strptime(the_time['YM'], '%Y%m'))
            month_year = the_time['YM']
            the_year = the_time['YM'][:-2]
        except Exception:
            print '++++++++++++++++++++++++++++++++++++++++++++++++++++'
            print(the_time)
    elif 'Y' in the_time:
        sql_date = datetime.date(datetime.strptime(the_time['Y'], '%Y'))
        month_year = the_time['Y']
        the_year = the_time['Y']

    # get aciton location
    al_dict = dict(ACTIONGEO_FULLNAME='', ACTIONGEO_COUNTRYCODE='', ACTIONGEO_ADM1CODE='',
                   ACTIONGEO_ADM2CODE='', ACTIONGEO_LAT='', ACTIONGEO_LONG='',
                   ACTIONGEO_FEATUREID='')
    if the_location:
        al_dict = get_actionGEO_detail(the_location[1])

    # 事件发生地纬度
    try:
        action_geo_latitude = float(al_dict['ACTIONGEO_LAT'])
    except ValueError:
        action_geo_latitude = None
    # 事件发生地经度
    try:
        action_geo_longitude = float(al_dict['ACTIONGEO_LONG'])
    except ValueError:
        action_geo_longitude = None
    
    # 发起者的名称，承受者的名称
    source_name = None
    target_name = None
    if PETRglobals.WriteActorText:
        actors = sent['meta']['actortext'][event]
        source_name = actors[0]
        target_name = actors[1]

    # read (person, country) dictionary
    person_country = read_key_value_file(constants.PERSON_COUNTRY_FILE)
    # read (country alias, country) dictionary
    country_country = read_key_value_file(constants.COUNTRY_COUNTRY_FILE)

    # source's info by solr
    source_countrycode = None
    if source_name.upper() in country_country:
        source_country = country_country[source_name.upper()]
        # 发起者的国家代码（Alpha3标准）
        source_countrycode = query_info_by_solr(source_country)
        # translate countrycode from alpha2 to alpha3 standard
        source_countrycode = alpha2_to_alpha3(source_countrycode)
    elif source_name.upper() in person_country:
        source_country = person_country[source_name.upper()]
        # 发起者的国家代码（Alpha3标准）
        source_countrycode = query_info_by_solr(source_country)
        # translate countrycode from alpha2 to alpha3 standard
        source_countrycode = alpha2_to_alpha3(source_countrycode)

    # target's info by solr
    target_countrycode = None
    if target_name.upper() in country_country:
        target_country = country_country[target_name.upper()]
        # 承受者的国家代码（Alpha3标准）
        target_countrycode = query_info_by_solr(target_country)
        # translate countrycode from alpha2 to alpha3 standard
        target_countrycode = alpha2_to_alpha3(target_countrycode)
    elif target_name.upper() in person_country:
        target_country = person_country[target_name.upper()]
        # 承受者的国家代码（Alpha3标准）
        target_countrycode = query_info_by_solr(target_country)
        # translate countrycode from alpha2 to alpha3 standard
        target_countrycode = alpha2_to_alpha3(target_countrycode)

    # 发起者的编码
    source_code = event[0]

    # 发起者 - 所属组织编码 , 宗教编码 , 国内角色编码
    skg_code = None
    srrc_dict = dict(RELIGION1CODE='', RELIGION2CODE='')
    srtc_dict = dict(TYPE1CODE='', TYPE2CODE='', TYPE3CODE='')
    if source_code:
        skg_code, srrc_dict, srtc_dict = ParseRoleCode.resolve_role_encoding(source_code)

    # 承受者的编码
    target_code = event[1]

    # 承受者 - 所属组织编码 , 宗教编码 , 国内角色编码
    tkg_code = None
    trrc_dict = dict(RELIGION1CODE='', RELIGION2CODE='')
    trtc_dict = dict(TYPE1CODE='', TYPE2CODE='', TYPE3CODE='')
    if target_code:
        tkg_code, trrc_dict, trtc_dict = ParseRoleCode.resolve_role_encoding(target_code)

    # 事件小类编码
    event_code = event[2]
    # EVENTBASECODE
    event_base_code = event_code[:3]
    # 事件大类编码
    event_root_code = event_code[:2]
    # 四大类编码
    evtQuadClass = ParseRoleCode.resolve_quadclass(event_root_code)
    # 事件类型得分
    evtClassScore = ParseRoleCode.get_goldsteinscale(event_code)

    # one record to be inserted to database
    one_record = PetrarchEvents2(
        globaleventid=global_event_id, sqldate=sql_date, monthyear=month_year,
        year=the_year, actor1code=source_code, actor1name=source_name,
        actor1countrycode=source_countrycode, actor1knowngroupcode=skg_code, actor1religion1code=srrc_dict['RELIGION1CODE'],
        actor1religion2code=srrc_dict['RELIGION2CODE'], actor1type1code=srtc_dict['TYPE1CODE'], actor1type2code=srtc_dict['TYPE2CODE'],
        actor1type3code=srtc_dict['TYPE3CODE'], actor2code=target_code, actor2name=target_name, actor2countrycode=target_countrycode,
        actor2knowngroupcode=tkg_code, actor2religion1code=trrc_dict['RELIGION1CODE'],
        actor2religion2code=trrc_dict['RELIGION2CODE'], actor2type1code=trtc_dict['TYPE1CODE'], actor2type2code=trtc_dict['TYPE2CODE'],
        actor2type3code=trtc_dict['TYPE3CODE'], eventcode=event_code,
        eventbasecode=event_base_code, eventrootcode=event_root_code,
        quadclass=evtQuadClass,
        goldsteinscale=evtClassScore, avgtone=None,
        actor1geo_fullname=None, actor1geo_countrycode=None, actor1geo_adm1code=None,
        actor1geo_adm2code=None, actor1geo_lat=None, actor1geo_long=None, actor1geo_featureid=None,
        actor2geo_fullname=None, actor2geo_countrycode=None, actor2geo_adm1code=None,
        actor2geo_adm2code=None, actor2geo_lat=None, actor2geo_long=None, actor2geo_featureid=None,
        actiongeo_type=None,
        actiongeo_fullname=al_dict['ACTIONGEO_FULLNAME'], actiongeo_countrycode=al_dict['ACTIONGEO_COUNTRYCODE'],
        actiongeo_adm1code=al_dict['ACTIONGEO_ADM1CODE'], actiongeo_adm2code=al_dict['ACTIONGEO_ADM2CODE'],
        actiongeo_lat=action_geo_latitude, actiongeo_long=action_geo_longitude,
        actiongeo_featureid=al_dict['ACTIONGEO_FEATUREID'],
        dateadded=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        sourceurl=story_id, event_sentence=sent_content, language_flag=0)

    return one_record


def get_time_location(content):
    """
    content is the original sentence
    """

    obj_EvtInfo = GetInfo.GetInfoForEvt(jars=constants.JARS_DIR, mark_time_ranges=True)
    tempsent = [content]
    strEvtTime = obj_EvtInfo.time_parse(tempsent)


    if len(strEvtTime) > 10:
        strEvtTime = strEvtTime[:10]

    #tmp = [{'value': '2017-09-22'}]
    tmp = [{'value': strEvtTime}]
    ## 20170925 modified end

    the_time = dict()
    for each in tmp:
        try:
            if '-' in each['value']:
                if each['value'].count('-') == 2:
                    times = datetime.strptime(each['value'], '%Y-%m-%d')
                    the_time['YMD'] = format(times, '%Y%m%d')
                    #the_time['YMD'] = each['value']
                elif each['value'].count('-') == 1:
                    times = datetime.strptime(each['value'], '%Y-%m')
                    the_time['YM'] = format(times, '%Y%m')
                    #the_time['YM'] = each['value']
            elif type(each['value']) == dict:
                #the_time['H'] = datetime.strptime(each['value']['begin'])
                pass  #事件发生的小时,暂时不需要存入数据库
            elif len(each['value']) == 4:
                times = datetime.strptime(each['value'], '%Y-%m-%d')
                the_time['Y'] = format(times, '%Y')
        except Exception:
            pass


    # format is xml flag
    strEvtLocation = obj_EvtInfo.loca_parse(tempsent, "2")

    the_location = dict()
    ## transfor location
    if len(strEvtLocation) > 0:
        ls_tempsents = strEvtLocation.strip().replace("\r\n", '\n').replace('\n', ' ').split('.')
        breakcount = 0
        for tempsents in ls_tempsents:
            while(('<LOCATION>' in tempsents) and breakcount < 40):
                breakcount = breakcount + 1
                lc_begin_index = tempsents.find('<LOCATION>')
                lc_begin_index = lc_begin_index + 10
                lc_end_index = tempsents.find('</LOCATION>', lc_begin_index)
                tempLocation = tempsents[lc_begin_index:lc_end_index].strip()

                if tempLocation:
                    the_location[breakcount] = tempLocation

                lc_end_index = lc_end_index + 11
                tempsents = tempsents[lc_end_index:]

    return the_time, the_location


def get_actionGEO_detail(event_location):

    solr_address = read_solr_address('geo')
    solr_address = solr_address + '/select?indent=on&q=%s&wt=json'

    # catch solr timeout
    try:
        action_location = requests.get(solr_address % event_location, timeout=3).text
        action_location = json.loads(action_location)
    except:
        # solr none
        action_location = '{"responseHeader": {"status": 0,"QTime": 0,"params": {"q": "","indent": "true","wt": "json","_": "1111122222"}},"response": {"numFound": 0,"start": 0,"docs": []}}'
        action_location = json.loads(action_location)
    
    actionInfo_dict = dict(ACTIONGEO_FULLNAME='', ACTIONGEO_COUNTRYCODE='', ACTIONGEO_ADM1CODE='',
                           ACTIONGEO_ADM2CODE='', ACTIONGEO_LAT='', ACTIONGEO_LONG='', ACTIONGEO_FEATUREID='')

    docs_list = list()
    docs_list = action_location['response']['docs']

    for i, val in enumerate(docs_list):
        #if val['asciiname'] == event_location:
            actionInfo_dict['ACTIONGEO_FULLNAME'] = val['asciiname']
            actionInfo_dict['ACTIONGEO_COUNTRYCODE'] = val['countrycode']    # Alpha2
            actionInfo_dict['ACTIONGEO_ADM1CODE'] = val['admin1code']
            actionInfo_dict['ACTIONGEO_ADM2CODE'] = val['admin2code']
            actionInfo_dict['ACTIONGEO_LAT'] = val['latitude']
            actionInfo_dict['ACTIONGEO_LONG'] = val['longitude']
            actionInfo_dict['ACTIONGEO_FEATUREID'] = val['id']
            break

    return actionInfo_dict
