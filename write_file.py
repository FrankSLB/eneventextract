# -*- coding: utf-8 -*-

import io
import os
import datetime
from shutil import copyfile

from constants import MULTI_PROCESS_INFO, MULTI_PROCESS_LOG_DIR

# 多进程日志文件数量
BACKUP_LOG_NUM = 5
# 多进程日志文件名字列表
BACKUP_LOG_NAMES = [os.path.join(MULTI_PROCESS_LOG_DIR, (u'multi_process_' + unicode(i) + u'.log'))
                    for i in range(1, BACKUP_LOG_NUM + 1)]
# 多进程日志文件，每个文件的最大值，单位为字节（B），1024表示1KB，10485760表示10MB
BACKUP_LOG_SIZE = 10485760


# dst must be in the scope of list
def _backup(src, dst):
    if not os.path.exists(dst):
        copyfile(src, dst)
    else:
        if dst == BACKUP_LOG_NAMES[-1]:
            copyfile(src, dst)
        else:
            pos = BACKUP_LOG_NAMES.index(dst)
            _backup(dst, BACKUP_LOG_NAMES[pos + 1])
            copyfile(src, dst)


def write_multiprocess_log(lock, content, mode='a'):
    lock.acquire()

    # 向日志文件写日志
    with io.open(MULTI_PROCESS_INFO, mode, encoding=u'utf-8') as f:
        time_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        f.write(unicode(time_now) + u' ' + content + u'\n')

    # 判断日志大小是否超过阀值
    if os.path.getsize(MULTI_PROCESS_INFO) > BACKUP_LOG_SIZE:
        # 备份当前日志文件
        _backup(MULTI_PROCESS_INFO, BACKUP_LOG_NAMES[0])
        # 清空日志文件
        with io.open(MULTI_PROCESS_INFO, 'w', encoding=u'utf-8'):
            pass

    lock.release()
