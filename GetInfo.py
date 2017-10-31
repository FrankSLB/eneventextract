import os
import imp
import jpype
import socket
import threading
#import json

socket.setdefaulttimeout(15)


class GetInfoForEvt(object):

    _required_jars = {
        'STFnlp.jar'
    }

    def __init__(self, jars=[], jvm_started=False, mark_time_ranges=False, include_range=False):
        """Initializes GetTimeForEvt.
        """
        self.mark_time_ranges = mark_time_ranges
        self.include_range = include_range
        self.jars = jars
        self._is_loaded = False
        self._lock = threading.Lock()

        if not jvm_started:
            self._classpath = self._create_classpath()
            self._start_jvm()

        try:
            # make it thread-safe
            if threading.activeCount() > 1:
                if jpype.isThreadAttachedToJVM() is not 1:
                    jpype.attachThreadToJVM()
            self._lock.acquire()

            EvtInfoWrapper = jpype.JClass('STFnlp.getEvtInfo')
            #self._sutime = EvtTimeWrapper(self.mark_time_ranges, self.include_range)
            self._getinfo = EvtInfoWrapper()
            self._is_loaded = True
        finally:
            self._lock.release()

    def _start_jvm(self):
        if jpype.isJVMStarted() is not 1:
            jpype.startJVM(
                jpype.getDefaultJVMPath(),
                '-Djava.class.path={classpath}'.format(
                    classpath=self._classpath)
            )

    def _create_classpath(self):

        sutime_jar = os.getcwd() + '\\jars'

        jars = [sutime_jar]
        jar_file_names = []
        for top, dirs, files in os.walk(self.jars):
            for file_name in files:
                if file_name.endswith('.jar'):
                    jars.append(os.path.join(top, file_name))
                    jar_file_names.append(file_name)
        if not GetInfoForEvt._required_jars.issubset(jar_file_names):
            raise RuntimeError('Not all necessary Java dependencies have been downloaded!')
        return os.pathsep.join(jars)

    def time_parse(self, input_str):

        if self._is_loaded is False:
            raise RuntimeError('Please load GetInfo model first!')

        if input_str:
            return self._getinfo.getEvtTime(input_str)
        else:
            return ""

    def loca_parse(self, input_str,func_flag=None):

        if self._is_loaded is False:
            raise RuntimeError('Please load GetInfo model first!')

        if input_str and func_flag:
            return self._getinfo.getLocation(input_str, func_flag)
        else:
            return ""