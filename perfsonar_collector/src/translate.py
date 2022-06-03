import os
import time
import inspect
import json
import warnings
import sys
import configparser
from time import strftime
from time import localtime
import traceback

import pscheduler

from .esmond.api.client.perfsonar.query import ApiFilters
from .esmond.api.client.perfsonar.query import ApiConnect, ApiConnectWarning


class Uploader():
    def __init__(self, start = 1600, connect, metricName, config = None, log = None, backprocess_start = None, backprocess_end = None):
        self.log = log
        self.config = config
        self.metricName =  metricName
        #conf_dir = os.path.join("/", "etc", "rsv", "metrics")
        ########################################        
        self.debug = self.str2bool(self.readConfigFile('debug', "false"))
        verbose = self.debug
        # Filter variables
        self.connect = connect
        self.filters = ApiFilters()
        self.filters.verbose = True
        self.filters.ssl_verify = False
        #filters.verbose = True 
        # this are the filters that later will be used for the data
        if backprocess_start and backprocess_end:
            self.time_end = int(time.mktime(backprocess_end.timetuple()))
            self.time_start = int(time.mktime(backprocess_start.timetuple()))
            log.info("Inside uploader, starting backprocess")
        else:
            self.time_end = int(time.time())
            self.time_start = int(self.time_end - start)
            
            
    def getData(self):
        disp = self.debug
        summary = self.summary
        if summary:
            self.log.info("Reading Summaries")
        else:
            self.log.info("Omitting Summaries")
        self.getMetadata(self.time_start)

    def getMetadata(self, time_start):
        self.filters.time_start = time_start
        self.log.debug("Querying starting time {}".format(time_start))

        # Try SSL first
        if self.useSSL == True:
            self.conn = ApiConnect("https://"+self.connect, self.filters)
        else:
            self.conn = ApiConnect("http://"+self.connect, self.filters)
        
        try:
            #Test to see if https connection is succesfull
            metadata = self.conn.get_metadata()
            md = next(metadata)
            self.readMetaData(md)
        except  StopIteration:
            self.log.warning("There is no metadata in this time range")
            return
        except ConnectionError as httpsException:
            #Test to see if https connection is sucesful      
            self.log.debug("Failed to connect to %s with https, trying http" % self.connect)                                                                                         
            try:
                self.conn = ApiConnect("http://"+self.connect, self.filters)
                metadata = self.conn.get_metadata()
                md = next(metadata)
                self.useSSL = False
                self.readMetaData(md)
            except  StopIteration:
                self.log.warning("There is no metadata in this time range")
            except  ConnectionError as e:
                raise Exception("Unable to connect to %s, exception was %s, " % ("https://"+self.connect, type(e)))
        except Exception as e:
            self.log.exception("Failed to read metadata")
            return

        for md in metadata:
            self.readMetaData(md)
            
    def readMetaData(self, md):
        disp = self.debug
        disp = True
        summary = self.summary
        arguments = {}
        # Building the arguments for the post
        arguments = {
            "subject_type": md.subject_type,
            "source": md.source,
            "destination": md.destination,
            "tool_name": md.tool_name,
            "measurement_agent": md.measurement_agent,
            "input_source": md.input_source,
            "input_destination": md.input_destination,
            "tool_name": md.tool_name
        }
        metadata_key = md.metadata_key
   
##CHANGE TO NEW LOGSTASH OR ES
    def postarchiver(self,msg_body,event,metadata_key):
        result = None
        arguments['org_metadata_key'] = metadata_key
        try:
            requests.post('https://ps-collection.atlas-ml.org',data=self.msg_body)
            break
        except Exception as e:
            self.log.exception("Failed to Upload to archiver,, exception was %s, " % (repr(e)))
            
            
##WORK IN ACCORDANCE WITH FORMATTING OF CLASS
    def createbody(self,arguments,event_types,datapoints):
        self.msg_body = {}
         for event in list(datapoints.keys()):
#             # skip events that have no datapoints 
            if not datapoints[event]:
                continue
#             # compose msg
            arguments['rsv-timestamp'] = "%s" % time.time()
            arguments['event-type'] =  event
            arguments['summaries'] = 0
#             # including the timestart of the smalles measureement
            ts_start = min(datapoints[event].keys())
            arguments['ts_start'] = ts_start
            self.msg_body = {"archiver":"http","data":{"_url":"https://ps-collection.atlas-ml.org",
                "op":"post","retry_policy":[{"attempts":5,"wait":"PT1M"},
                {"attempts":11,"wait":"PT5M"},{"attempts":23,"wait":"PT1H"},
                {"attempts":6,"wait":"PT4H"}]},
                "ttl":"P2DT1H"}
            self.msg_body['data']['version'] = 2
            self.msg_body['data']['datapoints'] = datapoints[event]
            self.postarchiver(msg_body, event)