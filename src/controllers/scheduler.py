#!/usr/bin/python
# -*- coding: utf-8 -*-
from src.dbaccess.local_dbaccess import LocalDBAccess
from src.dbaccess.psql_dbaccess import PSQLDBAccess
from src.logging.ditLogger import DITLogger

__author__ = 'George K. <gkiom@iit.demokritos.gr>'

import subprocess
import os
import traceback
import requests

CRAWL_DIR_NAME = "/home/ubuntu/crawler/"
CRAWL_CONFIG_FILE_PATH = "./crawler.properties"
JAVA_NAME = "OpenGovCrawler.jar"
SOLR_INDEX_URL = "http://localhost:8983/solr/dataimport?command=delta-import"
WORDCLOUD_URL = "http://localhost:28084/WordCloud/Extractor"


class Scheduler:
    """Main scheduler implementation"""

    total = 0  # total controllers
    results = {} # results of each module, if any

    def __init__(self):
        # init storage
        self.psql = PSQLDBAccess()
        self.local = LocalDBAccess()
        # init logger
        self.logger = DITLogger()

    def get_modules(self):
        # possibly read controllers dir and fetch a list of file names (except init)
        # TODO: add more modules
        modules = {1: ControllerCrawl(dir_name=CRAWL_DIR_NAME, java_exec=JAVA_NAME, config_file=CRAWL_CONFIG_FILE_PATH),
                   2: ControllerIndex(url=SOLR_INDEX_URL),
                   3: ControllerWordCloud(url=WORDCLOUD_URL)}
        return modules

    def execute_pipeline(self, first=None):
        # get all modules
        modules = self.get_modules()
        self.total = len(modules)
        # get previous comment ID
        if not first:
            self.prev_comment_id = self.psql.get_latest_comment_id()
        if first and first == True:
            self.prev_comment_id = 0
        # log initialization
        self.logger.info("Initializing schedule for %d modules. "
                         "Last commend_id: %d" % (self.total, self.prev_comment_id))
        # execute pipeline
        for step, controller in modules.items():
            self._execute_controller(step, controller)

        # finalized
        self.logger.info("Finalized schedule successfully")

    def _execute_controller(self, step, controller):
        # log step
        self.logger.schedule_step(step_num=step, total_steps=self.total)
        # call controller
        result = controller.execute()
        if result:
            self.results[repr(controller)] = result


class ControllerCrawl(Scheduler):
    def __init__(self, dir_name=None, java_exec=None, config_file=None):
        self.dir_name = dir_name
        self.java_exec = java_exec
        self.config_file = config_file
        Scheduler.__init__(self)

    def __repr__(self):
        return "ControllerCrawl"

    def execute(self):
        """
        will initiate the crawler (os.subprocess).
        """
        try:
            # return [3451, 3452]
            os.chdir(os.path.dirname(self.dir_name))

            subprocess.call(['java', '-jar', self.java_exec, self.config_file])
            return self.psql.get_updated_consultations(self.prev_comment_id)
        except OSError, e:
            self.logger.error('Exception: %s :\n %s' % (e, traceback.format_exc()))


class ControllerIndex(Scheduler):
    def __init__(self, url=None):
        self.url = url if url else "http://localhost:8089/solr/etc"
        Scheduler.__init__(self)

    def __repr__(self):
        return "ControllerIndex"

    def execute(self):
        """
        :return: None
        """
        self.logger.info('executing delta import on comments table: calling %s' % self.url)
        r = requests.get(self.url)
        response = r.status_code
        self.logger.info("delta import completed: %d " % response)


class ControllerWordCloud(Scheduler):
    consultations = set()

    def __init__(self, url, consultations=None):
        self.url = url
        if consultations:
            self.consultations = consultations
        Scheduler.__init__(self)

    def __repr__(self):
        return "ControllerWordCloud"

    def execute(self):
        """
        :param consultations: a set of consultation IDs
        :return: None
        """
        if not self.consultations:
            self.consultations = self.results[repr(ControllerCrawl())]

        results = {}
        for cons in self.consultations:
            results[cons] = self._call_wordcloud_extractor(cons)

        for consultation_id, status_code in results.items():
            if status_code != 200:
                self.logger.error(
                    "Error: Response status code for consultation ID %d: %d" % (consultation_id, status_code))

    def _call_wordcloud_extractor(self, cons):
        """
        :param cons: a consultation ID
        :return the status_code response of the request
        """
        self.logger.info("Calling word cloud extractor for consultation %d" % cons)
        r = requests.get(self.url + "?consultation_id=%d" % cons)
        return r.status_code


if __name__ == "__main__":
    import sys
    scheduler = Scheduler()
    if len(sys.argv) == 1:
        scheduler.execute_pipeline()
    else:
        scheduler.execute_pipeline(True)