#!/usr/bin/python
# -*- coding: utf-8 -*-


__author__ = 'George K. <gkiom@iit.demokritos.gr>'

import subprocess
import os
import traceback

import requests

from psql_dbaccess import PSQLDBAccess
from ditLogger import DITLogger


CRAWL_DIR_NAME = "/home/gkioumis/Downloads/"
CRAWL_CONFIG_FILE_PATH = "/home/ubuntu/crawler/config.properties"
CRAWLER_JAVA_NAME = "OpenGovCrawler.jar"
SOLR_INDEX_URLS = \
    ["http://localhost:8983/solr/dit_comments/dataimport?command=full-import&clean=true",
     "http://localhost:8983/solr/dit_articles/dataimport?command=full-import&clean=true"]
# SOLR_INDEX_URL = ["http://localhost:8983/solr/dataimport?command=delta-import"]  
# currently cannot get delta-import to work
WORDCLOUD_URL = "http://localhost:28084/WordCloud/Extractor"
LOG_FILE = os.path.abspath(os.path.join(os.getcwd(), os.pardir)) + "/scheduler.log"

class Scheduler:
    """Main scheduler implementation"""
    total = 0  # total controllers
    results = {}  # results of each module, if any

    def __init__(self, log_file=None):
        # init storage
        self.psql = PSQLDBAccess()
        # init logger
        self.logger = DITLogger(filename=log_file if log_file else LOG_FILE)
        self.prev_comment_id = 0

    def get_modules(self):
        # possibly read controllers dir and fetch a list of file names (except init)
        # TODO: add more modules
        modules = {
            1: ControllerCrawl(dir_name=CRAWL_DIR_NAME, java_exec=CRAWLER_JAVA_NAME,
                               config_file=CRAWL_CONFIG_FILE_PATH),
            2: ControllerIndex(urls=SOLR_INDEX_URLS),
            3: ControllerWordCloud(url=WORDCLOUD_URL)}
        return modules

    def execute_pipeline(self, first=False):
        # get all modules
        modules = self.get_modules()
        self.total = len(modules)
        # get previous comment ID
        if first:
            self.prev_comment_id = 0
        if not first:
            self.prev_comment_id = self.psql.get_latest_comment_id()
        # log initialization
        self.logger.info("Initializing schedule for %d modules. "
                         "Last commend_id: %d" % (self.total, self.prev_comment_id))
        # execute pipeline
        for step, controller in modules.items():
            self._execute_controller(step, controller)

        # finalized
        self.logger.info("Finalized schedule successfully at ")

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
            print os.getcwd()
            os.chdir(os.path.dirname(self.dir_name))
            print os.getcwd()
            subprocess.call(['java', '-jar', self.java_exec, self.config_file])
            # return the consultations updated by the crawler
            return self.psql.get_updated_consultations(self.prev_comment_id)
        except OSError, er:
            self.logger.error('Exception: %s :\n %s' % (er, traceback.format_exc()))


class ControllerIndex(Scheduler):
    def __init__(self, urls=None):
        self.urls = urls if urls else ["http://localhost:8983/solr/dit_comments/etc"]
        Scheduler.__init__(self)

    def __repr__(self):
        return "ControllerIndex"

    def execute(self):
        """
        :return: None
        """
        for eachURL in self.urls:
            self.logger.info('executing import on comments table: calling %s' % eachURL)
            r = requests.get(eachURL)
            response = r.status_code
            self.logger.info("import completed with response code: %d " % response)


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
        :return: None
        """
        if not self.consultations:
            consultations = self.results.get(repr(ControllerCrawl()))
            if not consultations:  # if no crawler has run, then we must load all
                self.consultations = self.psql.get_updated_consultations(prev_comment_id=0)
                self.logger.info(self.__repr__() + ": " + "No consultations passed: fetching all (%d total)" % len(
                    self.consultations))
            else:
                self.consultations = consultations
        # init procedure
        results = {}
        # for each consultation
        for cons in self.consultations:
            # call extractor and keep result status code
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
    import gflags

    gflags.DEFINE_string('log_file',
                     LOG_FILE,
                     'The file to log.')

    gflags.DEFINE_bool('first_run',
                     False,
                     'use this if running for first time.')

    FLAGS = gflags.FLAGS

    try:
        argv = FLAGS(sys.argv)
    except gflags.FlagsError as e:
        print('%s\\nUsage: %s ARGS\\n%s' % (e, argv[0], FLAGS))
        sys.exit(1)

    scheduler = Scheduler(log_file=FLAGS.log_file)
    scheduler.execute_pipeline(first=FLAGS.first_run)

