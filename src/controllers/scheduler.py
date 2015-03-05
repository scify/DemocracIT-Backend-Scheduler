#!/usr/bin/python
# -*- coding: utf-8 -*-
from src.dbaccess.local_dbaccess import LocalDBAccess
from src.dbaccess.psql_dbaccess import PSQLDBAccess

__author__ = 'George K. <gkiom@iit.demokritos.gr>'

import subprocess
import os

CRAWL_DIR_NAME = "/home/ubuntu/crawler/"
CRAWL_CONFIG_FILE_PATH = "./crawler.properties"
JAVA_NAME = "OpenGovCrawler.jar"


class Scheduler:

    total = 0 # total controllers
    results = {}

    def __init__(self):
        # init storage
        self.psql = PSQLDBAccess()
        self.local = LocalDBAccess()

    def get_modules(self):
        # possibly read controllers dir and fetch a list of file names (except init)
        # TODO: add more modules
        modules = {1: ControllerCrawl, 2: ControllerIndex, 3: ControllerWordCloud}
        return modules

    def execute(self):
        # get all modules
        modules = self.get_modules()
        self.total = len(modules)

        # get previous comment ID
        self.prev_comment_id = self.psql.get_latest_comment_id()

        for k, v in modules.items():
            self._execute_controller(k, v)

    def _execute_controller(self, step, controller):
        # log step initiation
        self.local.schedule_step(step_num=step)
        # call controller
        result = controller.execute()
        if result:
            self.results[controller] = result


class ControllerCrawl(Scheduler):
    def __init__(self, dir_name=None, java_name=None, config_file=None):
        self.dir_name = dir_name
        self.java_name = java_name
        self.config_file = config_file
        Scheduler.__init__()

    def execute(self):
        """
        will initiate the crawler (os.subprocess).
        If no params specified, will use defaults
        """
        java_name_cur = JAVA_NAME
        try:
            if not self.dir_name:
                os.chdir(os.path.dirname(CRAWL_DIR_NAME))
            else:
                os.chdir(os.path.dirname(self.dir_name))

            if self.java_name:
                java_name_cur = self.java_name

            if not self.config_file:
                subprocess.call(['java', '-jar', java_name_cur, CRAWL_CONFIG_FILE_PATH])
            else:
                subprocess.call(['java', '-jar', java_name_cur, self.config_file])

                return self.psql._get_consultation_ids_after(self.prev_comment_id)
        except OSError, e:
            print 'Error %s' % e


class ControllerIndex(Scheduler):
    def __init__(self, url=None):
        if not url:
            self.url = "http://localhost:8089/solr/etc"
        else:
            self.url = url
        Scheduler.__init__()

    def execute(self):
        """
        :return: None
        """
        print 'executing delta import on comments table calling %s' % self.url


class ControllerWordCloud(Scheduler):

    consultations = set()

    def __init__(self, consultations=None):
        if consultations:
            self.consultations = consultations
        Scheduler.__init__()

    def execute(self):
        """
        :param consultations: a set of consultation IDs
        :return: None
        """
        if not self.consultations:
            pass
        else:
            for cons in self.consultations:
                self._call_wordcloud_extractor(cons)

    def _call_wordcloud_extractor(self, cons):
        print "Calling wordcloud extractor for consultation %d" % cons


if __name__ == "__main__":
    scheduler = Scheduler()
    scheduler.execute()