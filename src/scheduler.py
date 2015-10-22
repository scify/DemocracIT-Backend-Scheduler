#!/usr/bin/python
# -*- coding: utf-8 -*-
from datetime import datetime
import subprocess
import os
import re
import importlib

import requests
import yaml

from psql_dbaccess import PSQLDBAccess
from dit_logger import DITLogger

__author__ = 'George K. <gkiom@scify.org>'

CLASS_LABEL = 'class'
PACKAGE_LABEL = 'package'
PARAM_LABEL = 'params'

DEFAULT_LOG_FILE = os.path.abspath(os.path.join(os.getcwd(), os.pardir)) + "/scheduler.log"


class Scheduler:
    """Main scheduler implementation"""
    total = 0  # total controllers
    results = {}  # results of each module, if any
    prev_comment_id = 0  # comment ID from previous schedule
    date_start = 0  # start date of schedule

    def __init__(self, log_file=None, schedules=None):
        # init storage
        self.psql = PSQLDBAccess()
        # init logger
        self.logger = DITLogger(filename=log_file if log_file else DEFAULT_LOG_FILE)
        # schedules file
        self.schedule_settings_file = schedules

    def execute_pipeline(self, first=False):
        """
        Execute the schedule, as stated in the yaml file
        """
        # mark started
        self.date_start = datetime.now()
        # get all modules to execute
        modules = Scheduler.get_modules(self.schedule_settings_file)
        self.total = len(modules)
        # get previous comment ID
        if not first:
            Scheduler.prev_comment_id = self.psql.get_latest_comment_id()
            # DEBUG: Comment!
            # Scheduler.prev_comment_id = 387627
        else:
            Scheduler.prev_comment_id = 0
        # log initialization
        self.logger.info("Initializing schedule for %d modules. "
                         "Last commend_id: %d" % (self.total, Scheduler.prev_comment_id))
        # execute pipeline
        for step, controller in modules.items():
            self._execute_controller(step, controller)

        # finalized
        self.logger.schedule_step(step_num=step, total_steps=self.total, date_start=self.date_start, date_end=datetime.now())

    def _execute_controller(self, step, controller):
        """
        Execute the controller passed, and if this controller returns smth, store it
        """
        # log step
        self.logger.schedule_step(step_num=step, total_steps=self.total, date_start=self.date_start)
        result = controller.execute(self.results.get('ControllerCrawl'))  # applied custom hack to pass consultations to wordcloud
        if result:
            self.results[repr(controller).split(":")[0]] = result

    @staticmethod
    def get_previous_comment_id():
        return Scheduler.prev_comment_id

    @staticmethod
    def get_modules(schedules_file_path):
        """
        :param schedules_file_path: the path to the yaml file
        :return: a dict containing the instances to be executed
        """
        modules = {}
        # inject class instances, with parameters from settings file
        with open(schedules_file_path, 'r') as inp:
            scheduler_settings = yaml.load(inp)
            for index, setting in enumerate(scheduler_settings):
                cl_set = setting[CLASS_LABEL]
                pack_set = setting[PACKAGE_LABEL]
                params_set = setting[PARAM_LABEL]
                pack = importlib.import_module(pack_set)
                cl = getattr(pack, cl_set)
                modules[index + 1] = cl(**params_set)
        # print [k for k in modules.values()]  # debug
        return modules


class ControllerCrawl(Scheduler):
    def __init__(self, dir_name, java_exec, executable_class, config_file):
        self.dir_name = dir_name
        self.java_exec = java_exec
        self.executable_class = executable_class
        self.config_file = config_file
        Scheduler.__init__(self)

    def __repr__(self):
        return "ControllerCrawl: {}".format(self.__dict__)

    def execute(self, incoming):
        """
        will initiate the crawler (os.subprocess).
        :return the list of consultations updated with new comments
        """
        try:
            cur_work_dir = os.getcwd()
            os.chdir(os.path.dirname(self.dir_name))
            # find all dependencies
            libs = subprocess.check_output(['find', '-iname', '*.jar'])
            class_path = ":".join([os.path.join(os.getcwd(), k) for k in libs.split() if k.endswith('.jar')])
            # start crawler
            subprocess.call(["java", "-cp", class_path, self.executable_class, self.config_file])
            # return the consultations updated by the crawler
            found = self.psql.get_updated_consultations(Scheduler.get_previous_comment_id())
            os.chdir(cur_work_dir)
            if found:
                return found
            else:
                return []
        except Exception, ex:
            self.logger.exception(ex)
            return None


class ControllerIndex(Scheduler):
    def __init__(self, urls=None):
        self.urls = urls if urls else ["http://localhost/solr/dit_comments/etc"]  # just an example, urls MUST exist
        Scheduler.__init__(self)

    def execute(self, incoming):
        """
        :return: None
        """
        for eachURL in self.urls:
            self.logger.info('executing import on %s table: calling %s'
                             % (re.findall('dit_(\w+)', eachURL)[0], eachURL))
            try:
                r = requests.get(eachURL)
                response = r.status_code
                self.logger.info("import completed with response code: %d " % response)
            except Exception, ex:
                self.logger.exception(ex)

    def __repr__(self):
        return "ControllerIndex: {}".format(self.__dict__)


class ControllerWordCloud(Scheduler):
    consultations = set()

    def __init__(self, url, consultations=None, fetchall=False):
        self.url = url
        if consultations:
            self.consultations = consultations
        self.fetch_all_consultations = fetchall
        Scheduler.__init__(self)

    def execute(self, incoming):
        """
        :return: None
        """
        if not self.consultations:
            if incoming:
                consultations = incoming
            if not consultations:
                if self.fetch_all_consultations:
                    # if no crawler has run, then we must load all
                    self.consultations = self.psql.get_updated_consultations(prev_comment_id=0)
                    self.logger.info(self.__str__() + ": " + "No consultations passed: fetching all (%d total)"
                                     % len(self.consultations))
            else:
                self.consultations = consultations
                # self.logger.info('got %d consultations' % len(self.consultations))
        # init procedure
        results = {}

        if len(self.consultations) == 0:
            self.logger.info("No new consultations, or no consultations updated with new comments!")
            return results

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
        # self.logger.info("imitating Calling word cloud extractor for consultation %d" % cons)
        try:
            r = requests.get(self.url + "?consultation_id=%d" % cons)
            return r.status_code
            # return 200
        except Exception, ex:
            self.logger.exception(ex)
            return 503  # service unavailable

    def __repr__(self):
        return "ControllerWordCloud: {}".format(self.__dict__)

    def __str__(self):
        return 'ControllerWordCloud'


class ControllerFekAnnotator(Scheduler):
    def __init__(self, dir_name, java_exec, executable_class, config_file=None):
        self.dir_name = dir_name
        self.java_exec = java_exec
        self.executable_class = executable_class
        self.config_file = config_file
        Scheduler.__init__(self)

    def execute(self, incoming):
        """
        will initiate the annotator (os.subprocess)
        :return None
        """
        try:
            cur_work_dir = os.getcwd()
            os.chdir(os.path.dirname(self.dir_name))
            # find all dependencies
            libs = subprocess.check_output(['find', '-iname', '*.jar'])
            class_path = ":".join([os.path.join(os.getcwd(), k) for k in libs.split() if k.endswith('.jar')])
            # call annotator extractor
            if self.config_file:
                subprocess.call(["java", "-cp", class_path, self.executable_class, self.config_file])
            else:
                subprocess.call(["java", "-cp", class_path, self.executable_class])
            os.chdir(cur_work_dir)
        except Exception, ex:
            self.logger.exception(ex)
        return None

    def __repr__(self):
        return "ControllerFekAnnotator: {}".format(self.__dict__)

if __name__ == "__main__":
    import sys
    import gflags

    gflags.DEFINE_string('log_file', DEFAULT_LOG_FILE, 'The file to log.')

    gflags.DEFINE_string('schedules', "../schedules.yaml", 'the settings file to load')

    gflags.DEFINE_bool('first_run', False, 'use this if running for first time.')

    FLAGS = gflags.FLAGS

    try:
        argv = FLAGS(sys.argv)
    except gflags.FlagsError as e:
        print('%s\\nUsage: %s ARGS\\n%s' % (e, sys.argv[0], FLAGS))
        sys.exit(1)

    scheduler = Scheduler(log_file=FLAGS.log_file, schedules=FLAGS.schedules)
    scheduler.execute_pipeline(first=FLAGS.first_run)

