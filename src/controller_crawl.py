#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = 'George K. <gkiom@scify.org>'

import subprocess
import os
import sys

DIR_NAME = "/home/ubuntu/crawler/"
CRAWLER_CONFIG_FILE_PATH = "./crawler.properties"
JAVA_NAME = "OpenGovCrawler.jar"


def run_crawler(dir_name=None, java_name=None, config_file=None):
    """
    will initiate the crawler (os.subprocess).
    If no params specified, will use defaults
    """
    java_name_cur = JAVA_NAME

    if not dir_name:
        os.chdir(os.path.dirname(DIR_NAME))
    else:
        os.chdir(os.path.dirname(dir_name))

    if java_name:
        java_name_cur = java_name

    if not config_file:
        subprocess.call(['java', '-jar', java_name_cur, CRAWLER_CONFIG_FILE_PATH])
    else:
        subprocess.call(['java', '-jar', java_name_cur, config_file])


if __name__ == "__main__":
    if len(sys.argv) == 1:
        run_crawler()
    elif len(sys.argv) == 2:
        run_crawler(sys.argv[1])
    elif len(sys.argv) == 3:
        run_crawler(sys.argv[1], sys.argv[2])
    elif len(sys.argv) == 4:
        run_crawler(sys.argv[1], sys.argv[2], sys.argv[3])