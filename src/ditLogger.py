#!/usr/bin/python
# -*- coding: utf-8 -*-
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler

from models import Schedule


__author__ = 'George K. <gkiom@scify.org>'


class DITLogger:

    class __impl:
        """ Implementation of the singleton interface """

        def __init__(self, level=None, filename=None):
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%d/%m/%Y %H:%M:%S')
            self.logger = logging.getLogger("DIT")
            self.logfile = filename
            self.level = level if level else logging.DEBUG
            self.logger.setLevel(self.level)
            handler = RotatingFileHandler(self.logfile, maxBytes=1048576, backupCount=10)  # set to 1MB per file
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        def info(self, message):
            self.logger.info(message)

        def exception(self, ex):
            self.logger.exception(ex)

        def error(self, message):
            self.logger.error(message)

        def warn(self, message):
            self.logger.warn(message)

        def _schedule_initialized(self, total_steps):
            """
            log schedule started
            """
            init_time_repr = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
            schedule = Schedule(1, total_steps, init_time_repr)
            self.info('initiated new %s' % schedule)

        def schedule_step(self, step_num, total_steps, date_end=None):
            """
            log schedule step up
            """
            if step_num == 1:
                return self._schedule_initialized(total_steps)
            # else proceed
            init_time_repr = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
            if not date_end:
                schedule = Schedule(step_num, total_steps, init_time_repr)
                self.info('step up %s' % schedule)
            else:
                end_time_repr = datetime.strftime(date_end, '%Y-%m-%d %H:%M:%S')
                schedule = Schedule(step_num, total_steps, init_time_repr, end_time_repr)
                self.info('finalized %s' % schedule)

    # storage for the instance reference
    __instance = None

    def __init__(self, level=None, filename=None):
        """ Create singleton instance """
        # Check whether we already have an instance
        if DITLogger.__instance is None:
            # Create and remember instance
            DITLogger.__instance = DITLogger.__impl(level, filename)

        # Store instance reference as the only member in the handle
        self.__dict__['_Singleton__instance'] = DITLogger.__instance

    def __getattr__(self, attr):
        """ Delegate access to implementation """
        return getattr(self.__instance, attr)

    def __setattr__(self, attr, value):
        """ Delegate access to implementation """
        return setattr(self.__instance, attr, value)
