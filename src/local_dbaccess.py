#!/usr/bin/python
# -*- coding: utf-8 -*-
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from models import Schedule, Base

import sqlite3
from datetime import datetime
import traceback

__author__ = 'George K. <gkiom@scify.org>'


class LocalDBAccess:
    """
    @deprecated
    """
    db_name = ""

    def __init__(self, db_name=None):
        """
        :type db_name: String
        """
        # if not db_name:
        #     self.db_name = "democracit"
        # else:
        #     self.db_name = db_name
        # self._init_tables()
        pass

    def _schedule_initialized(self, total_steps):
        """ add a record to the DB to show that schedule is initialized
        """
        init_time_repr = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
        schedule = Schedule(1, total_steps, init_time_repr)
        print 'initiated new %s' % schedule

    def schedule_step(self, step_num, total_steps, date_end=None):
        """ add a record to the DB to show that schedule is initialized
        """
        if step_num == 1:
            return self._schedule_initialized(total_steps)
        # else proceed
        init_time_repr = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
        if not date_end:
            schedule = Schedule(step_num, total_steps, init_time_repr)
        else:
            end_time_repr = datetime.strftime(date_end, '%Y-%m-%d %H:%M:%S')
            schedule = Schedule(step_num, total_steps, init_time_repr, end_time_repr)
        print 'step up %s' % schedule

    def get_connection(self):
        con = None
        try:
            con = sqlite3.connect(self.db_name)
            return con
        except sqlite3.Error, e:
            if con:
                con.rollback()
            print 'Exception: %s :\n %s' % (e, traceback.format_exc())

    def _init_tables(self):
        con = None
        try:
            self.db = create_engine('sqlite:///' + self.db_name)
            Base.metadata.create_all(self.db)
            # get a db connection
            con = self.get_connection()
            # get a cursor
            cur = con.cursor()
            # query db (get latest comment ID)
            cur.execute\
                ("""
                CREATE TABLE IF NOT EXISTS schedules(
                   id INTEGER PRIMARY KEY   AUTOINCREMENT,
                   start           LONG      NOT NULL,
                   end            LONG       NULL
                );""")
            init_time_repr = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
            cur.execute("INSERT INTO schedules start VALUES(%d)", (init_time_repr,))
        except SQLAlchemyError, e:
            if con:
                con.rollback()
            print 'Exception: %s :\n %s' % (e, traceback.format_exc())
        finally:
            if con:
                con.close()


if __name__ == "__main__":
    dba = LocalDBAccess()
    print dba._init_tables()