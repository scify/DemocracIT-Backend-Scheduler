#!/usr/bin/python
# -*- coding: utf-8 -*-
from src.dbaccess.psql_dbaccess import DBAccess

__author__ = 'George K. <gkiom@scify.org>'

import sqlite3


class SQLiteDBAccess(DBAccess):
    db_name = ""
    consultations_to_run = {}
    prev_latest_comment_id = 0  # the latest comment id (0 for the first run)

    def __init__(self, db_name=None):
        """

        :type db_name: basestring
        """
        if not db_name:
            self.db_name = "democracit"
        else:
            self.db_name = db_name

    def get_updated_consultations(self):
        """
        get latest comment id from parent class, store in DB and init run
        """
        self.prev_latest_comment_id = self.get_latest_comment_id()
        self.consultations_to_run = self.get_consultation_ids()


    def get_connection(self):
        con = None
        try:
            con = sqlite3.connect(self.db_name)
            return con
        except sqlite3.Error, e:
            if con:
                con.rollback()
            print 'Error %s' % e


if __name__ == "__main__":
    dba = SQLiteDBAccess()
    # for each in dba.get_latest_comment_id():
    # print each, type(each)
