#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
depends: psycopg2
to install psycopg2m do
sudo pip install psycopg2
- if no postgres instance is installed in the server, then the above command will fail.
  All you need to do is
  sudo apt-get install libpq-dev
    and then rerun
    sudo pip install psycopg2
"""
__author__ = 'George Kiomourtzis <gkiom@iit.demokritos.gr>'

import sys
import os

import psycopg2


class DBAccess:
    ids_in_db = {}  # the ids to get
    db_host = ""
    db_user = ""
    db_pw = ""
    db_name = ""

    def __init__(self, db_host=None, db_user=None, db_pw=None, db_name=None):
        if not db_name:
            self.db_name = "democracit"
        else:
            self.db_name = db_name
        # get variables
        self._get_variables(db_host, db_user, db_pw)

    def _get_variables(self, db_host, db_user, db_pw):
        """
        get the variables, if none, from file (backup)
        file lines must be in the order of db_host, db_user, db_pw :-)
        """
        # backup (pycharm no see getenv)
        # read from file (TODO add settings file to gitignore)
        with open("../settings.properties", 'r') as f:
            settings = f.readlines()

        if not db_host:
            self.db_host = settings[0].split("=")[1].strip()
        else:
            self.db_host = db_host
        if not db_user:
            self.db_user = settings[1].split("=")[1].strip()
        else:
            self.db_user = db_user
        if not db_pw:
            self.db_pw = settings[2].split("=")[1].strip()
        else:
            self.db_pw = db_pw

    def get_consultation_ids(self):
        """
        return a list of consultation IDs
        """
        con = None
        try:
            # get a db connection
            con = self.get_connection()
            # get a cursor
            cur = con.cursor()
            # query db (get consultations that contain comments)
            cur.execute(
                "SELECT distinct(consultation.id) "
                "FROM consultation "
                "INNER JOIN articles ON articles.consultation_id = consultation.id "
                "INNER JOIN comments ON comments.article_id = articles.id "
                "ORDER BY consultation.id DESC;")
            # get all results at once
            consultations = cur.fetchall()
            # get a set of all the consultation IDs
            self.ids_in_db = {each[0] for each in consultations if isinstance(each, tuple)}
            # respond
            return self.ids_in_db
        except psycopg2.DatabaseError, e:
            if con:
                con.rollback()
            print 'Error %s' % e
            sys.exit(1)
        finally:
            if con:
                con.close()

    def get_connection(self):
        con = None
        try:
            con = psycopg2.connect(host=os.getenv("democracit_db_host", self.db_host),
                                   dbname=self.db_name,
                                   user=os.getenv("democracit_db_user", self.db_user),
                                   password=os.getenv("democracit_db_pw", self.db_pw))
            return con
        except psycopg2.DatabaseError, e:
            if con:
                con.rollback()
            print 'Error %s' % e
            sys.exit(1)


if __name__ == "__main__":
    dba = DBAccess()
    for each in dba.get_consultation_ids():
        print each, type(each)
