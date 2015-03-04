#!/usr/bin/python
# -*- coding: utf-8 -*-
from src.dbaccess.sqlite_dbaccess import SQLiteDBAccess

__author__ = 'George K. <gkiom@scify.org>'

# init sqlite storage
local_store = SQLiteDBAccess()
# get latest comment stored by the crawler in the previous run
prev_latest_comment_id = local_store.get_latest_comment_id()
# initiate crawler
