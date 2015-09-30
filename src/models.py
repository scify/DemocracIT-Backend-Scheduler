#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = 'George K. <gkiom@scify.org>'

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Date, ForeignKey

Base = declarative_base()


class Schedule(Base):  # not used yet, in data model, but only for logging
    __tablename__ = 'schedules'

    id = Column(Integer, primary_key=True, nullable=False)
    status_step = Column(
        Integer)  # increment id of module called (e.g. 1 of 10 if crawler runs and other 9 modules wait)
    total_steps = Column(Integer)
    date_init = Column(Date)
    date_end = Column(Date)

    def __init__(self, status, total_steps, date_init, date_end=None):
        self.status_step = status
        self.total_steps = total_steps
        self.date_init = date_init
        self.date_end = date_end

    def __repr__(self):
        return "<Schedule:('%s', '%s', '%s', '%s')>" % (
            self.status_step, self.total_steps, self.date_init, self.date_end)


class CommentsHistory(Base):
    __tablename__ = 'comment_ids'

    pipeline_id = Column(Integer, ForeignKey('schedules.id'), primary_key=True)
    last_commend_id = Column(Integer)

    def __init__(self, pipeline_id, comment_id):
        self.pipeline_id = pipeline_id
        self.last_commend_id = comment_id

    def __repr__(self):
        return "<CommentID:('%s', '%s')>" % (self.pipeline_id, self.last_commend_id)