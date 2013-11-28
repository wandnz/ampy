#!/usr/bin/env python

"""
Connects to the event database produced by netevmon and queries it for
events.
"""

import datetime
import time
import sys
import ampy.result
import random

import psycopg2
import psycopg2.extras

try:
    import pylibmc
    _have_memcache = True
except ImportError:
    _have_memcache = False

class Connection(object):

    def __reflect_db(self):
        self.metadata = MetaData(self.engine)
        try:
            self.metadata.reflect(bind=self.engine)
        except OperationalError, e:
            print >> sys.stderr, "Error binding to database %s" % (dbname)
            print >> sys.stderr, "Are you sure you've specified the right database name?"
            sys.exit(1)

        # reflect() is supposed to take a 'views' argument which will
        # force it to reflects views as well as tables, but our version of
        # sqlalchemy didn't like that. So fuck it, I'll just reflect the
        # views manually
        views = self.inspector.get_view_names()
        for v in views:
            view_table = Table(v, self.metadata, autoload=True)


    def __init__(self, host=None, name="events", pwd=None, user=None):
        cstring = "dbname=%s" % (name)
        if host != "" and host != None:
            cstring += " host=%s" % (host)
        if user != "" and user != None:
            cstring += " user=%s" % (user)
        if pwd != "" and pwd != None:
            cstring += " password=%s" % (pwd)

        self.datacursor = None

        try:
            self.conn = psycopg2.connect(cstring)
        except psycopg2.DatabaseError as e:
            print >> sys.stderr, "Error connecting to event database:", e
            self.conn = None
            return

        self.cursorname = 'ampy_%030x' % random.randrange(16**30)

    def _reset_cursor(self):
        if self.conn == None:
            return

        if self.datacursor:
            self.datacursor.close()

        try:
            self.datacursor = self.conn.cursor(self.cursorname,
                    cursor_factory = psycopg2.extras.DictCursor)
        except psycopg2.DatabaseError as e:
            print >> sys.stderr, "Failed to create event data cursor:", e
            self.datacursor = None


    def __del__(self):
        if self.datacursor:
            self.datacursor.close()
        if self.conn:
            self.conn.close()

    def get_stream_events(self, stream_ids, start=None, end=None):
        """Fetches all events for a given stream between a start and end
           time. Events are returned as a Result object."""
        # Honestly, start and end should really be set by the caller
        if end is None:
            end = int(time.time())

        if start is None:
            start = end - (12 * 60 * 60)

        self._reset_cursor()
        if self.datacursor == None:
            return ampy.result.Result([])

        selclause = "SELECT * FROM event_view "

        whereclause = "WHERE timestamp >= %s AND timestamp <= %s "
        # iterate over all stream_ids and fetch all events
        if len(stream_ids) != 0:
            streamclause = "AND ("
            for i in range(0, len(stream_ids)):
                streamclause += "stream_id = %s"
                if i != len(stream_ids) - 1:
                    streamclause += " OR "
            streamclause += ")"
        whereclause += streamclause

        orderclause = " ORDER BY timestamp, stream_id "
        sql = selclause + whereclause + orderclause

        params = tuple([start] + [end] + stream_ids)

        self.datacursor.execute(sql, params)

        eventlist = []
        while True:
            fetched = self.datacursor.fetchmany(200)
            if fetched == []:
                break
            eventlist += fetched

        return ampy.result.Result(eventlist)


    def get_events_in_group(self, group_id):
        """Fetches all of the events belonging to a specific event group.
           The events are returned as a Result object."""
        self._reset_cursor()
        if self.datacursor == None:
            return ampy.result.Result([])

        sql = "SELECT * FROM full_event_group_view WHERE group_id=%s ORDER BY timestamp"
        self.datacursor.execute(sql, (str(group_id),))

        eventlist = []
        while True:
            fetched = self.datacursor.fetchmany(200)
            if fetched == []:
                break
            eventlist += fetched

        return ampy.result.Result(eventlist)

    def get_event_groups(self, start=None, end=None):
        """Fetches all of the event groups between a start and end time.
           The groups are returned as a Result object."""
        if end is None:
            end = int(time.time())

        if start is None:
            start = 0

        start_dt = datetime.datetime.fromtimestamp(start)
        end_dt = datetime.datetime.fromtimestamp(end)

        self._reset_cursor()
        if self.datacursor == None:
            return ampy.result.Result([])

        sql = "SELECT * FROM event_group WHERE group_start_time >= %s AND group_end_time <= %s ORDER BY group_start_time"
        self.datacursor.execute(sql, (start_dt, end_dt))

        eventlist = []
        while True:
            fetched = self.datacursor.fetchmany(200)
            if fetched == []:
                break
            eventlist += fetched

        return ampy.result.Result(eventlist)


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
