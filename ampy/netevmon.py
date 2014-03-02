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


    def __init__(self, host=None, name="events", pwd=None, user=None):
        cstring = "dbname=%s" % (name)
        if host != "" and host != None:
            cstring += " host=%s" % (host)
        if user != "" and user != None:
            cstring += " user=%s" % (user)
        if pwd != "" and pwd != None:
            cstring += " password=%s" % (pwd)

        self.datacursor = None
        self.cstring = cstring
        self.cursorname = 'ampy_%030x' % random.randrange(16**30)
        self.connect()

    def connect(self):
        try:
            self.conn = psycopg2.connect(self.cstring)
        except psycopg2.DatabaseError as e:
            print >> sys.stderr, "Error connecting to event database:", e
            self.conn = None
            return -1

        if self.datacursor is not None:
            self._reset_cursor()
        return 0

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

    def _execute_netevmon_query(self, sql, params):
        retried = False
        if self.conn == None:
            if self.connect() == -1:
                return None

        self._reset_cursor()
        if self.datacursor == None:
            return None

        while 1:
            try:
                result = self.datacursor.execute(sql, params)
                break
            except psycopg2.OperationalError as e:
                if not retried:
                    if self.connect() == -1:
                        return None
                    retried = True
                    continue
                return None

        return result


    def get_stream_events(self, stream_ids, start=None, end=None):
        """Fetches all events for a given stream between a start and end
           time. Events are returned as a Result object."""
        # Honestly, start and end should really be set by the caller
        if end is None:
            end = int(time.time())

        if start is None:
            start = end - (12 * 60 * 60)

        selclause = "SELECT * FROM full_event_group_view "

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

        orderclause = " ORDER BY timestamp, group_id "
        sql = selclause + whereclause + orderclause

        params = tuple([start] + [end] + stream_ids)

        result = self._execute_netevmon_query(sql, params)
        if result == None:
            return ampy.result.Result([])

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

        sql = "SELECT * FROM full_event_group_view WHERE group_id=%s ORDER BY timestamp"
        params = (str(group_id),)
        result = self._execute_netevmon_query(sql, params)
        if result == None:
            return ampy.result.Result([])

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

        sql = "SELECT * FROM event_group WHERE group_start_time >= %s AND group_end_time <= %s ORDER BY group_start_time"
        params = (start_dt, end_dt)
        result = self._execute_netevmon_query(sql, params)
        if result == None:
            return ampy.result.Result([])

        eventlist = []
        while True:
            fetched = self.datacursor.fetchmany(200)
            if fetched == []:
                break
            eventlist += fetched

        return ampy.result.Result(eventlist)


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
