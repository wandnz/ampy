#!/usr/bin/env python

"""
Connects to the event database produced by netevmon and queries it for
events.
"""

import datetime
import time
import urllib2
import sys
import ampy.result

from sqlalchemy.sql import and_, or_, not_, text
from sqlalchemy.sql.expression import select, outerjoin, func, label
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.engine import reflection

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
        cstring = URL('postgresql', password=pwd, \
                host=host, database=name, username=user)

        self.engine = create_engine(cstring, echo=False)
        self.inspector = reflection.Inspector.from_engine(self.engine)
        self.__reflect_db()

        self.conn = self.engine.connect()

    def __del__(self):
        self.conn.close()

    def get_stream_events(self, stream_ids, start=None, end=None):
        """Fetches all events for a given stream between a start and end
           time. Events are returned as a Result object."""
        # Honestly, start and end should really be set by the caller
        if end is None:
            end = int(time.time())

        if start is None:
            start = end - (12 * 60 * 60)

        evtable = self.metadata.tables['event_view']

        # iterate over all stream_ids and fetch all events
        stream_str = "("
        index = 0
        for stream_id in stream_ids:
            stream_str += "%s = %s" % (evtable.c.stream_id, stream_id)
            index += 1
            # Don't put OR after the last stream!
            if index != len(stream_ids):
                stream_str += " OR "
        stream_str += ")"

        wherecl = "(%s >= %u AND %s <= %u AND %s)" % ( \
                evtable.c.timestamp, start, evtable.c.timestamp, \
                end, stream_str)

        query = evtable.select().where(wherecl).order_by(evtable.c.timestamp)
        return self.__execute_query(query)

    def __execute_query(self, query):

        res = query.execute()

        event_list = []

        for row in res:
            foo = {}
            for k,v in row.items():
                foo[k] = v
            event_list.append(foo)
        res.close()

        return ampy.result.Result(event_list)

    def get_events_in_group(self, group_id):
        """Fetches all of the events belonging to a specific event group.
           The events are returned as a Result object."""
        evtable = self.metadata.tables['full_event_group_view']

        wherecl = "(%s = %u)" % (evtable.c.group_id, group_id)

        query = evtable.select().where(wherecl).order_by(evtable.c.timestamp)
        return self.__execute_query(query)

    def get_event_groups(self, start=None, end=None):
        """Fetches all of the event groups between a start and end time.
           The groups are returned as a Result object."""
        if end is None:
            end = int(time.time())

        if start is None:
            start = 0

        start_dt = datetime.datetime.fromtimestamp(start)
        end_dt = datetime.datetime.fromtimestamp(end)

        grptable = self.metadata.tables['event_group']

        wherecl = and_(grptable.c.group_start_time >= start_dt, \
                grptable.c.group_end_time <= end_dt)

        query = grptable.select().where(wherecl).order_by(grptable.c.group_start_time)
        return self.__execute_query(query)


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
