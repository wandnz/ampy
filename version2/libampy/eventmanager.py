import datetime

from libampy.database import AmpyDatabase
from libnntscclient.logger import *

class EventManager(object):

    def __init__(self, eventdbconfig):

        if 'name' not in eventdbconfig:
            viewdbconfig['name'] = "netevmon"

        self.dbconfig = eventdbconfig
        self.db = AmpyDatabase(eventdbconfig, False)
        self.db.connect(15)

    def fetch_events(self, labels, start, end):
        query = """SELECT * FROM full_event_group_view 
                   WHERE timestamp >= %s AND timestamp <= %s AND stream_id IN (
                """

        first = True
        streamslist = []
        for lab in labels:
            if 'streams' not in lab:
                log("Error while fetching events: label has no associated streams")
                return None

            for s in lab['streams']:
                if first:
                    query += "%s"
                    first = False
                else:
                    query += ", %s"
                streamslist.append(s)

        if first:
            # No streams were added to our query
            log("Warning: requested events for a set of labels with no associated streams")
            return []
        
        query += ")"
        params = tuple([start, end] + streamslist)
        
        if self.db.executequery(query, params) == -1:
            log("Error while querying for events")
            return None
        
        events = []
        for row in self.db.cursor.fetchall():
            events.append(dict(row))
        self.db.closecursor() 
        return events



    def fetch_groups(self, start, end):
        start_dt = datetime.datetime.fromtimestamp(start)
        end_dt = datetime.datetime.fromtimestamp(end)

        query = """SELECT * FROM event_group WHERE group_start_time >= %s
                   AND group_end_time <= %s ORDER BY group_start_time
                """
        params = (start_dt, end_dt)
        if self.db.executequery(query, params) == -1:
            log("Error while querying event groups")
            return None
        
        groups = []
        
        for row in self.db.cursor.fetchall():
            groups.append(dict(row))
        self.db.closecursor() 
        return groups

    def fetch_event_group_members(self, groupid):
        query = """SELECT * FROM full_event_group_view
                   WHERE group_id=%s ORDER BY timestamp
                """

        params = (str(groupid), )
        if self.db.executequery(query, params) == -1:
            log("Error while querying event group members")
            return None

        events = []
        for row in self.db.cursor.fetchall():
            events.append(dict(row))
        self.db.closecursor() 
        return events

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
