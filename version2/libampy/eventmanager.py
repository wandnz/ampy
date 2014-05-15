import datetime

from libampy.database import AmpyDatabase
from libnntscclient.logger import *

class EventManager(object):
    """
    Class for interacting with the netevmon event database

    API Functions
    -------------
    fetch_events: 
        fetch all events that occurred in a time period for a given set of
        stream labels
    fetch_groups:
        fetch all event groups that were observed in a given time period
    fetch_event_group_members:
        fetch all events that belong to a specific event group

    """

    def __init__(self, eventdbconfig):
        """ 
        Init function for the EventManager class

        Parameters:
          eventdbconfig -- dictionary containing configuration parameters that
                describe how to connect to the event database. See
                the AmpyDatabase class for details on the possible parameters
        """
    
        # Default database name is netevmon
        if 'name' not in eventdbconfig:
            viewdbconfig['name'] = "netevmon"

        self.dbconfig = eventdbconfig
        self.db = AmpyDatabase(eventdbconfig, False)
        self.db.connect(15)

    def fetch_events(self, labels, start, end):
        """ 
        Fetches all events for a given set of labels that occurred within a
        particular time period.

        This method is used to acquire the events that need to be shown on
        a graph.

        Parameters:
          labels -- a list of dictionaries, where each dict describes a label:
                a set of one or more streams that share common group properties.
                The dictionary must contain a 'streams' element which is a
                list of stream ids for the label.
          start -- the timestamp at the start of the time period of interest
          end -- the timestamp at the end of the time period of interest

        Returns:
          a list of events that were detected between 'start' and 'end' for
          all streams that belong to a label in the label list.
          Returns None if a major error occurs.
        """

        query = """SELECT * FROM full_event_group_view 
                   WHERE timestamp >= %s AND timestamp <= %s AND stream_id IN (
                """

        first = True
        streamslist = []
        
        # Construct our query by adding every stream id in the labels dict
        # to our IN clause
        for lab in labels:
            if 'streams' not in lab:
                log("Error while fetching events: label has no associated streams")
                return None

            for s in lab['streams']:
                # Don't put a comma in front of the first stream id!
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
        """
        Fetches all of the event groups that were observed during the given
        time period.

        Used to populate the event group list on the dashboard.

        Parameters:
          start -- the timestamp at the start of the time period of interest
          end -- the timestamp at the end of the time period of interest

        Returns:
          a list of event groups or None if there is an error while querying
          the event database.
        """

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
        """
        Fetches the events that belong to a specific event group.

        Used to populate the event list when a user clicks on an event
        group on the dashboard.

        Parameters:
          groupid -- the unique id of the event group

        Returns:
          a list of events or None if there was an error while querying the
          event database.
        """
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
