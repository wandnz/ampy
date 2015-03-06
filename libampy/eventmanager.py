import datetime

from libampy.database import AmpyDatabase
from libnntscclient.logger import *
from threading import Lock

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
            eventdbconfig['name'] = "netevmon"

        self.dbconfig = eventdbconfig
        self.db = AmpyDatabase(eventdbconfig, False)
        self.db.connect(15)
        self.dblock = Lock()

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

        events = []
        self.dblock.acquire()
        for lab in labels:
            if 'streams' not in lab:
                log("Error while fetching events: label has no associated streams")
                return None
          
            for s in lab['streams']:
                
                query = "SELECT count(*) FROM eventing.group_membership WHERE"
                query += " stream = %s"
                params = (s,)

                if self.db.executequery(query, params) == -1:
                    log("Error while querying for events")
                    self.dblock.release()
                    return None
               
                if self.db.cursor.fetchone()[0] == 0:
                    continue 
                
                stable = "eventing.events_str%s" % (s)
                query = "SELECT * FROM " + stable
                query += " WHERE ts_started >= %s AND ts_started <= %s"
                
                params = (start, end) 

                if self.db.executequery(query, params) == -1:
                    log("Error while querying for events")
                    self.dblock.release()
                    return None

                for row in self.db.cursor.fetchall():
                    events.append(dict(row))
                    events[-1]['stream'] = s

                self.db.closecursor()

        self.dblock.release()
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

        #start_dt = datetime.datetime.fromtimestamp(start)
        #end_dt = datetime.datetime.fromtimestamp(end)

        query = """SELECT * FROM eventing.groups WHERE ts_started >= %s
                   AND ts_ended <= %s ORDER BY ts_started
                """
        params = (start, end)
        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while querying event groups")
            self.dblock.release()
            return None
        
        groups = []
        
        for row in self.db.cursor.fetchall():
            groups.append(dict(row))
        self.db.closecursor() 
        self.dblock.release()
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
        query = """SELECT * FROM eventing.group_membership
                   WHERE group_id=%s
                """

        params = (str(groupid), )
        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while querying event group membership")
            self.dblock.release()
            return None

        events = []
        members = self.db.cursor.fetchall()
        self.db.closecursor()

        for row in members:
            # Now fetch the events within that group
            stream = row[2]
            evid = row[1]
            colname = row[3]

            query = "SELECT * from eventing.events_str%s" % (str(stream))
            query += " WHERE event_id=%s"
            params = (str(evid),) 
            
            if self.db.executequery(query, params) == -1:
                log("Error while querying for event group member (%s,%s)" % \
                        (str(stream), str(evid)))
                self.dblock.release()
                return None

            evrow = self.db.cursor.fetchone()
            events.append(dict(evrow))
            events[-1]['stream'] = stream
            events[-1]['collection'] = colname

        self.db.closecursor() 
        self.dblock.release()
        return sorted(events, key=lambda s: s['ts_started'])


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
