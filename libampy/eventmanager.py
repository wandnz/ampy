#
# This file is part of ampy.
#
# Copyright (C) 2013-2017 The University of Waikato, Hamilton, New Zealand.
#
# Authors: Shane Alcock
#          Brendon Jones
#
# All rights reserved.
#
# This code has been developed by the WAND Network Research Group at the
# University of Waikato. For further information please see
# http://www.wand.net.nz/
#
# ampy is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.
#
# ampy is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with ampy; if not, write to the Free Software Foundation, Inc.
# 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#
# Please report any bugs, questions or comments to contact@wand.net.nz
#

from threading import Lock
from libampy.database import AmpyDatabase
from libnntscclient.logger import log

class EventManager(object):
    """
    Class for interacting with the netevmon event database

    API Functions
    -------------
    fetch_specific_event:
        fetches a single known event from the database
    fetch_events:
        fetch all events that occurred in a time period for a given set of
        stream labels
    fetch_groups:
        fetch all event groups that were observed in a given time period
    fetch_event_group_members:
        fetch all events that belong to a specific event group
    get_event_filter:
        Fetches a specific event filter from the views database.
    create_event_filter:
        Inserts a new event filter into the views database.
    delete_event_filter:
        Removes an event filter from the views database.
    update_event_filter:
        Replaces an existing event filter with a new set of filtering options.

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
        if eventdbconfig is None:
            self.disabled = True
            return
        else:
            self.disabled = False

        if 'name' not in eventdbconfig:
            eventdbconfig['name'] = "netevmon"

        self.dbconfig = eventdbconfig
        self.db = AmpyDatabase(eventdbconfig, True)
        self.db.connect(15)
        self.dblock = Lock()

    def fetch_specific_event(self, stream, eventid):
        """
        Fetches a specific event in the database, given the stream ID and the
        event ID.

        Parameters:
          stream -- the stream that the requested event belongs to
          eventid -- the ID number of the requested event

        Returns:
          a dictionary describing the event in question, or None if an error
          occurs or no such event exists.
        """

        if self.disabled:
            return None

        self.dblock.acquire()

        stable = "eventing.events_str%s" % (stream)
        query = "SELECT * FROM " + stable
        query += " WHERE event_id = %s"
        params = (eventid,)

        if self.db.executequery(query, params) == -1:
            log("Error while querying for a specific event (%s %s)" % \
                    (stream, eventid))
            self.dblock.release()
            return None

        result = self.db.cursor.fetchone()
        self.dblock.release()
        return dict(result)

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
        if self.disabled:
            return events
        self.dblock.acquire()
        for lab in labels:
            if 'streams' not in lab:
                log("Error while fetching events: label has no associated streams")
                return None

            for stream in lab['streams']:

                query = "SELECT count(*) FROM eventing.group_membership WHERE"
                query += " stream = %s"
                params = (stream,)

                if self.db.executequery(query, params) == -1:
                    log("Error while querying for events")
                    self.dblock.release()
                    return None

                if self.db.cursor.fetchone()[0] == 0:
                    continue

                stable = "eventing.events_str%s" % (stream)
                query = "SELECT * FROM " + stable
                query += " WHERE ts_started >= %s AND ts_started <= %s"

                params = (start, end)

                if self.db.executequery(query, params) == -1:
                    log("Error while querying for events")
                    self.dblock.release()
                    return None

                for row in self.db.cursor.fetchall():
                    events.append(dict(row))
                    events[-1]['stream'] = stream

                    if 'groupid' in lab:
                        events[-1]['groupid'] = lab['groupid']
                    else:
                        events[-1]['groupid'] = None

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

        if self.disabled:
            return []

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

        if self.disabled:
            return []

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

    def get_event_filter(self, username, filtername):
        """
        Fetches the event filter that matches a given user, filtername
        combination.

        Parameters:
          username -- the user that is requesting the event filter
          filtername -- the name of the filter to be fetched

        Returns:
          The row in the event filter table that matches the given username
          and filter name, or None if no such filter exists (or an error
          occurs while querying the database).
        """
        if self.disabled:
            return None

        query = """SELECT * FROM eventing.userfilters WHERE user_id=%s AND filter_name=%s"""
        params = (username, filtername)

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while searching for event filter")
            self.dblock.release()
            return None

        # Ideally, this shouldn't happen but let's try and do something
        # sensible if it does
        if self.db.cursor.rowcount > 1:
            log("Warning: multiple event filters match the description %s %s" % (username, filtername))
            log("Using first instance")

        if self.db.cursor.rowcount == 0:
            self.dblock.release()
            return None

        filterdata = self.db.cursor.fetchone()
        self.db.closecursor()
        self.dblock.release()
        return filterdata

    def create_event_filter(self, username, filtername, filterstring):
        """
        Inserts a new event filter into the filter table.

        Parameters:
          username -- the user who the new filter belongs to.
          filtername -- the name to be associated with this new filter.
          filterstring -- a string containing stringified JSON that describes
                          the filter options.

        Returns:
          the tuple (username, filtername) if the new filter is successfully
          inserted into the filter table, or None if the insertion fails.
        """
        if self.disabled:
            return None

        query = """INSERT INTO eventing.userfilters (user_id, filter_name, filter) VALUES (%s, %s, %s) """
        params = (username, filtername, filterstring)

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while inserting new event filter")
            self.dblock.release()
            return None
        self.dblock.release()
        return username, filtername

    def update_event_filter(self, username, filtername, filterstring, email):
        """
        Replaces the filter string for an existing event filter.

        Parameters:
          username -- the user who the updated filter belongs to.
          filtername -- the name of the filter to be updated.
          filterstring -- a string containing stringified JSON that describes
                          the new filter options.

        Returns:
          the tuple (username, filtername) if the filter is successfully
          updated, or None if the filter doesn't exist or the update fails.
        """
        if self.disabled:
            return None

        query = """ UPDATE eventing.userfilters SET filter = %s, email = %s
                    WHERE user_id=%s AND filter_name=%s """
        params = (filterstring, email, username, filtername)
        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while updating event filter")
            self.dblock.release()
            return None
        self.dblock.release()
        return username, filtername

    def delete_event_filter(self, username, filtername=None):
        """
        Removes an existing event filter from the filter table.

        Parameters:
          username -- the user who owns the filter to be removed.
          filtername -- the name of the filter to be removed

        Returns:
          the tuple (username, filtername) if the filter is successfully
          removed from the filter table, or None if the removal fails.
        """
        if self.disabled:
            return username, filtername

        query = "DELETE FROM eventing.userfilters WHERE user_id=%s"
        params = [username]
        if filtername is not None:
            query += " AND filter_name=%s"
            params.append(filtername)
        self.dblock.acquire()
        if self.db.executequery(query, tuple(params)) == -1:
            log("Error while removing event filter")
            self.dblock.release()
            return None
        self.dblock.release()
        return username, filtername

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
