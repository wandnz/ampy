from libampy.database import AmpyDatabase
from libnntscclient.logger import *
from threading import Lock

class ViewManager(object):
    """
    Class for interacting with the views database.

    A view is a unique identifier for a particular graph and consists
    of one or more stream groups. 
    
    A stream group describes a line or a set of lines to be drawn on a 
    graph which have many stream properties in common. In many cases, a
    stream group will aggregate together multiple streams into a single
    line, e.g. all amp-icmp streams for a source-dest pair where the 
    target address was an IPv4 address. An aggregation method may also
    combine all results into a single line or create one line per stream.
    Each line for a stream group will consist of one or more streams.

    Some notes on collections:
      The terminology here can get slightly confusing, now that a view
      contain groups from multiple collections.

      Stream group collections are simple -- they describe the collection
      that all of the streams in the group belong to and therefore the
      module that must be used to operate on that group. There is a clear
      one-to-one mapping of group collection to collection module.
      Groups from the amp-icmp collection use the amp-icmp module, for 
      instance.

      However, it makes sense for some collections to be shown on the
      same graph. For example, amp-icmp, amp-dns and amp-tcpping are
      all latency measurements and are therefore directly comparable. 
      In this case, we use 'amp-latency' to describe a view that can
      consist of groups from any available latency collection. However,
      there is no 'amp-latency' module in ampy -- each group is still
      processed using its own collection module.

      Wherever possible, we'll try to use the term 'viewstyle' to refer
      to the view-level collection, e.g. 'amp-latency' for latency views,
      and 'collection' to refer to the group collection.

    The views database is also used to store event filters for use with the
    event dashboard. Event filters allow users to control which events
    appear on their dashboard, e.g. removing events for a particular target
    that they do not care about.

    API Functions
    -------------
      get_view_groups:
        Returns the description strings for all groups that belong to a view.
      get_group_id:
        Searches for a group that matches a given description. If one does 
        not exist, a new group is created.
      get_view_id
        Searches for a view that contains a given set of groups. If one does 
        not exist, a new view is created.
      add_groups_to_view:
        Returns the id of the view that results from adding new groups to
        an existing view.
      remove_group_from_view:
        Returns the id of the view that results from removing a group from an
        existing view.
      get_event_filter:
        Fetches a specific event filter from the views database.
      create_event_filter:
        Inserts a new event filter into the views database.
      delete_event_filter:
        Removes an event filter from the views database.
      update_event_filter:
        Replaces an existing event filter with a new set of filtering options.

    """

    def __init__(self, viewdbconfig):
        """
        Init function for the ViewManager class.
        
        Parameters:
          viewdbconfig -- dictionary containing configuration parameters
                          describing how to connect to the views database.
                          See the AmpyDatabase class for details on possible
                          configuration parameters.
        """
        
        # Use 'views' as the default database name
        if 'name' not in viewdbconfig:
            viewdbconfig['name'] = "views"

        self.dbconfig = viewdbconfig
        self.db = AmpyDatabase(viewdbconfig, True)
        self.db.connect(15)
        self.dblock = Lock()

    def get_view_groups(self, viewstyle, viewid):
        """
        Queries the views database to find the set of groups that belong 
        to a given view.

        Parameters:
          collection -- the name of the collection that the view belongs to
          viewid -- the id number of the view

        Returns:
          a dictionary containing the groups that are part of the given view,
          broken down by the group collection.
          The dictionary keys are the collection names and the values
          are tuples containing the group id and the group description
          string.
          Will return None if the query fails.
        """
        groups = {}

        if viewid == 0:
            return groups

        query = """SELECT collection, group_id, group_description FROM 
                groups WHERE group_id IN (SELECT unnest(view_groups)
                FROM views WHERE collection=%s AND view_id=%s) """
        params = (viewstyle, viewid)

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while fetching the groups for a view")
            self.dblock.release()
            return None

        # No groups matched this view
        if self.db.cursor.rowcount == 0:
            self.db.closecursor()
            self.dblock.release()
            return groups

        for row in self.db.cursor.fetchall():
            if row['collection'] in groups:
                groups[row['collection']].append( \
                    (row['group_id'], row['group_description']))
            else:
                groups[row['collection']] = \
                    [(row['group_id'], row['group_description'])]

        
        self.db.closecursor()
        self.dblock.release()
        return groups

    def get_group_id(self, collection, description):
        """
        Queries the views database for a group that matches the given
        description. If a matching group does not exist, one is created.

        Parameters:
          collection -- the collection that the group belongs to
          description -- a string describing the group

        Returns:
          the ID number of the group that has a description matching the
          one that was provided or None if there was a database error.


        """

        query = """SELECT group_id FROM groups WHERE collection=%s AND 
                group_description=%s"""
        params = (collection, description)

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while checking if group exists")
            self.dblock.release()
            return None

        # Ideally, this shouldn't happen but let's try and do something
        # sensible if it does
        if self.db.cursor.rowcount > 1:
            log("Warning: multiple groups match the description %s %s" % (collection, description))
            log("Using first instance")

        if self.db.cursor.rowcount == 0:
            # No groups found that matched the description, so create a
            # a new group and return its id
            query = """INSERT INTO groups (collection, group_description) 
                    VALUES (%s, %s) RETURNING group_id
                    """
            if self.db.executequery(query, params) == -1:
                log("Error while inserting new group")
                self.dblock.release()
                return None

        group_id = self.db.cursor.fetchone()['group_id']
        self.db.closecursor()
        self.dblock.release()
        return group_id

        
    def get_view_id(self, viewstyle, groups):
        """
        Queries the views database for a view that contains the given
        set of groups. If a matching view does not exist, one is created.

        Parameters:
          viewstyle -- the collection that the view belongs to
          groups -- a list of group IDs to query for

        Returns:
          the ID number of the view that consists of the groups that were
          provided or None if there was a database error.


        """
        # Create view if it doesn't exist
        query = """SELECT view_id FROM views WHERE collection=%s AND 
                view_groups=%s"""
        params = (viewstyle, groups)

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while checking if view exists")
            self.dblock.release()
            return None

        # Ideally, this shouldn't happen but let's try and do something
        # sensible if it does
        if self.db.cursor.rowcount > 1:
            log("Warning: multiple views match in collection %s, %s" % (viewstyle, description))
            log("Using first instance")

        if self.db.cursor.rowcount == 0:
            # No groups found that matched the description, so create a
            # a new group and return its id
            query = """INSERT INTO views (collection, view_groups) 
                    VALUES (%s, %s) RETURNING view_id
                    """
            if self.db.executequery(query, params) == -1:
                log("Error while inserting new view")
                self.dblock.release()
                return None

        view_id = self.db.cursor.fetchone()['view_id']
        self.db.closecursor()
        self.dblock.release()
        return view_id

    def add_groups_to_view(self, viewstyle, collection, viewid, descriptions):
        """
        Adds new groups to an existing view and returns the ID of the
        modified view.

        Parameters:
          viewstyle -- the collection that the existing view belongs to
          collection -- the collection that the new groups belong to
          viewid -- the ID number of the view being modified. A view id of
                    zero represents an empty view (i.e. with no groups)
          descriptions -- a list of strings describing the groups to be 
                          added to the view

        Returns:
          the ID of the view that results from adding the group to the
          existing view. Returns the original view ID if the view is
          unchanged. Returns None if a database error occurred while
          modifying the view.

        """
        # First, find all the groups for the original view
        groups = self.get_view_groups(viewstyle, viewid)
        if groups is None:
            return None

        existing = []
        for col, vgs in groups.iteritems():
            existing += [x[0] for x in vgs]

        for d in descriptions:

            # Find the group ID for the group we are about to add
            groupid = self.get_group_id(collection, d)
            if groupid is None:
                return None

            # Always keep our groups in sorted order, as this makes it much
            # easier to query the views table later on
            if groupid not in existing:
                existing.append(groupid)
                existing.sort()
        
        # Work out the view id for the new set of groups
        newview = self.get_view_id(viewstyle, existing)
        if newview is None:
            return None
        return newview


    def remove_group_from_view(self, viewstyle, viewid, groupid):
        """
        Removes a group from an existing view and returns the ID of the
        modified view.

        Parameters:
          viewstyle -- the view collection that the view belongs to
          viewid -- the ID number of the view being modified. A view id of
                    zero represents an empty view (i.e. with no groups)
          groupid -- the ID number of the group to be removed

        Returns:
          the ID of the view that results from removing the group from the
          existing view. Returns 0 if there are no groups left in the view
          after the removal and returns the original view ID if the group
          is not present in the view.

          Returns None if a database error occurs.
        """

        # First, find all the groups that belong to the original view
        groups = self.get_view_groups(viewstyle, viewid)
        if groups is None:
            return None
        existing = []
        for col, vgs in groups.iteritems():
            existing += [x[0] for x in vgs]

        # Remove the group from the group list if present
        if groupid in existing:
            existing.remove(groupid)
        else:
            return viewid

        # If the view is now empty, return 0 to indicate no active groups
        if len(existing) == 0:
            return 0

        # Work out the view id for the new set of groups
        newview = self.get_view_id(viewstyle, existing)
        if newview is None:
            return None
        return newview

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
        query = """SELECT * FROM userfilters WHERE user_id=%s AND filter_name=%s"""
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
        query = """INSERT INTO userfilters (user_id, filter_name, filter) VALUES (%s, %s, %s) """
        params = (username, filtername, filterstring)

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while inserting new event filter")
            self.dblock.release()
            return None
        self.dblock.release()
        return username, filtername

    def update_event_filter(self, username, filtername, filterstring):
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
        query = """UPDATE userfilters SET filter = %s WHERE user_id=%s AND filter_name=%s"""
        params = (filterstring, username, filtername)
        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while updating event filter")
            self.dblock.release()
            return None
        self.dblock.release()
        return username, filtername

    def delete_event_filter(self, username, filtername):
        """
        Removes an existing event filter from the filter table.

        Parameters:
          username -- the user who owns the filter to be removed.
          filtername -- the name of the filter to be removed

        Returns:
          the tuple (username, filtername) if the filter is successfully
          removed from the filter table, or None if the removal fails.
        """
        query = """DELETE FROM userfilters WHERE user_id=%s AND filter_name=%s"""
        params = (username, filtername)
        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while removing event filter")
            self.dblock.release()
            return None
        self.dblock.release()
        return username, filtername

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
