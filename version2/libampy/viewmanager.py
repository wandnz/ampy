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
      add_group_to_view:
        Returns the id of the view that results from adding a new group to
        an existing view.
      remove_group_from_view:
        Returns the id of the view that results from removing a group from an
        existing view.

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

    def get_view_groups(self, collection, viewid):
        """
        Queries the views database to find the set of groups that belong 
        to a given view.

        Parameters:
          collection -- the name of the collection that the view belongs to
          viewid -- the id number of the view

        Returns:
          a dictionary containing the groups that are part of the given view.
          The dictionary keys are the group id numbers and the values are
          the group description strings.
          Will return None if the query fails.
        """
        groups = {}

        if viewid == 0:
            return groups

        query = """SELECT group_id, group_description FROM groups WHERE
                collection = %s AND group_id IN (SELECT unnest(view_groups)
                FROM views WHERE view_id=%s) """
        params = (collection, viewid)

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
            groups[row['group_id']] = row['group_description']
        
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

        
    def get_view_id(self, collection, groups):
        """
        Queries the views database for a view that contains the given
        set of groups. If a matching view does not exist, one is created.

        Parameters:
          collection -- the collection that the group belongs to
          groups -- a list of group IDs to query for

        Returns:
          the ID number of the view that consists of the groups that were
          provided or None if there was a database error.


        """
        # Create view if it doesn't exist
        query = """SELECT view_id FROM views WHERE collection=%s AND 
                view_groups=%s"""
        params = (collection, groups)

        self.dblock.acquire()
        if self.db.executequery(query, params) == -1:
            log("Error while checking if view exists")
            self.dblock.release()
            return None

        # Ideally, this shouldn't happen but let's try and do something
        # sensible if it does
        if self.db.cursor.rowcount > 1:
            log("Warning: multiple views match in collection %s, %s" % (collection, description))
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

    def add_group_to_view(self, collection, viewid, description):
        """
        Adds a new group to an existing view and returns the ID of the
        modified view.

        Parameters:
          collection -- the collection that the view belongs to
          viewid -- the ID number of the view being modified. A view id of
                    zero represents an empty view (i.e. with no groups)
          description -- a string describing the group to be added to the
                         view

        Returns:
          the ID of the view that results from adding the group to the
          existing view. Returns the original view ID if the view is
          unchanged. Returns None if a database error occurred while
          modifying the view.

        """
        # First, find all the groups for the original view
        groups = self.get_view_groups(collection, viewid)
        if groups is None:
            return None
        groups = groups.keys()
        
        # Find the group ID for the group we are about to add
        groupid = self.get_group_id(collection, description)
        if groupid is None:
            return None

        # Always keep our groups in sorted order, as this makes it much
        # easier to query the views table later on
        if groupid not in groups:
            groups.append(groupid)
            groups.sort()
        
        # Work out the view id for the new set of groups
        newview = self.get_view_id(collection, groups)
        if newview is None:
            return None
        return newview


    def remove_group_from_view(self, collection, viewid, groupid):
        """
        Removes a group from an existing view and returns the ID of the
        modified view.

        Parameters:
          collection -- the collection that the view belongs to
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
        groups = self.get_view_groups(collection, viewid)
        if groups is None:
            return None
        groups = groups.keys()

        # Remove the group from the group list if present
        if groupid in groups:
            groups.remove(groupid)
        else:
            return viewid

        # If the view is now empty, return 0 to indicate no active groups
        if len(groups) == 0:
            return 0

        # Work out the view id for the new set of groups
        newview = self.get_view_id(collection, groups)
        if newview is None:
            return None
        return newview

       
# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
