from libampy.database import AmpyDatabase
from libnntscclient.logger import *

class ViewManager(object):

    def __init__(self, viewdbconfig):
        
        if 'name' not in viewdbconfig:
            viewdbconfig['name'] = "views"

        self.dbconfig = viewdbconfig
        self.db = AmpyDatabase(viewdbconfig, True)
        self.db.connect(15)
    

    def get_view_groups(self, collection, viewid):

        # TODO Check the cache to see if we already know the groups for this
        # particular view

        groups = {}

        if viewid == 0:
            return groups

        query = """SELECT group_id, group_description FROM groups WHERE
                collection = %s AND group_id IN (SELECT unnest(view_groups)
                FROM views WHERE view_id=%s) """
        params = (collection, viewid)

        if self.db.executequery(query, params) == -1:
            log("Error while fetching the groups for a view")
            return None

        if self.db.cursor.rowcount == 0:
            return groups

        for row in self.db.cursor.fetchall():
            groups[row['group_id']] = row['group_description']
        return groups

    def update_view_groups(self, collection, viewid, groups):
        pass 

    def get_stream_view(self, collection, stream):
        # Check if there is cache entry describing which view this stream
        # maps to. If there is, return that and update the cache timeout.
        # If not, return 0 so that the caller can work it out manually.
        pass

    def update_stream_view(self, collection, stream, view_id):
        # Insert the stream-to-view mapping into the cache
        pass

    def get_group_id(self, collection, description):
        # Create group if it doesn't exist

        query = """SELECT group_id FROM groups WHERE collection=%s AND 
                group_description=%s"""
        params = (collection, description)

        if self.db.executequery(query, params) == -1:
            log("Error while checking if group exists")
            return None

        if self.db.cursor.rowcount > 1:
            log("Warning: multiple groups match the description %s %s" % (collection, description))
            log("Using first instance")

        if self.db.cursor.rowcount == 0:
            query = """INSERT INTO groups (collection, group_description) 
                    VALUES (%s, %s) RETURNING group_id
                    """
            if self.db.executequery(query, params) == -1:
                log("Error while inserting new group")
                return None

        group_id = self.db.cursor.fetchone()['group_id']
        return group_id

        

    def get_view_id(self, collection, groups):
        # Create view if it doesn't exist
        query = """SELECT view_id FROM views WHERE collection=%s AND 
                view_groups=%s"""
        params = (collection, groups)

        if self.db.executequery(query, params) == -1:
            log("Error while checking if view exists")
            return None

        if self.db.cursor.rowcount > 1:
            log("Warning: multiple views match in collection %s, %s" % (collection, description))
            log("Using first instance")

        if self.db.cursor.rowcount == 0:
            query = """INSERT INTO views (collection, view_groups) 
                    VALUES (%s, %s) RETURNING view_id
                    """
            if self.db.executequery(query, params) == -1:
                log("Error while inserting new view")
                return None

        view_id = self.db.cursor.fetchone()['view_id']
        return view_id

    def add_group_to_view(self, collection, viewid, description):
        groups = self.get_view_groups(collection, viewid).keys()
        if groups is None:
            return None
        
        groupid = self.get_group_id(collection, description)
        if groupid is None:
            return view_id

        if groupid not in groups:
            groups.append(groupid)
            groups.sort()
        
        # Work out the view id for the new set of groups
        newview = self.get_view_id(collection, groups)
        if newview is None:
            return viewid
        return newview


    def remove_group_from_view(self, collection, viewid, groupid):
        groups = self.get_view_groups(collection, viewid).keys()
        if groups is None:
            return None

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
            return viewid
        return newview

       
# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
