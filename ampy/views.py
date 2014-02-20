#!/usr/bin/env python
# -*- coding: ascii -*-

import sqlalchemy
import re, sys

# XXX move all this into nntsc.py? or can we get enough info into here that
# it can actually do all the hard work?
class View(object):
    """ Map between a view id and lists of stream ids describing lines """

    def __init__(self, nntsc, dbconfig):
        """ Create connection to views database """
        self.nntsc = nntsc
        # The view group database stores view group rules
        try:
            # TODO make this configurable somewhere?
            url = sqlalchemy.engine.url.URL("postgresql", database="views",
                    username=dbconfig['user'], host=dbconfig['host'],
                    password=dbconfig['pwd'])
            self.viewdb = sqlalchemy.create_engine(url)
            # test query to see if the database connection was actually made:
            # sqlalchemy is apparently stupid and doesn't let us easily check
            self.viewdb.table_names()
        except sqlalchemy.exc.OperationalError:
            self.viewdb = None


    def create_view(self, collection, oldview, action, options):
        """ Return a view_id describing the desired view """
        # determine the groups that exist in the current view
        groups = self._get_groups_in_view(oldview)
        if groups is None:
            return None

        if len(options) == 0:
            return oldview

        if action == "add":
            # create the description string for the group based on the options
            group = self.nntsc.parse_group_options(collection, options)
            if group is None:
                # something is wrong with the options string, don't do anything
                return oldview

            # get the group id, creating it if it doesn't already exist
            group_id = self._get_group_id(group)
            if group_id is None:
                # something went wrong trying to create the group
                return oldview

            # combine the existing groups with the new group id
            if group_id not in groups:
                groups.append(group_id)
                groups.sort()

        elif action == "del":
            # remove the group from the existing list to get a new view
            group_id = int(options[0])
            if group_id in groups:
                groups.remove(group_id)
            else:
                return oldview

            # if this was the last group, return view 0 so that the caller
            # can react accordingly
            if len(groups) == 0:
                return 0

        else:
            # don't do anything, return the current view
            return oldview

        # get the view id containing the current view groups plus/minus the new
        # group, creating it if it doesn't already exist
        view_id = self._get_view_id(groups)
        if view_id is None:
            return oldview
        return view_id

    # NOTE: stream is actually a stream here -- it is the nominated stream
    # from the group that the event was actually detected for. We use this
    # stream to find the others that make up the group
    def create_view_from_event(self, collection, stream):
        group = self.nntsc.event_to_group(collection, stream)
        if group == "":
            print >> sys.stderr, "Failed to find group for stream %s" % (stream)
            return -1

        group_id = self._get_group_id(group)
        view_id = self._get_view_id([group_id])
        return view_id

    def validate_tabview(self, basecol, view, tabcol):
        """ Checks if the proposed tab would link to an empty graph, i.e.
            there are no streams matching the group(s) that would result
            from switching to the new collection. We can then avoid 
            drawing tabs that won't go to a useful graph.

            This is essentially a dry-run of create_tabview that doesn't
            create any new groups or views -- we want to avoid creating
            groups etc. for tabs unless someone actually clicks on them so
            we have to have this extra validation function instead.
        """
        groups = self.get_view_groups(basecol, view)

        for gid, rule in groups.iteritems():
            splitrule = self.nntsc.split_group_rule(basecol, rule)
            tabrule = self.nntsc.get_graphtab_group(tabcol, splitrule)

            if tabrule is  None:
                continue

            newgroups = self.nntsc.find_group_streams(tabcol, tabrule, "checkrule")
            if len(newgroups.keys()) > 0:
                # This group will have at least one stream in it, so it is
                # valid.
                return True

        return False

    def create_tabview(self, basecol, view, tabcol):
        groups = self.get_view_groups(basecol, view) 

        tabgroups = []
        for gid, rule in groups.iteritems():
            splitrule = self.nntsc.split_group_rule(basecol, rule)
            tabrule = self.nntsc.get_graphtab_group(tabcol, splitrule)

            if tabrule is None:
                continue

            tabid = self._get_group_id(tabrule)
            if tabid is None:
                continue

            tabgroups.append(tabid)
       
        if tabgroups == []:
            return -1
        
        tabgroups.sort()
        tabview = self._get_view_id(tabgroups)
        if tabview is None:
            return -1
        return tabview

    def create_view_from_stream(self, collection, stream):
        group = self.nntsc.stream_to_group(collection, stream)
        if group == "":
            print >> sys.stderr, "Failed to find group for stream %d" % (stream)
            return -1

        group_id = self._get_group_id(group)
        view_id = self._get_view_id([group_id])
        return view_id

    def _get_groups_in_view(self, view_id):
        """ Get a list of the groups in a given view """
        select = self.viewdb.execute(sqlalchemy.text(
                    "SELECT view_groups FROM views WHERE view_id = :view_id"),
                {"view_id": view_id})
        groups = select.first()
        if groups is None:
            return []
        return groups[0]


    def _get_view_id(self, groups):
        """ Get the view id describing given groups, creating if necessary """
        select = self.viewdb.execute(sqlalchemy.text(
                    "SELECT view_id FROM views WHERE view_groups = :groups"),
                {"groups": groups})
        view_id = select.first()
        if view_id is None:
            return self._create_view_entry(groups)
        return view_id[0]


    def _get_group_id(self, group):
        """ Get the group id describing given group, creating if necessary """
        select = self.viewdb.execute(sqlalchemy.text(
                    "SELECT group_id FROM groups WHERE "
                    "group_description = :group"),
                {"group": group})
        group_id = select.first()
        if group_id is None:
            return self._create_group_entry(group)
        return group_id[0]


    def _create_group_entry(self, group):
        """ Insert a group entry into the database """
        insert = self.viewdb.execute(sqlalchemy.text(
                    "INSERT INTO groups (group_description) VALUES (:group) "
                    "RETURNING group_id"),
            {"group": group})
        group_id = insert.first()
        if group_id is None:
            return None
        return group_id[0]


    def _create_view_entry(self, groups):
        """ Insert a view entry into the database """
        insert = self.viewdb.execute(sqlalchemy.text(
                    "INSERT INTO views (view_label, view_groups) VALUES "
                    "(:label, :groups) RETURNING view_id"),
                {"label": "no description", "groups": groups})
        view_id = insert.first()
        if view_id is None:
            return None
        return view_id[0]


    def get_view_groups(self, collection, view_id):
        """ Get the view groups associated with a view id """
        # Each group of stream ids here is a single set of data and represents
        # one line on the graph. They should all be combined together by the
        # database to give a response aggregated across all the members.
        groups = {}

        if self.viewdb is None:
            return {}

        result = self.viewdb.execute(sqlalchemy.text(
                "SELECT group_description, group_id FROM groups WHERE "
                "group_id IN "
                "(SELECT unnest(view_groups) FROM views WHERE "
                "view_id = :view_id)"),
                {"view_id": view_id})

        rules = {}

        for row in result:
            rules[row[1]] = row[0]
        result.close()

        return rules

    def get_group_streams(self, collection, groupid, rule):
        found = self.nntsc.find_group_streams(collection, rule, groupid)

        streams = {}
        for k, v in found.iteritems():
            streams[k] = v['streams']
        return streams

    def get_view_streams(self, collection, viewid):
        result = {}

        # XXX do we need collection to be passed in?
        # XXX do matrix properly
        if str(viewid).startswith("matrix_"):
            return self._get_matrix_view_groups(viewid)

        groups = self.get_view_groups(collection, viewid)

        for gid, rule in groups.iteritems():
            result.update(self.get_group_streams(collection, gid, rule))

        return result

    def _get_matrix_view_groups(self, view_id):
        """ Quick way to get all the matrix data without using the database """
        groups = {}
        parts = view_id.split("_")
        assert(len(parts) == 5 and parts[0] == "matrix")
        assert(parts[1] == "amp-icmp" or parts[1] == "amp-traceroute")
        collection = parts[1]
        src_mesh = parts[2]
        dst_mesh = parts[3]
        packet_size = parts[4]

        sources = self.nntsc.get_selection_options(collection,
                {"_requesting": "sources", "mesh": src_mesh})
        destinations = self.nntsc.get_selection_options(collection,
                {"_requesting": "destinations", "mesh": dst_mesh})

        for source in sources:
            for destination in destinations:
                streams = self.nntsc.get_stream_id(collection, {
                    "source": source,
                    "destination": destination,
                    "packet_size": packet_size})
                for stream in streams:
                    info = self.nntsc.get_stream_info(collection, stream)
                    if info == None:
                        # Could not fetch streams due to timeout?
                        # TODO What do?
                        continue
                    if len(info) == 0:
                        continue
                    if "." in info["address"]:
                        family = "ipv4"
                    else:
                        family = "ipv6"

                    key = "%s_%s_%s" % (source, destination, family)
                    if key not in groups:
                        groups[key] = [stream]
                    else:
                        groups[key].append(stream)
        return groups


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
