#!/usr/bin/env python
# -*- coding: ascii -*-

import sqlalchemy
import re

# XXX move all this into nntsc.py? or can we get enough info into here that
# it can actually do all the hard work?
class View(object):
    """ Map between a view id and lists of stream ids describing lines """

    def __init__(self, nntsc, dbconfig):
        """ Create connection to views database """
        self.nntsc = nntsc
        self.splits = ["FULL", "NONE", "NETWORK", "FAMILY"]
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


    def create_view(self, collection, oldview, options):
        """ Return a view_id describing the desired view """
        # determine the groups that exist in the current view
        groups = self._get_groups_in_view(oldview)
        if groups is None:
            return None

        # create the description string for the group based on the options
        group = self._parse_group_string(collection, options)
        if group is None:
            # something is wrong with the options string, don't do anything
            return oldview

        # get the group id, creating it if it doesn't already exist
        group_id = self._get_group_id(group)
        if group_id is None:
            # something went wrong trying to create the group, don't do anything
            return oldview


        # combine the existing groups with the new group id
        if group_id not in groups:
            groups.append(group_id)
            groups.sort()

        # get the view id containing the current view groups plus the new
        # group, creating it if it doesn't already exist
        view_id = self._get_view_id(groups)
        if view_id is None:
            return oldview
        return view_id


    def _parse_group_string(self, collection, options):
        """ Convert the URL arguments into a group description string """
        if options[3].upper() not in self.splits:
            return None

        if collection == "amp-icmp":
            return "%s FROM %s TO %s OPTION %s %s" % (
                    collection, options[0], options[1], options[2],
                    options[3].upper())
        return None


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

        # XXX do we need collection to be passed in?
        # XXX do matrix properly
        if view_id.startswith("matrix_"):
            return self._get_matrix_view_groups(view_id)

        if self.viewdb is None:
            return []

        rules = [x[0] for x in self.viewdb.execute(sqlalchemy.text(
                    "SELECT group_description FROM groups WHERE group_id IN "
                    "(SELECT unnest(view_groups) FROM views WHERE "
                    "view_id = :view_id)"),
                {"view_id": view_id})]

        for rule in rules:
            parts = re.match("(?P<collection>[a-z-]+) "
                    "FROM (?P<source>[.a-zA-Z0-9-]+) "
                    "TO (?P<destination>[.a-zA-Z0-9-]+) "
                    "OPTION (?P<option>[a-zA-Z0-9]+) "
                    "(?P<split>[A-Z]+)", rule)
            if parts is None:
                continue
            assert(parts.group("split") in self.splits)
            # fetch all streams for this group
            # XXX this is only for amp data with packet size
            streams = self.nntsc.get_stream_id(collection, {
                        "source": parts.group("source"),
                        "destination": parts.group("destination"),
                        "packet_size": parts.group("option") })

            # split the streams into the desired groupings
            if type(streams) == list and len(streams) > 0:
                if parts.group("split") == "FULL":
                    groups.update(self._get_combined_view_groups(collection,
                                parts, streams))
                elif parts.group("split") == "NONE":
                    groups.update(self._get_all_view_groups(collection,
                                parts, streams))
                elif parts.group("split") == "NETWORK":
                    pass
                elif parts.group("split") == "FAMILY":
                    groups.update(self._get_family_view_groups(collection,
                                parts, streams))
        return groups



    def _get_combined_view_groups(self, collection, parts, streams):
        """ Combined all streams together into a single result line """
        key = "%s_%s_%s" % (collection, parts.group("source"),
                parts.group("destination"))
        return { key: streams }


    def _get_all_view_groups(self, collection, parts, streams):
        """ Display all streams as individual result lines """
        groups = {}
        for stream in streams:
            info = self.nntsc.get_stream_info(collection, stream)
            key = "%s_%s_%s_%s" % (collection, parts.group("source"),
                    parts.group("destination"), info["address"])
            groups[key] = [stream]
        return groups


    def _get_family_view_groups(self, collection, parts, streams):
        """ Group streams by address family, displaying a line for ipv4/6 """
        groups = {}
        for stream in streams:
            info = self.nntsc.get_stream_info(collection, stream)
            if "." in info["address"]:
                family = "ipv4"
            else:
                family = "ipv6"
            key = "%s_%s_%s_%s" % (collection, parts.group("source"),
                    parts.group("destination"), family)
            if key not in groups:
                groups[key] = []
            groups[key].append(stream)
        return groups


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
                if len(streams) > 0:
                    groups[source + "_" + destination] = streams
        return groups


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
