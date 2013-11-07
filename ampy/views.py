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
        self.splits = ["COMBINED", "ALL", "NETWORK", "FAMILY"]
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


    def get_view_groups(self, collection, view_id):
        """ Get the view groups associated with a view id """
        # Each group of stream ids here is a single set of data and represents
        # one line on the graph. They should all be combined together by the
        # database to give a response aggregated across all the members.
        groups = {}

        # cheat and just add all the sites we know should be in the matrix
        # TODO look things up in the database rather than parsing the view
        if view_id.startswith("matrix_"):
            return self._get_matrix_view_groups(view_id)

        if self.viewdb is None:
            return []
        rules = [x[0] for x in self.viewdb.execute(sqlalchemy.text(
                    "SELECT view_group FROM views WHERE view_label = :label"),
                {"label": view_id})]

        for rule in rules:
            # get current streams that match the rule
            # TODO this language needs expanding to work properly with more
            # collection types other than amp-icmp and amp-traceroute
            parts = re.match("FROM (?P<source>[.a-zA-Z0-9-]+) TO (?P<destination>[.a-zA-Z0-9-]+) (?P<split>[A-Z]+)",
                    rule)
            assert(parts.group("split") in self.splits)
            if collection == "amp-icmp":
                packet_size = "84"
            else:
                packet_size = "60"
            # fetch all streams for this group
            # XXX is this list sometimes coming back with lots of duplicates?
            # XXX this is only for amp data with packet size
            streams = self.nntsc.get_stream_id(collection, {
                        "source": parts.group("source"),
                        "destination": parts.group("destination"),
                        "packet_size": packet_size })
            # split the streams into the desired groupings
            if type(streams) == list and len(streams) > 0:
                if parts.group("split") == "COMBINED":
                    groups.update(self._get_combined_view_groups(collection,
                                parts, streams))
                elif parts.group("split") == "ALL":
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
