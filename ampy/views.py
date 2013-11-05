#!/usr/bin/env python
# -*- coding: ascii -*-

import sqlalchemy
import re

# XXX move all this into nntsc.py? or can we get enough info into here that
# it can actually do all the hard work?
class View(object):
    """
    """

    def __init__(self, nntsc, dbconfig):
        """  """
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


    def get_view_groups(self, view_id):
        """  """
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
            parts = re.match("(?P<test>[a-zA-Z-]+) FROM (?P<source>[.a-zA-Z-]+) TO (?P<destination>[.a-zA-Z-]+) (?P<split>[A-Z]+)",
                    rule)
            assert(parts.group("split") in self.splits)
            # fetch all streams for this group
            # XXX is this list sometimes coming back with lots of duplicates?
            # XXX this is only for icmp data with packet size 84bytes
            streams = self.nntsc.get_stream_id(parts.group("test"), {
                        "source": parts.group("source"),
                        "destination": parts.group("destination"),
                        "packet_size": "84" })
            # split the streams into the desired groupings
            if len(streams) > 0:
                if parts.group("split") == "COMBINED":
                    groups.update(self._get_combined_view_groups(parts, streams))
                elif parts.group("split") == "ALL":
                    groups.update(self._get_all_view_groups(parts, streams))
                elif parts.group("split") == "NETWORK":
                    pass
                elif parts.group("split") == "FAMILY":
                    groups.update(self._get_family_view_groups(parts, streams))
        return groups


    def _get_combined_view_groups(self, parts, streams):
        """ Combined all streams together into a single result line """
        key = "%s_%s" % (parts.group("source"), parts.group("destination"))
        return { key: streams }


    def _get_all_view_groups(self, parts, streams):
        """ Display all streams as individual result lines """
        groups = {}
        for stream in streams:
            info = self.nntsc.get_stream_info(parts.group("test"), stream)
            key = "%s_%s_%s" % (parts.group("source"),
                    parts.group("destination"), info["address"])
            groups[key] = [stream]
        return groups


    def _get_family_view_groups(self, parts, streams):
        """ Group streams by address family, displaying a line for ipv4/6 """
        groups = {}
        for stream in streams:
            info = self.nntsc.get_stream_info(parts.group("test"), stream)
            if "." in info["address"]:
                family = "ipv4"
            else:
                family = "ipv6"
            key = "%s_%s_%s" % (parts.group("source"),
                    parts.group("destination"), family)
            if key not in groups:
                groups[key] = []
            groups[key].append(stream)
        return groups


    def _get_matrix_view_groups(self, view_id):
        """ Quick way to get all the matrix data without using the database """
        groups = {}
        parts = view_id.split("_")
        assert(len(parts) == 3 and parts[0] == "matrix")

        sources = self.nntsc.get_selection_options("amp-icmp",
                {"_requesting": "sources", "mesh": parts[1]})
        destinations = self.nntsc.get_selection_options("amp-icmp",
                {"_requesting": "destinations", "mesh": parts[2]})

        for source in sources:
            for destination in destinations:
                streams = self.nntsc.get_stream_id("amp-icmp", {
                    "source": source,
                    "destination": destination,
                    "packet_size": "84"})
                if len(streams) > 0:
                    groups[source + "_" + destination] = streams
        return groups


# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
