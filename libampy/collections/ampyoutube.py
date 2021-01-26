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

from libnntscclient.logger import log
from libampy.collection import Collection

class AmpYoutube(Collection):
    def __init__(self, colid, viewmanager, nntscconf):
        super(AmpYoutube, self).__init__(colid, viewmanager, nntscconf)
        self.streamproperties = [
            'source', 'destination', 'quality'
        ]
        self.groupproperties = [
            'source', 'destination', 'quality'
        ]
        # XXX is quality best as an integer or a string?
        # XXX desired quality vs actual quality
        self.integerproperties = [
            'quality',
        ]

        self.collection_name = "amp-youtube"
        self.viewstyle = self.collection_name

    def get_maximum_view_groups(self):
        return 1

    def convert_property(self, streamprop, value):
        return value

    def detail_columns(self, detail):
        """
        Determines which data table columns should be queried and how they
        should be aggregated, given the amount of detail required by the user.
        """
        if detail in ['matrix', 'basic', 'spark', 'tooltiptext']:
            aggs = ['avg', 'stddev', 'avg', 'stddev', 'avg', 'stddev',
                    'avg', 'stddev', 'avg', 'stddev']
            cols = ['total_time', 'total_time',
                    'pre_time', 'pre_time',
                    'initial_buffering', 'initial_buffering',
                    'stall_time', 'stall_time',
                    'stall_count', 'stall_count',
            ]
        elif detail in ["rainbow", "rainbow-summary", "raw"]:
            # XXX this needs to actually come from the event timeline list
            aggs = ["avg", "avg", "avg", "avg", "avg"]
            cols = ["pre_time", "initial_buffering", "playing_time",
                    "stall_time", "total_time"]
        else:
            # XXX what other detail types get used?
            aggs = ['avg', 'stddev']
            cols = ['total_time', 'total_time']

        return cols, aggs

    def calculate_binsize(self, start, end, detail):
        """
        Determines an appropriate binsize for a graph covering the
        specified time period.
        """
        if (end - start) / 900 < 200:
            return 900

        if (end - start) / (900 * 4) < 200:
            return (900 * 4)

        if (end - start) / (900 * 12) < 200:
            return (900 * 12)

        return (900 * 24)

    def create_group_description(self, properties):
        """
        Converts a dictionary of stream or group properties into a string
        describing the group.
        """
        for prop in self.groupproperties:
            if prop not in properties:
                log("Required group property '%s' not present in %s group" % \
                        (prop, self.collection_name))
                return None

        return "FROM %s FETCH %s %s" % (
                    properties['source'], properties['destination'],
                    properties['quality'])

    def parse_group_description(self, description):
        """
        Converts a group description string into a dictionary mapping
        group properties to their values.
        """
        regex = "FROM (?P<source>[.a-zA-Z0-9_-]+) "
        regex += "FETCH (?P<destination>[\S]+) "
        regex += "(?P<quality>[0-9]+)"

        parts = self._apply_group_regex(regex, description)
        if parts is None:
            return None

        keydict = {
            'source': parts.group('source'),
            'destination': parts.group('destination'),
            'quality': int(parts.group('quality')),
        }

        return keydict

    def _get_quality_label(self, quality):
        if quality == 1:
            label = "default"
        elif quality == 2:
            label = "small"
        elif quality == 3:
            label = "medium"
        elif quality == 4:
            label = "large"
        elif quality == 5:
            label = "hd720"
        elif quality == 6:
            label = "hd1080"
        elif quality == 7:
            label = "hd1440"
        elif quality == 8:
            label = "hd2160"
        elif quality == 9:
            label = "highres"
        else:
            label = "unknown"
        return label

    def get_legend_label(self, description):
        """
        Converts a group description string into an appropriate label for
        placing on a graph legend.
        """
        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate legend label")
            return None

        quality = self._get_quality_label(groupparams["quality"])

        label = "%s from %s quality:%s" % (groupparams['destination'],
                groupparams['source'], quality)
        return label, ""

    def group_to_labels(self, groupid, description, lookup=True):
        """
        Converts a group description string into a set of labels describing
        each of the lines that would need to be drawn on a graph for that group.
        """
        labels = []

        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate legend label")
            return None

        baselabel = 'group_%s' % (groupid)

        for key, value in groupparams.items():
            if key in self.integerproperties:
                groupparams[key] = int(value)

        if lookup:
            streams = self.streammanager.find_streams(groupparams)
            if streams is None:
                log("Failed to find streams for label %s, %s" % \
                        (baselabel, self.collection_name))
                return None
        else:
            streams = []

        labels.append({
            'labelstring': baselabel,
            'streams': streams,
            'shortlabel': '%s' % (groupparams['destination'])
        })

        return labels

    def update_matrix_groups(self, cache, source, dest, optdict, groups, views,
            viewmanager, viewstyle):
        """
        Finds all of the groups that need to queried to populate a matrix cell,
        including the stream ids of the group members.
        """
        groupprops = {'source': source, 'destination': dest}

        # XXX use correct address family -- how to tell what it was?
        label = "%s_%s_ipv4" % (source, dest)
        streams = self.streammanager.find_streams(groupprops)

        if len(streams) == 0:
            views[(source, dest)] = -1
            return

        groups.append({'labelstring': label, 'streams': streams})

        cellgroups = []
        for stream in streams:
            props = self.streammanager.find_stream_properties(stream)

            proplist = [props[x] for x in self.groupproperties]
            cellgroup = self.create_group_from_list(proplist)

            if cellgroup is None:
                log("Failed to create group for %s matrix cell" % \
                        (self.collection_name))
                break
            cellgroups.append(cellgroup)

        cachelabel = "_".join([viewstyle, self.collection_name,
                source, dest])
        viewid = cache.search_matrix_view(cachelabel)
        if viewid is not None:
            views[(source, dest)] = viewid
            return

        viewid = viewmanager.add_groups_to_view(viewstyle,
                self.collection_name, 0, cellgroups)
        if viewid is None:
            views[(source, dest)] = -1
            cache.store_matrix_view(cachelabel, -1, 300)
        else:
            views[(source, dest)] = viewid
            cache.store_matrix_view(cachelabel, viewid, 0)

    # replace some labels used in the database with better human-readable text
    def get_selections(self, selected, term, page, pagesize, logmissing=True):
        options = super(AmpYoutube, self).get_selections(selected, term, page,
                pagesize, logmissing)
        if "quality" in options and "items" in options["quality"]:
            for quality in options["quality"]["items"]:
                quality["text"] = self._get_quality_label(quality["id"])
        return options

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
