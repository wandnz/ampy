#
# This file is part of ampy.
#
# Copyright (C) 2013-2019 The University of Waikato, Hamilton, New Zealand.
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

class AmpExternal(Collection):
    def __init__(self, colid, viewmanager, nntscconf):
        super(AmpExternal, self).__init__(colid, viewmanager, nntscconf)
        self.streamproperties = ['source', 'destination', 'command']
        self.groupproperties = ['source', 'destination', 'command']
        self.collection_name = "amp-external"
        self.preferences = {}

    def detail_columns(self, detail):
        if detail in ['matrix', 'basic', 'spark', 'tooltiptext']:
            aggfuncs = ["avg", "stddev"]
            aggcols = ["value", "value"]
        else:
            aggfuncs = ["avg"]
            aggcols = ["value"]
        return (aggcols, aggfuncs)

    def calculate_binsize(self, start, end, detail):
        # TODO confirm a sensible binsize
        if (end - start) / 60.0 < 200:
            return 60
        return super(AmpExternal, self).calculate_binsize(start, end, detail)

    def get_legend_label(self, description):
        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate legend label")
            return None, ""

        label = "%s to %s: %s" % (
                groupparams['source'], groupparams['destination'],
                groupparams['command'])

        return label, ""

    def group_to_labels(self, groupid, description, lookup=True):
        """
        Converts a group description string into a set of labels describing
        each of the lines that would need to be drawn on a graph for that group.
        """
        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate legend label")
            return None

        baselabel = 'group_%s' % (groupid)

        for key, value in groupparams.iteritems():
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

        return [{
            'labelstring': baselabel,
            'streams': streams,
            'shortlabel': '%s' % (groupparams['destination'])
        }]

    def create_group_description(self, properties):
        for prop in self.groupproperties:
            if prop not in properties:
                log("Required group property '%s' not present in %s group" % \
                    (prop, self.collection_name))
                return None

        return "FROM %s TO %s OPTION %s" % (
                    properties['source'], properties['destination'],
                    properties['command'])

    def parse_group_description(self, description):
        regex = "FROM (?P<source>[.a-zA-Z0-9_-]+) "
        regex += "TO (?P<destination>[.a-zA-Z0-9_-]+) "
        regex += "OPTION (?P<command>[a-zA-Z0-9]+)"

        parts = self._apply_group_regex(regex, description)

        if parts is None:
            log("no matches for %s" % description)
            return None

        keydict = {
            "source": parts.group("source"),
            "destination": parts.group("destination"),
            "command": parts.group("command"),
        }

        return keydict

    # TODO move this to the parent class, most tests can probably use it
    # Set all the properties available for the external test, preferring first
    # to use the provided values if possible, otherwise using the ordered list
    # of preferences for each property, or the first value if none of the above
    # are present in the database.
    def _set_all_properties(self, properties):
        if 'source' not in properties or 'destination' not in properties:
            return None

        for prop in self.streamproperties:
            if prop in ['source', 'destination', 'family']:
                continue

            selections = self.streammanager.find_selections(properties, "", "1",
                    30000, False)
            if selections is None:
                return None

            key, values = selections
            if key != prop:
                log("Unable to find %s for %s matrix cell %s to %s" % (
                            key, self.collection_name, properties["source"],
                            properties["destination"]))
                return None

            if values == {} or 'items' not in values:
                return None

            # if the property was preset, try to use that same value
            if prop in properties and properties[prop] in values['items']:
                continue

            # otherwise, pick the most preferred value available
            if key in self.preferences:
                for pref in self.preferences[key]:
                    if any(pref == found['text'] for found in values['items']):
                        properties[key] = pref
                        break

            # Just use the first value for now if preferred value isn't there
            if key not in properties:
                properties[key] = values['items'][0]['text']

        return properties

    # TODO figure out best way to approach the matrix
    def update_matrix_groups(self, cache, source, dest, optdict, groups,
            views, viewmanager, viewstyle):
        views[(source, dest)] = -1
        return


    def translate_group(self, groupprops):
        if 'source' not in groupprops:
            return None
        if 'destination' not in groupprops:
            return None

        properties = self._set_all_properties(groupprops)

        if properties is None:
            return None

        return self.create_group_description(properties)

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
