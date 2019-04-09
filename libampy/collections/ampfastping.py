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

from operator import itemgetter
from libnntscclient.logger import log
from libampy.collections.ampicmp import AmpIcmp

class AmpFastping(AmpIcmp):
    def __init__(self, colid, viewmanager, nntscconf):
        super(AmpFastping, self).__init__(colid, viewmanager, nntscconf)
        self.streamproperties = [
            'source', 'destination', 'packet_size', 'packet_rate',
            'packet_count', 'preprobe', 'family'
        ]
        self.groupproperties = [
            'source', 'destination', 'packet_size', 'packet_rate',
            'packet_count', 'preprobe', 'aggregation'
        ]
        self.collection_name = "amp-fastping"
        self.splits = {
            "FAMILY":"IPv4/IPv6",
            "FULL":"All Addresses",
            "IPV4":"IPv4",
            "IPV6":"IPv6"
        }
        self.default_packet_size = "64"
        self.default_packet_rate = "1"
        self.default_packet_count = "60"
        self.default_preprobe = False
        self.viewstyle = "amp-latency"
        self.integerproperties = ['packet_size', 'packet_rate', 'packet_count']
        self.preferences = {
            "packet_size": [self.default_packet_size],
            "packet_rate": [self.default_packet_rate],
            "packet_count": [self.default_packet_count],
            "preprobe": [self.default_preprobe, not self.default_preprobe],
        }

    def detail_columns(self, detail):
        # TODO influx doesn't appear to be able to do anything useful with
        # the array of percentiles? Do we want to try to make it work?
        if detail == "matrix":
            aggfuncs = ["avg", "stddev", "count", "avg", "stddev"]
            aggcols = ["median", "median", "median", "lossrate", "lossrate"]
        elif detail in ["basic", "spark", "tooltiptext"]:
            aggfuncs = ["avg", "avg"]
            aggcols = ["median", "lossrate"]
        else:
            aggfuncs = ["avg", "smokearray", "avg"]
            aggcols = ["median", "percentiles", "lossrate"]
        return (aggcols, aggfuncs)

    def calculate_binsize(self, start, end, detail):
        # TODO confirm a sensible binsize
        if (end - start) / 60.0 < 200:
            return 60
        return super(AmpFastping, self).calculate_binsize(start, end, detail)

    def get_legend_label(self, description):
        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate legend label")
            return None

        label = "%s to %s (%s packets %sB %spps) ICMP Stream" % (
                groupparams['source'], groupparams['destination'],
                groupparams['packet_count'], groupparams['packet_size'],
                groupparams['packet_rate'])

        return label, self.splits[groupparams['aggregation']]

    def _generate_label(self, baselabel, search, family, lookup):
        if family is None:
            key = baselabel
            shortlabel = "All addresses"
        else:
            key = baselabel + "_" + family
            search['family'] = family.lower()
            shortlabel = family

        if lookup:
            streams = self.streammanager.find_streams(search)
            if streams is None:
                log("Failed to find streams for label %s, %s" % \
                        (key, self.collection_name))
                return None
        else:
            streams = []

        return {
            'labelstring': key,
            'streams': streams,
            'shortlabel': shortlabel
        }

    def _group_to_search(self, groupparams):
        return {
            'source': groupparams['source'],
            'destination': groupparams['destination'],
            'packet_size': int(groupparams['packet_size']),
            'packet_rate': int(groupparams['packet_rate']),
            'packet_count': int(groupparams['packet_count']),
            'preprobe': groupparams['preprobe'] in ["true", "True"]
        }

    def group_to_labels(self, groupid, description, lookup=True):
        labels = []

        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate labels")
            return None

        baselabel = 'group_%s' % (groupid)
        search = self._group_to_search(groupparams)

        if groupparams['aggregation'] in ['IPV4', 'FAMILY']:
            nextlab = self._generate_label(baselabel, search, "IPv4", lookup)
            if nextlab is None:
                return None
            labels.append(nextlab)

        if groupparams['aggregation'] in ['IPV6', 'FAMILY']:
            nextlab = self._generate_label(baselabel, search, "IPv6", lookup)
            if nextlab is None:
                return None
            labels.append(nextlab)

        if groupparams['aggregation'] == "FULL":
            nextlab = self._generate_label(baselabel, search, None, lookup)
            if nextlab is None:
                return None
            labels.append(nextlab)

        return sorted(labels, key=itemgetter('shortlabel'))

    def create_group_description(self, properties):
        # If we're creating a description based on an existing group or
        # stream, we need to convert the 'family' into an appropriate
        # aggregation method.
        if 'family' in properties:
            properties['aggregation'] = properties['family'].upper()

        for prop in self.groupproperties:
            if prop not in properties:
                log("Required group property '%s' not present in %s group" % \
                    (prop, self.collection_name))
                return None

        return "FROM %s TO %s OPTION %s %s %s %s %s" % ( \
                properties['source'], properties['destination'],
                properties['packet_size'], properties['packet_rate'],
                properties['packet_count'], properties['preprobe'],
                properties['aggregation'].upper())

    def parse_group_description(self, description):
        regex = "FROM (?P<source>[.a-zA-Z0-9_-]+) "
        regex += "TO (?P<destination>[.a-zA-Z0-9_-]+) "
        regex += "OPTION (?P<size>[0-9]+) "
        regex += "(?P<rate>[0-9]+) "
        regex += "(?P<count>[0-9]+) "
        regex += "(?P<preprobe>[A-Za-z0-9]+) "
        regex += "(?P<split>[A-Z0-9]+)"

        parts = self._apply_group_regex(regex, description)

        if parts is None:
            log("no matches for %s" % description)
            return None

        if parts.group("split") not in self.splits:
            log("%s group description has no aggregation method" % \
                    (self.collection_name))
            log(description)
            return None

        keydict = {
            "source": parts.group("source"),
            "destination": parts.group("destination"),
            "packet_size": parts.group("size"),
            "packet_rate": parts.group("rate"),
            "packet_count": parts.group("count"),
            "preprobe": parts.group("preprobe"), # XXX int vs bool vs string
            "aggregation": parts.group("split")
        }

        return keydict

    # Set all the properties available for the fastping test, preferring first
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
            for pref in self.preferences[key]:
                if any(pref == found['text'] for found in values['items']):
                    properties[key] = pref
                    break

            # Just use the first value for now if preferred value isn't there
            if key not in properties:
                properties[key] = values['items'][0]['text']

        return properties

    def update_matrix_groups(self, cache, source, dest, optdict, groups,
            views, viewmanager, viewstyle):

        properties = self._set_all_properties(
                {"source": source, "destination": dest})

        if properties is None:
            views[(source, dest)] = -1
            return

        ipv4 = self._matrix_group_streams(properties, 'ipv4', groups)
        ipv6 = self._matrix_group_streams(properties, 'ipv6', groups)

        if ipv4 == 0 and ipv6 == 0:
            views[(source, dest)] = -1
            return

        if optdict['split'] == "ipv4":
            split = "IPV4"
        elif optdict['split'] == "ipv6":
            split = "IPV6"
        else:
            split = "FAMILY"

        proplist = [str(properties[x]) for x in self.streamproperties if x not in ['family']]

        cachelabel = "_".join(
                [viewstyle, self.collection_name, split] + proplist)

        viewid = cache.search_matrix_view(cachelabel)
        if viewid is not None:
            views[(source, dest)] = viewid
            return

        cellgroup = self.create_group_from_list(proplist + [split])
        if cellgroup is None:
            log("Failed to create group for %s matrix cell" % (
                        self.collection_name))
            return None

        viewid = viewmanager.add_groups_to_view(viewstyle,
                self.collection_name, 0, [cellgroup])
        if viewid is None:
            views[(source, dest)] = -1
            cache.store_matrix_view(cachelabel, -1, 300)
        else:
            views[(source, dest)] = viewid
            cache.store_matrix_view(cachelabel, viewid, 0)

    def translate_group(self, groupprops):
        if 'source' not in groupprops:
            return None
        if 'destination' not in groupprops:
            return None

        properties = self._set_all_properties(groupprops)

        if properties is None:
            return None

        if 'aggregation' not in properties:
            properties['aggregation'] = "FAMILY"

        return self.create_group_description(properties)

    def _matrix_group_streams(self, props, family, groups):
        props['family'] = family
        label = "%s_%s_%s" % (props['source'], props['destination'], family)
        streams = self.streammanager.find_streams(props)

        if len(streams) > 0:
            groups.append({'labelstring': label, 'streams': streams})

        return len(streams)

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
