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
from libampy.collection import Collection

class AmpIcmp(Collection):
    def __init__(self, colid, viewmanager, nntscconf):
        super(AmpIcmp, self).__init__(colid, viewmanager, nntscconf)
        self.streamproperties = [
            'source', 'destination', 'packet_size', 'family'
        ]
        self.groupproperties = [
            'source', 'destination', 'packet_size', 'aggregation'
        ]
        self.collection_name = "amp-icmp"
        self.splits = {
            "FAMILY":"IPv4/IPv6",
            "FULL":"All Addresses",
            "IPV4":"IPv4",
            "IPV6":"IPv6"
        }
        self.default_packet_size = "84"
        self.viewstyle = "amp-latency"
        self.sizepreferences = [self.default_packet_size]

    def detail_columns(self, detail):
        # the matrix view expects both the mean and stddev for the latency
        if detail == "matrix":
            aggfuncs = ["avg", "stddev", "count", "sum", "sum", "stddev"]
            aggcols = ["median", "median", "median", "loss", "results", "lossrate"]
        elif detail in ["basic", "spark", "tooltiptext"]:
            aggfuncs = ["avg", "sum", "sum"]
            aggcols = ["median", "loss", "results"]
        else:
            aggfuncs = ["avg", "smokearray", "sum", "sum"]
            aggcols = ["median", "rtts", "loss", "results"]
        return (aggcols, aggfuncs)

    def calculate_binsize(self, start, end, detail):
        if (end - start) / 60.0 < 200:
            return 60
        return super(AmpIcmp, self).calculate_binsize(start, end, detail)

    def get_legend_label(self, description):
        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate legend label")
            return None

        label = "%s to %s ICMP" % (groupparams['source'],
                groupparams['destination'])

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
            'packet_size': groupparams['packet_size']
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

        return "FROM %s TO %s OPTION %s %s" % ( \
                properties['source'], properties['destination'],
                properties['packet_size'], properties['aggregation'].upper())

    def parse_group_description(self, description):
        regex = "FROM (?P<source>[.a-zA-Z0-9-]+) "
        regex += "TO (?P<destination>[.a-zA-Z0-9-]+) "
        regex += "OPTION (?P<option>[a-zA-Z0-9]+) "
        regex += "(?P<split>[A-Z0-9]+)"

        parts = self._apply_group_regex(regex, description)

        if parts is None:
            return None

        if parts.group("split") not in self.splits:
            log("%s group description has no aggregation method" % \
                    (self.collection_name))
            log(description)
            return None

        keydict = {
            "source": parts.group("source"),
            "destination": parts.group("destination"),
            "packet_size": parts.group("option"),
            "aggregation": parts.group("split")
        }

        return keydict

    def update_matrix_groups(self, cache, source, dest, split, groups, views,
            viewmanager, viewstyle):

        baseprop = {'source': source, 'destination': dest}

        sels = self.streammanager.find_selections(baseprop, "", "1", 30000, False)
        if sels is None:
            return None

        req, sizes = sels
        if req != 'packet_size':
            log("Unable to find packet size for %s matrix cell %s to %s" % (
                        self.collection_name, source, dest))
            return None

        if sizes == {} or 'items' not in sizes:
            views[(source, dest)] = -1
            return

        for size in self.sizepreferences:
            if any(size == found['text'] for found in sizes['items']):
                baseprop['packet_size'] = size
                break

        if 'packet_size' not in baseprop:
            # Just use the lowest packet size for now
            baseprop['packet_size'] = sizes['items'][0]['text']

        ipv4 = self._matrix_group_streams(baseprop, 'ipv4', groups)
        ipv6 = self._matrix_group_streams(baseprop, 'ipv6', groups)

        if ipv4 == 0 and ipv6 == 0:
            views[(source, dest)] = -1
            return

        if split == "ipv4":
            split = "IPV4"
        elif split == "ipv6":
            split = "IPV6"
        else:
            split = "FAMILY"

        cachelabel = "_".join([viewstyle, self.collection_name, source, dest,
                split, baseprop['packet_size']])

        viewid = cache.search_matrix_view(cachelabel)
        if viewid is not None:
            views[(source, dest)] = viewid
            return

        cellgroup = self.create_group_from_list([source, dest,
                baseprop['packet_size'], split])
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

        if 'packet_size' not in groupprops:
            packetsize = self.default_packet_size
        else:
            packetsize = groupprops['packet_size']

        newprops = {
            'source': groupprops['source'],
            'destination': groupprops['destination']
        }

        sels = self.streammanager.find_selections(newprops, "", "1", 10000)
        if sels is None:
            return None

        req, sizes = sels
        if req != 'packet_size':
            log("Unable to find packet sizes for %s %s to %s" % \
                    (self.collection_name, newprops['source'], \
                    newprops['destination']))
            return None

        if sizes is not None and len(sizes['items']) > 0:
            packetsize = None
            for size in sizes['items']:
                if size['text'] == self.default_packet_size:
                    packetsize = self.default_packet_size
                    break
                elif packetsize is None or int(size['text']) < packetsize:
                    packetsize = size['text']

        if 'aggregation' not in groupprops:
            aggregation = "FAMILY"
        else:
            aggregation = groupprops['aggregation']

        newprops['aggregation'] = aggregation
        newprops['packet_size'] = packetsize

        return self.create_group_description(newprops)

    def _matrix_group_streams(self, props, family, groups):
        props['family'] = family
        label = "%s_%s_%s" % (props['source'], props['destination'], family)
        streams = self.streammanager.find_streams(props)

        if len(streams) > 0:
            groups.append({'labelstring': label, 'streams': streams})

        return len(streams)

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
