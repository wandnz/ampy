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

class AmpThroughput(Collection):
    def __init__(self, colid, viewmanager, nntscconf):
        super(AmpThroughput, self).__init__(colid, viewmanager, nntscconf)

        self.streamproperties = [
            'source', 'destination', 'protocol', 'duration', 'writesize',
            'tcpreused', 'direction', 'family'
        ]
        self.groupproperties = self.streamproperties
        self.integerproperties = ['duration', 'writesize']
        self.collection_name = "amp-throughput"
        self.viewstyle = "amp-throughput"

        self.default_duration = 10000
        self.default_writesize = 131072
        self.default_protocol = "default"

        self.dirlabels = {"in": "Download", "out": "Upload"}

    def detail_columns(self, detail):
        aggfuncs = ["sum", "sum", "sum", "stddev"]
        aggcols = ["bytes", "packets", "runtime", "rate"]
        return aggcols, aggfuncs

    def calculate_binsize(self, start, end, detail):
        # Hard to pre-determine a suitable binsize for throughput tests
        # as the measurement frequency is likely to change from test to test.
        # Problem is, if we choose a bad binsize we can easily end up in a
        # situation where we think there's a gap in the data when there
        # really isn't
        if (end - start) / 3600 < 200:
            return 3600

        if (end - start) / (3600 * 4) < 200:
            return (3600 * 4)

        if (end - start) / (3600 * 12) < 200:
            return (3600 * 12)

        return (3600 * 24)

    def prepare_stream_for_storage(self, stream):
        if 'address' not in stream:
            return stream, {}

        if self._address_to_family(stream['address']) == "ipv4":
            stream['family'] = "ipv4"
        else:
            stream['family'] = "ipv6"

        return stream, {'address':stream['address']}

    def create_group_description(self, properties):
        # TODO tcpreused should always be false now, can we remove the need
        # for it to be part of the group description?
        if 'tcpreused' in properties:
            if properties['tcpreused'] is True:
                reuse = "T"
            else:
                reuse = "F"
        else:
            reuse = "F"

        if 'direction' not in properties:
            properties['direction'] = "BOTH"
        if 'family' not in properties and 'address' in properties:
            properties['family'] = \
                    self._address_to_family(properties['address'])

        for prop in self.groupproperties:
            if prop not in properties:
                log("Required group property '%s' not present in %s group" % \
                        (prop, self.collection_name))
                return None

        properties['direction'] = properties['direction'].upper()
        properties['family'] = properties['family'].upper()

        return "FROM %s TO %s DURATION %s WRITESIZE %s %s DIRECTION %s FAMILY %s PROTOCOL %s" \
                % (properties['source'], properties['destination'],
                   properties['duration'], properties['writesize'], reuse,
                   properties['direction'], properties['family'],
                   properties['protocol'])

    def get_legend_label(self, description):
        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate legend label")
            return None

        source = groupparams['source']
        dest = groupparams['destination']

        if groupparams['family'] == "IPV4":
            family = "IPv4"
        elif groupparams['family'] == "IPV6":
            family = "IPv6"
        elif groupparams['family'] == "BOTH":
            family = "IPv4/IPv6"
        else:
            family = ""

        durationsecs = groupparams['duration'] / 1000.0
        kilobytes = groupparams['writesize'] / 1024.0

        if groupparams['direction'] == "BOTH":
            dirstr = ""
        elif groupparams['direction'] == "IN":
            dirstr = " Download"
        else:
            dirstr = " Upload"

        if groupparams['protocol'] == "http":
            protocol = "as HTTP"
        else:
            protocol = ""

        label = "%s : %s for %.1f secs, %.1f kB writes, %s" % (source, dest,
                durationsecs, kilobytes, protocol)

        return label, "%s%s" % (family, dirstr)

    def parse_group_description(self, description):
        regex = "FROM (?P<source>[.a-zA-Z0-9_-]+) "
        regex += "TO (?P<destination>[.a-zA-Z0-9:_-]+) "
        regex += "DURATION (?P<duration>[0-9]+) "
        regex += "WRITESIZE (?P<writesize>[0-9]+) "
        regex += "(?P<reused>[TF]) "
        regex += "DIRECTION (?P<direction>[A-Z]+) "
        regex += "FAMILY (?P<family>[A-Z0-9]+) "
        regex += "(PROTOCOL (?P<protocol>[a-zA-Z0-9]+))?"

        parts = self._apply_group_regex(regex, description)
        if parts is None:
            return None

        if parts.group('direction') not in ['IN', 'OUT', 'BOTH']:
            log("%s is not a valid direction for a %s group" % \
                    (parts.group('direction'), self.collection_name))
            return None

        if parts.group('family') not in ['IPV4', 'IPV6', 'BOTH', 'NONE']:
            log("%s is not a valid address family for a %s group" % \
                    (parts.group('family'), self.collection_name))
            return None

        if parts.group('protocol') is None:
            # try to be backwards compatible with any old views
            protocol = self.default_protocol;
        elif parts.group('protocol') in ['default', 'http']:
            protocol = parts.group('protocol')
        else:
            log("%s is not a valid protocol for a %s group" % \
                    (parts.group('protocol'), self.collection_name))
            return None

        keydict = {
            'source':  parts.group("source"),
            'destination': parts.group("destination"),
            'family': parts.group("family"),
            'protocol': protocol,
            'direction': parts.group("direction"),
            'duration': int(parts.group("duration")),
            "writesize": int(parts.group("writesize")),
        }

        if parts.group("reused") == 'T':
            keydict['tcpreused'] = True
        else:
            keydict['tcpreused'] = False

        return keydict

    def _generate_direction_labels(self, baselabel, search, direction, family,
            lookup):
        key = baselabel + "_" + direction
        search['direction'] = direction

        if direction in self.dirlabels:
            shortlabel = self.dirlabels[direction]
        else:
            shortlabel = ""

        labels = []

        if family in ["BOTH", "FAMILY", "IPV4"]:
            label = self._generate_family_label(key, search, "IPv4", lookup)
            if label is None:
                return None
            labels.append(label)

        if family in ["BOTH", "FAMILY", "IPV6"]:
            label = self._generate_family_label(key, search, "IPv6", lookup)
            if label is None:
                return None
            labels.append(label)

        if family == "NONE":
            streams = self.streammanager.find_streams(search)
            if streams is None:
                log("Failed to find streams for %s, %s" % \
                        (key, self.collection_name))

            for sid, store in streams:
                if 'address' not in store:
                    log("Error: no address stored with stream id %s" % (sid))
                    return None

                if shortlabel == self.dirlabels["in"]:
                    streamlabel = "From %s" % (store['address'])
                else:
                    streamlabel = "To %s" % (store['address'])

                label = {
                    'labelstring': key + "_" + str(sid),
                    'streams': [sid],
                    'shortlabel': streamlabel
                }
                labels.append(label)

        return labels

    def _generate_family_label(self, baselabel, search, family, lookup):
        key = baselabel + "_" + family
        search['family'] = family.lower()
        if search['direction'] in self.dirlabels:
            shortlabel = family + " " + self.dirlabels[search['direction']]
        else:
            shortlabel = family

        if 'protocol' in search and search['protocol'] == 'http':
            shortlabel += " (as HTTP)"

        if lookup:
            streams = self.streammanager.find_streams(search)
            if streams is None:
                log("Failed to find streams for label %s, %s" % \
                        (key, self.collection_name))
                return None

            famstreams = [item[0] for item in streams]
        else:
            famstreams = []

        return {
            'labelstring': key,
            'streams': famstreams,
            'shortlabel': shortlabel
        }

    def group_to_labels(self, groupid, description, lookup=True):
        labels = []

        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate group labels")
            return None

        baselabel = 'group_%s' % (groupid)

        search = dict(groupparams)
        del search['direction']
        del search['family']

        if groupparams['direction'] in ['IN', 'BOTH']:
            label = self._generate_direction_labels(baselabel, search, 'in',
                    groupparams['family'], lookup)
            if label is None:
                return None
            labels += label


        if groupparams['direction'] in ['OUT', 'BOTH']:
            label = self._generate_direction_labels(baselabel, search, 'out',
                    groupparams['family'], lookup)
            if label is None:
                return None
            labels += label

        return sorted(labels, key=itemgetter('shortlabel'))

    def update_matrix_groups(self, cache, source, dest, optdict, groups,
            views, viewmanager, viewstyle):

        groupprops = {
            'source': source,
            'destination': dest,
            'protocol': self.default_protocol,
            'duration': self.default_duration,
            'writesize': self.default_writesize,
            'tcpreused': False,
        }

        if 'metric' in optdict and optdict['metric'] == 'http':
            groupprops['protocol'] = 'http'
        elif 'metric' in optdict and optdict['metric'] == 'tcp':
            groupprops['protocol'] = 'default'

        tputin4 = self._matrix_group_streams(groupprops, "in", "ipv4", groups)
        tputout4 = self._matrix_group_streams(groupprops, "out", "ipv4", groups)
        tputin6 = self._matrix_group_streams(groupprops, "in", "ipv6", groups)
        tputout6 = self._matrix_group_streams(groupprops, "out", "ipv6", groups)

        if tputin4 == 0 and tputout4 == 0:
            views[(source, dest, "ipv4")] = -1

        if tputin6 == 0 and tputout6 == 0:
            views[(source, dest, "ipv6")] = -1

        if tputin4 + tputin6 + tputout4 + tputout6 == 0:
            return

        if optdict['split'] == "down":
            split = "IN"
        elif optdict['split'] == "up":
            split = "OUT"
        else:
            split = "BOTH"

        if tputin4 != 0 or tputout4 != 0:
            # XXX this could become a function
            cellgroup = self.create_group_from_list([source, dest,
                    groupprops['protocol'],
                    self.default_duration,
                    self.default_writesize, False, split, "IPV4"])
            if cellgroup is None:
                log("Failed to create group for %s matrix cell" % \
                        (self.collection_name))
                return None

            cachelabel = "_".join([viewstyle, self.collection_name,
                    source, dest, split, groupprops['protocol'], "IPV4"])
            viewid = cache.search_matrix_view(cachelabel)
            if viewid is not None:
                views[(source, dest, "ipv4")] = viewid
            else:
                viewid = viewmanager.add_groups_to_view(viewstyle,
                        self.collection_name, 0, [cellgroup])

                if viewid is None:
                    views[(source, dest, "ipv4")] = -1
                    cache.store_matrix_view(cachelabel, -1, 300)
                else:
                    views[(source, dest, "ipv4")] = viewid
                    cache.store_matrix_view(cachelabel, viewid, 0)

        if tputin6 != 0 or tputout6 != 0:
            cellgroup = self.create_group_from_list([source, dest,
                    groupprops['protocol'],
                    self.default_duration,
                    self.default_writesize, False, split, "IPV6"])
            if cellgroup is None:
                log("Failed to create group for %s matrix cell" % \
                        (self.collection_name))
                return None

            cachelabel = "_".join([viewstyle, self.collection_name,
                    source, dest, split, groupprops['protocol'], "IPV6"])
            viewid = cache.search_matrix_view(cachelabel)
            if viewid is not None:
                views[(source, dest, "ipv6")] = viewid
            else:
                viewid = viewmanager.add_groups_to_view(viewstyle,
                        self.collection_name, 0, [cellgroup])

                if viewid is None:
                    views[(source, dest, "ipv6")] = -1
                    cache.store_matrix_view(cachelabel, -1, 300)
                else:
                    views[(source, dest, "ipv6")] = viewid
                    cache.store_matrix_view(cachelabel, viewid, 0)

    def _matrix_group_streams(self, baseprops, direction, family, groups):

        baseprops['direction'] = direction
        baseprops['family'] = family
        label = "%s_%s_%s_%s_%s" % (baseprops['source'], baseprops['destination'],
                baseprops['protocol'], direction, family)
        streams = self.streammanager.find_streams(baseprops)

        if len(streams) > 0:
            groups.append({
                'labelstring': label,
                'streams': [x[0] for x in streams]
            })

        return len(streams)

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
