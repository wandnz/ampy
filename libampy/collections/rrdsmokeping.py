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

from libnntscclient.logger import *
from libampy.collection import Collection
from operator import itemgetter

class RRDSmokeping(Collection):
    def __init__(self, colid, viewmanager, nntscconf):
        super(RRDSmokeping, self).__init__(colid, viewmanager, nntscconf)

        self.streamproperties = ['source', 'host', 'family']
        self.groupproperties = ['source', 'host', 'aggregation']
        self.collection_name = "rrd-smokeping"
        self.viewstyle = self.collection_name

        self.splits = {
            "IPV4": "IPv4",
            "IPV6": "IPv6",
            "FAMILY": "IPv4/IPv6"
        }

        self.default_aggregation = "FAMILY"

    def detail_columns(self, detail):
        if detail == "matrix":
            aggcols = ["median", "median", "median", "loss", "pingsent", "lossrate"]
            aggfuncs = ["avg", "stddev", "count", "sum", "sum", "stddev"]
        elif detail in ["basic", "spark", "tooltiptext"]:
            aggcols = ["loss", "pingsent", "median"]
            aggfuncs = ["sum", "sum", "avg"]
        else:
            aggcols = ["median", "pings", "loss", "pingsent"]
            aggfuncs = ["avg", "smokearray", "sum", "sum"]

        return aggcols, aggfuncs

    def calculate_binsize(self, start, end, detail):
        if (end - start) / 300.0 < 200:
            return 300

        return super(RRDSmokeping, self).calculate_binsize(start, end, detail)

    def get_legend_label(self, description):
        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate legend label")
            return None

        label = "Smokeping: %s to %s" % (groupparams['source'], groupparams['host'])
        return label, self.splits[groupparams['aggregation']]

    def create_group_description(self, properties):
        if 'family' in properties:
            properties['aggregation'] = properties['family'].upper()

        for p in self.groupproperties:
            if p not in properties:
                log("Required group property '%s' not present in %s group" % \
                        (p, self.collection_name))
                return None

        return "SOURCE %s TARGET %s %s" % (
                properties['source'], properties['host'],
                properties['aggregation'].upper())

    def parse_group_description(self, description):

        regex = "SOURCE (?P<source>[.a-zA-Z0-9-]+) "
        regex += "TARGET (?P<host>\S+) "
        regex += "(?P<split>[A-Z0-9]+)"

        parts = self._apply_group_regex(regex, description)
        if parts is None:
            return None

        if parts.group('split') not in self.splits:
            log("%s group description has no aggregation method" % \
                    (self.collection_name))
            log(description)
            return None

        keydict = {
            'source':parts.group('source'),
            'host':parts.group('host'),
            'aggregation': parts.group('split'),
        }
        return keydict

    def _generate_label(self, baselabel, search, family, lookup):
        if family is None:
            return None

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

        return {'labelstring': key, 'streams': streams, 'shortlabel':shortlabel}

    def group_to_labels(self, groupid, description, lookup=True):

        labels = []

        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate labels")
            return None

        baselabel = 'group_%s' % (groupid)
        search = {'source': groupparams['source'],
                'host': groupparams['host']}

        if groupparams['aggregation'] in ['IPV4', 'FAMILY']:
            nextlab = self._generate_label(baselabel, search, "IPv4", lookup)
            if nextlab is None:
                return None
            labels.append(nextlab)

        if groupparams['aggregation'] in ['IPV6', 'FAMILY']:
            nextlab = self._generate_label(baselabel, search, "IPv4", lookup)
            if nextlab is None:
                return None
            labels.append(nextlab)

        return sorted(labels, key=itemgetter('shortlabel'))

        if lookup:
            streams = self.streammanager.find_streams(groupparams)
            if streams is None:
                log("Failed to find streams for label %s, %s" % \
                        (key, self.collection_name))
                return None
            # No storage included with the streams so we should already have
            # a list of stream IDs
        else:
            streams = []

        return [{'labelstring':label, 'streams':streams, \
                'shortlabel':groupparams['host']}]

    def update_matrix_groups(self, cache, source, dest, optdict, groups,
            views, viewmanager, viewstyle):
        return

    def translate_group(self, groupprops):
        return None

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
