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
from libampy.collections.ampicmp import AmpIcmp

class AmpTraceroute(AmpIcmp):
    def __init__(self, colid, viewmanager, nntscconf, asnmanager):
        super(AmpTraceroute, self).__init__(colid, viewmanager, nntscconf)

        self.streamproperties = [
            'source', 'destination', 'packet_size', 'family'
        ]
        self.groupproperties = [
            'source', 'destination', 'packet_size', 'aggregation'
        ]
        self.collection_name = "amp-traceroute"
        self.default_packet_size = "60"
        self.viewstyle = "amp-traceroute"
        self.asnmanager = asnmanager

    def group_columns(self, detail):
        if detail in ["ippaths", "raw"]:
            return ['aspath', 'path']
        return []

    def detail_columns(self, detail):
        if detail in ["ippaths", "raw"]:
            aggfuncs = ["most", "most", "count", "most", "most"]
            aggcols = ["error_type", "error_code", "path", "path_id", "length"]
        elif detail == "ippaths-summary":
            aggfuncs = ["count"]
            aggcols = ["path_id"]
        else:
            aggfuncs = ["smoke"]
            aggcols = ["length"]

        return aggcols, aggfuncs

    def extra_blocks(self, detail):
        if detail == "full":
            return 2
        return 0

    def get_collection_history(self, cache, labels, start, end, detail,
            binsize):

        # most detail levels behave normally, except amp-traceroute ippaths
        # detail, which needs to aggregate unique paths across the whole
        # time period (and can't use collection.get_collection_history()
        # because it will set binsize to something smaller, and possibly have
        # weird interactions with the block fetching algorithm).
        # TODO create another magic binsize value that will cause
        # collection.get_collection_history() to behave correctly?
        if self.collection_name != "amp-traceroute" or detail != "ippaths":
            return super(AmpTraceroute, self).get_collection_history(cache,
                    labels, start, end, detail, binsize)

        uncached = {}
        paths = {}

        for lab in labels:
            cachelabel = lab['labelstring'] + "_ippaths_" + self.collection_name
            if len(cachelabel) > 128:
                log("Warning: ippath cache label %s is too long" % (cachelabel))

            cachehit = cache.search_ippaths(cachelabel, start, end)
            if cachehit is not None:
                paths[lab['labelstring']] = cachehit
                continue

            if len(lab['streams']) == 0:
                paths[lab['labelstring']] = []
            else:
                uncached[lab['labelstring']] = lab['streams']

        if len(uncached) > 0:
            result = self._fetch_history(uncached, start, end, end-start,
                    detail)

            for label, queryresult in result.items():
                if len(queryresult['timedout']) != 0:
                    paths[label] = []
                    continue

                formatted = self.format_list_data(queryresult['data'], detail)

                cachelabel = label + "_ippaths_" + self.collection_name
                if len(cachelabel) > 128:
                    log("Warning: ippath cache label %s is too long" % \
                            (cachelabel))
                cache.store_ippaths(cachelabel, start, end, formatted)
                paths[label] = formatted

        return paths

    def format_list_data(self, datalist, detail):
        reslist = []
        for data in datalist:
            reslist.append(self.format_single_data(data, detail))
        return reslist

    def format_single_data(self, data, detail):
        if 'aspath' not in data or data['aspath'] is None:
            return data

        if detail in ['matrix', 'basic', 'raw', 'tooltiptext', 'spark']:
            return data

        pathlen = 0
        aspath = []
        toquery = set()

        for asn in data['aspath']:
            asnsplit = asn.split('.')
            if len(asnsplit) != 2:
                continue

            if asnsplit[1] == "-2":
                aslabel = asname = "RFC 1918"
            elif asnsplit[1] == "-1":
                aslabel = asname = "No response"
            elif asnsplit[1] == "0":
                aslabel = asname = "Unknown"
            else:
                aslabel = "AS" + asnsplit[1]
                asname = None
                toquery.add(aslabel)

            repeats = int(asnsplit[0])
            pathlen += repeats

            for i in range(0, repeats):
                aspath.append([asname, 0, aslabel])

        data['aspathlen'] = pathlen

        if len(toquery) == 0:
            data['aspath'] = aspath
            return data

        queried = self.asnmanager.queryASNames(toquery)
        if queried is None:
            log("Unable to query AS names")
            data['aspath'] = aspath
            return data

        for asp in aspath:
            if asp[0] != None:
                continue
            if asp[2] not in queried:
                asp[0] = asp[2]
            else:
                asp[0] = queried[asp[2]]

        data['aspath'] = aspath
        return data

    def get_maximum_view_groups(self):
        return 1

    def translate_group(self, groupprops):
        if 'aggregation' not in groupprops or groupprops['aggregation'] \
                    not in ["IPV4", "IPV6"]:
            return None
        return super(AmpTraceroute, self).translate_group(groupprops)

    def get_legend_label(self, description):
        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate legend label")
            return None

        # We can only show one family on a graph at a time
        #if groupparams['aggregation'] == "FAMILY":
        #    groupparams['aggregation'] = "IPV4"

        label = "%s to %s" % (groupparams['source'],
                groupparams['destination'])
        return label, self.splits[groupparams['aggregation']]


class AmpTraceroutePathlen(AmpTraceroute):
    def __init__(self, colid, viewmanager, nntscconf, asnmanager):
        super(AmpTraceroutePathlen, self).__init__(colid, viewmanager,
                nntscconf, asnmanager)

        self.collection_name = "amp-traceroute_pathlen"
        self.viewstyle = "amp-traceroutelength"

    def get_maximum_view_groups(self):
        return 0

    def group_columns(self, detail):
        return []

    def detail_columns(self, detail):
        if detail == "raw":
            aggfuncs = []
            aggcols = ['path_length', 'unused']
        elif detail in ["matrix", "basic", "tooltiptext", "spark"]:
            aggfuncs = ['mode']
            aggcols = ['path_length']
        else:
            aggfuncs = ['smoke']
            aggcols = ['path_length']

        return aggcols, aggfuncs

    def extra_blocks(self, detail):
        if detail == "full":
            return 2
        return 0


class AmpAsTraceroute(AmpTraceroute):
    def __init__(self, colid, viewmanager, nntscconf, asnmanager):
        super(AmpAsTraceroute, self).__init__(colid, viewmanager, nntscconf,
                asnmanager)
        self.collection_name = "amp-astraceroute"
        self.viewstyle = "amp-astraceroute"

    def get_maximum_view_groups(self):
        return 1

    def group_columns(self, detail):
        return []

    def detail_columns(self, detail):
        if detail in ["matrix", "basic", "tooltiptext", "spark", "raw"]:
            aggfuncs = ["avg", "most_array"]
            aggcols = ["responses", "aspath"]
        elif detail == "hops-full" or detail == "hops-summary":
            aggfuncs = ["most_array"]
            aggcols = ["aspath"]
        else:
            aggfuncs = ["smoke"]
            aggcols = ["responses"]
        return aggcols, aggfuncs

    def extra_blocks(self, detail):
        if detail == "hops-full" or detail == "full":
            return 2
        return 0

    def translate_group(self, groupprops):
        if 'aggregation' not in groupprops or groupprops['aggregation'] \
                    not in ["IPV4", "IPV6"]:
            return None
        return super(AmpAsTraceroute, self).translate_group(groupprops)

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
