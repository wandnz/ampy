#
# This file is part of ampy.
#
# Copyright (C) 2013-2020 The University of Waikato, Hamilton, New Zealand.
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

class AmpSip(Collection):
    def __init__(self, colid, viewmanager, nntscconf):
        super(AmpSip, self).__init__(colid, viewmanager, nntscconf)

        self.streamproperties = [
            'source', 'destination', 'proxy', 'filename', 'dscp',
            'max_duration', 'repeat', 'direction', 'family'
            ]
        self.groupproperties = [
            'source', 'destination', 'proxy', 'filename', 'dscp',
            'max_duration', 'repeat', 'direction', 'aggregation'
        ]
        self.integerproperties = [
            'max_duration'
        ]
        self.collection_name = "amp-sip"
        self.viewstyle = "amp-sip"
        self.splits = {
            "FAMILY": "IPv4/IPv6",
            #"FULL": "All Addresses",
            "IPV4": "IPv4",
            "IPV6": "IPv6",
            "NONE": ""
        }

        self.defaults = {
            "filename": "sip-test-8000.wav",
            "repeat": True,
            "max_duration": 30,
            "dscp": "Default",
            "proxy": "",
        }

        self.dirlabels = {"rx": "Receive", "tx": "Transmit"}


    def detail_columns(self, detail):
        if detail == "matrix":
            aggcols = [
                'rtt_mean', 'rtt_mean',
                'mos', 'mos',
                'response_time', 'response_time',
                'connect_time', 'connect_time',
            ]
            aggmethods = [
                'avg', 'stddev',
                'avg', 'stddev',
                'avg', 'stddev',
                'avg', 'stddev',
            ]
            return (aggcols, aggmethods)

        #if detail in ['basic', 'tooltiptext', 'spark']:
        aggcols = ['rtt_mean', 'mos', 'response_time', 'connect_time']
        aggmethods = ['avg', 'avg', 'avg', 'avg']
        return (aggcols, aggmethods)


    def get_legend_label(self, description):
        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate %s legend label" % (self.collection_name))
            return None, ""

        aggregation = self.splits[groupparams['aggregation']]

        if groupparams['direction'] == "BOTH":
            dirstr = ""
        elif groupparams['direction'] == "IN":
            dirstr = " Rx"
        else:
            dirstr = " Tx"

        label = "%s to %s, %ss (DSCP %s)" % (
                groupparams['source'], groupparams['destination'],
                groupparams['max_duration'], groupparams['dscp'])
        return label, "%s%s" % (aggregation, dirstr)


    # XXX still unsure if I need the address later, only used by matrix
    # and that doesn't split on v4/v6 currently?
    def prepare_stream_for_storage(self, stream):
        if 'address' not in stream:
            return stream, {}

        if self._address_to_family(stream['address']) == "ipv4":
            stream['family'] = "ipv4"
        else:
            stream['family'] = "ipv6"

        return stream, {"address": stream["address"]}


    def create_group_description(self, properties):
        if 'direction' not in properties:
            properties['direction'] = "BOTH"
        #if 'family' not in properties and 'address' in properties:
        #    properties['family'] = self._address_to_family(properties['address'])
        if 'proxy' not in properties:
            properties['proxy'] = ""

        for prop in self.groupproperties:
            if prop not in properties:
                log("Required group property '%s' not present in %s group" % (
                        prop, self.collection_name))
                return None

        properties['direction'] = properties['direction'].upper()
        properties['aggregation'] = properties['aggregation'].upper()

        return "FROM %s TO %s DSCP %s VIA %s FILE %s DURATION %d REPEAT %s DIRECTION %s %s"\
                % (properties['source'], properties['destination'],
                   properties['dscp'], properties['proxy'],
                   properties['filename'], properties['max_duration'],
                   str(properties['repeat']).upper(),
                   properties['direction'], properties['aggregation'])


    def parse_group_description(self, description):
        regex = "FROM (?P<source>[.a-zA-Z0-9_-]+) "
        regex += "TO (?P<destination>[.a-zA-Z0-9_:@-]+) "
        regex += "DSCP (?P<dscp>[a-zA-Z0-9-]+) "
        regex += "VIA (?P<proxy>[.a-zA-Z0-9:-]+) "
        regex += "FILE (?P<filename>[/.a-zA-Z0-9-]+) "
        regex += "DURATION (?P<duration>[0-9]+) "
        regex += "REPEAT (?P<repeat>[A-Z]+) "
        regex += "DIRECTION (?P<direction>[A-Z]+) "
        regex += "(?P<aggregation>[A-Z0-9]+)"

        parts = self._apply_group_regex(regex, description)
        if parts is None:
            return None

        if parts.group('direction') not in ['IN', 'OUT', 'BOTH']:
            log("%s is not a valid direction for a %s group" % \
                    (parts.group('direction'), self.collection_name))
            return None

        if parts.group('aggregation') not in self.splits:
            log("%s has no aggregation method %s" % (
                        self.collection_name, parts.group('family')))
            return None

        keydict = {
            'source': parts.group('source'),
            'destination': parts.group("destination"),
            'aggregation': parts.group("aggregation"),
            'direction': parts.group("direction"),
            'max_duration': int(parts.group('duration')),
            'proxy': parts.group('proxy'),
            'filename': parts.group('filename'),
            'dscp': parts.group('dscp'),
            'repeat': parts.group('repeat') == "TRUE",
        }

        return keydict


    def update_matrix_groups(self, cache, source, dest, optdict, groups, views,
            viewmanager, viewstyle):

        baseprop = {
            'source': source,
            'destination': dest,
        }

        rxstreams = self._matrix_group_streams(baseprop, "rx")
        txstreams = self._matrix_group_streams(baseprop, "tx")

        if len(rxstreams) == 0 and len(txstreams) == 0:
            return

        if optdict['split'] == "rx":
            split = "IN"
        elif optdict['split'] == "tx":
            split = "OUT"
        else:
            split = "BOTH"

        if len(rxstreams) > 0:
            groups.append({
                'labelstring': '%s_%s_rx' % (source, dest),
                'streams': [x[0] for x in rxstreams]
            })

        if len(txstreams) > 0:
            groups.append({
                'labelstring': '%s_%s_tx' % (source, dest),
                'streams': [x[0] for x in txstreams]
            })

        baseprop["direction"] = split
        baseprop["aggregation"] = "FAMILY"
        cellgroup = self.create_group_description(baseprop)

        cachelabel = "_".join([viewstyle, self.collection_name, source,
                dest, split])
        viewid = cache.search_matrix_view(cachelabel)
        if viewid is not None:
            views[(source, dest)] = viewid
        else:
            viewid = viewmanager.add_groups_to_view(viewstyle,
                self.collection_name, 0, [cellgroup])
            if viewid is None:
                views[(source, dest)] = -1
                cache.store_matrix_view(cachelabel, -1, 300)
            else:
                views[(source, dest)] = viewid
                cache.store_matrix_view(cachelabel, viewid, 0)


    def _matrix_group_streams(self, baseprop, direction):
        # temporarily add the specific direction to the properties
        baseprop['direction'] = direction
        streams = []
        if len(streams) == 0:
            # otherwise we'll need to try to pick a stream
            props = self.get_selections(baseprop, "", "1", 30000, False)
            if props is None:
                return []

            # as long as there are properties that need setting, keep setting
            # them until we get a stream id (that hopefully has recent data!)
            while len(props) > 0:
                for prop, values in props.iteritems():
                    # prefer the default values, but if they aren't present
                    # then pick the first option available
                    if prop in self.defaults and \
                                any(n['text'] == self.defaults[prop] \
                                    for n in values['items']):
                        baseprop[prop] = self.defaults[prop]
                    else:
                        baseprop[prop] = values['items'][0]['text']
                props = self.get_selections(baseprop, "", "1", 30000, False)

            streams = self.streammanager.find_streams(baseprop)

        return streams


    def group_to_labels(self, groupid, description, lookup=True):
        labels = []

        groupparams = self.parse_group_description(description)
        if groupparams is None:
            log("Failed to parse group description to generate group labels")
            return None

        baselabel = 'group_%s' % (groupid)

        search = dict(groupparams)
        del search['direction']

        # XXX aggregation vs family
        if groupparams['direction'] in ['IN', 'BOTH']:
            label = self._generate_direction_labels(baselabel, search, 'rx',
                    groupparams['aggregation'], lookup)
                    #groupparams['family'], lookup)
            if label is None:
                return None
            labels += label

        if groupparams['direction'] in ['OUT', 'BOTH']:
            label = self._generate_direction_labels(baselabel, search, 'tx',
                    groupparams['aggregation'], lookup)
            if label is None:
                return None
            labels += label

        return sorted(labels, key=itemgetter('shortlabel'))


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
                # TODO do i care about addresses? is it easy to remove?
                if 'address' not in store:
                    log("Error: no address stored with stream id %s" % (sid))
                    return None

                if shortlabel == self.dirlabels["rx"]:
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
        shortlabel = family + " " + self.dirlabels[search['direction']]

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

# vim: set smartindent shiftwidth=4 tabstop=4 softtabstop=4 expandtab :
